from __future__ import annotations

import asyncio
from dataclasses import asdict

from .aristotle import AristotleBackend, LocalAristotleBackend, poll_results, submit_aristotle_jobs
from .artifacts import utc_now
from .compiler import CompileError, compile_delta_to_reduction
from .cohorts import build_aristotle_cohorts
from .db import LimaCoreDB
from .events import apply_event_artifacts
from .frontier import ensure_target_frontier, select_frontier_gap
from .models import DeltaProposal, FrontierNode, ProblemSpec
from .program import maybe_accept_program_delta, write_candidate_program_delta
from .proposer import Proposer
from .retriever import Retriever
from .scorer import score_results
from .solved import solved_checker


class LimaCoreLoop:
    def __init__(
        self,
        db: LimaCoreDB,
        *,
        backend: AristotleBackend | None = None,
        proposer: Proposer | None = None,
        retriever: Retriever | None = None,
    ) -> None:
        self.db = db
        self.backend = backend or LocalAristotleBackend()
        self.proposer = proposer or Proposer(db)
        self.retriever = retriever or Retriever(db)

    def run_iteration(self, problem_slug_or_id: str, *, forced_delta: DeltaProposal | None = None) -> dict:
        problem_row = self.db.get_problem(problem_slug_or_id)
        if problem_row is None:
            raise KeyError(problem_slug_or_id)
        problem = ProblemSpec(**problem_row)
        ensure_target_frontier(self.db, problem.id, target_statement=problem.target_theorem or problem.statement_md)
        if solved_checker(self.db, problem.id).solved:
            return {"decision": "noop", "reason": "already solved"}
        gap = select_frontier_gap(self.db, problem.id)
        select_event = self.db.append_event(
            problem.id,
            "frontier_gap_selected",
            "selected",
            summary_md=f"Selected gap `{gap['node_key']}`.",
        )
        delta = forced_delta or self.proposer.propose_delta(problem, gap)
        delta_ref = self.db.store_artifact("delta_proposal", asdict(delta))
        proposed_event = self.db.append_event(
            problem.id,
            "delta_proposed",
            "proposed",
            parent_event_id=select_event,
            artifact_refs=[delta_ref],
            summary_md=delta.summary_md,
        )
        if delta.delta_type == "program_delta":
            candidate = write_candidate_program_delta(self.db, problem.id, note=delta.summary_md)
            accepted = maybe_accept_program_delta(self.db, problem.id, candidate)
            event_type = "program_updated" if accepted else "delta_reverted"
            score = {"accepted": accepted, "replayable_gain": 0, "proof_debt_delta": 0, "fracture_gain": 0}
            self.db.append_event(
                problem.id,
                event_type,
                "accepted" if accepted else "reverted",
                parent_event_id=proposed_event,
                score_delta=score,
                summary_md="Program delta kept after verified yield improvement." if accepted else "Program delta rejected: verified yield did not improve.",
            )
            return {"accepted": accepted, "delta_type": "program_delta"}
        grounding = self.retriever.build_grounding_bundle(problem, delta)
        grounding_ref = self.db.store_artifact("grounding_bundle", asdict(grounding))
        grounding_event = self.db.append_event(
            problem.id,
            "grounding_built",
            "grounded",
            parent_event_id=proposed_event,
            artifact_refs=[grounding_ref],
            summary_md="Grounding built.",
        )
        try:
            reduction, agenda = compile_delta_to_reduction(gap, delta, grounding)
        except CompileError as exc:
            self.db.append_event(
                problem.id,
                "delta_reverted",
                "reverted",
                parent_event_id=grounding_event,
                summary_md=f"Compile rejected early: {exc}",
            )
            return {"accepted": False, "reason": str(exc)}
        reduction_ref = self.db.store_artifact("reduction_packet", asdict(reduction))
        compile_event = self.db.append_event(
            problem.id,
            "agenda_compiled",
            "compiled",
            parent_event_id=grounding_event,
            artifact_refs=[reduction_ref],
            summary_md=reduction.rationale_md,
        )
        _cohorts = build_aristotle_cohorts(agenda)
        world_id = None
        if delta.world_packet is not None:
            world_id = f"{problem.id}:{delta.world_packet.family_key}"
        cohort_ids, _job_ids = submit_aristotle_jobs(
            self.db,
            problem,
            delta,
            agenda,
            self.backend,
            world_id=world_id,
            event_id=compile_event,
        )
        self.db.append_event(
            problem.id,
            "aristotle_jobs_submitted",
            "submitted",
            parent_event_id=compile_event,
            summary_md=f"Submitted {len(agenda.job_specs)} jobs across {len(cohort_ids)} cohort(s).",
        )
        jobs = poll_results(self.db, cohort_ids)
        self.db.append_event(
            problem.id,
            "aristotle_jobs_finished",
            "finished",
            parent_event_id=compile_event,
            summary_md=f"Finished {len(jobs)} jobs.",
        )
        score = score_results(self.db, problem, delta, reduction, jobs)
        if score.accepted:
            artifacts = self._commit_delta(problem, delta, reduction, jobs, score)
            refs = [self.db.store_artifact(kind, content) for kind, content in artifacts]
            event_id = self.db.append_event(
                problem.id,
                "frontier_improved",
                "accepted",
                parent_event_id=compile_event,
                score_delta=asdict(score),
                artifact_refs=refs,
                summary_md=score.summary_md,
            )
            structured = [{"artifact_kind": kind, "content": content} for kind, content in artifacts]
            apply_event_artifacts(self.db, problem.id, event_id, structured)
        else:
            refs = []
            structured = []
            fracture = None
            if delta.family_key:
                fracture = {
                    "family_key": delta.family_key,
                    "failure_type": "stale_or_refuted",
                    "smallest_counterexample_ref": {},
                    "blocker_note_md": score.summary_md,
                    "required_delta_md": "Change ontology or supply a new bridge before retrying this family.",
                    "ban_level": "soft" if delta.delta_type != "kill_delta" else "hard",
                    "repeat_count": 1 + sum(1 for row in self.db.list_fracture_heads(problem.id) if row["family_key"] == delta.family_key),
                    "updated_at": utc_now(),
                }
                refs = [self.db.store_artifact("fracture_head", fracture)]
                structured = [{"artifact_kind": "fracture_head", "content": fracture}]
            event_id = self.db.append_event(
                problem.id,
                "delta_reverted",
                "reverted",
                parent_event_id=compile_event,
                score_delta=asdict(score),
                artifact_refs=refs,
                summary_md=score.summary_md,
            )
            if structured:
                apply_event_artifacts(self.db, problem.id, event_id, structured)
        report = solved_checker(self.db, problem.id)
        if report.solved:
            self.db.append_event(
                problem.id,
                "solved_confirmed",
                "accepted",
                summary_md="Solved checker passed from clean replayable state.",
            )
            self.db.update_problem_status(problem.id, "solved")
        return {
            "accepted": score.accepted,
            "delta_type": delta.delta_type,
            "gap": gap["node_key"],
            "score": asdict(score),
            "solved": report.solved,
        }

    def _commit_delta(self, problem: ProblemSpec, delta: DeltaProposal, reduction, jobs: list[dict], score) -> list[tuple[str, dict]]:
        artifacts: list[tuple[str, dict]] = []
        proved_replay = [job for job in jobs if job["replayable"]]
        if delta.world_packet is not None:
            artifacts.append(
                (
                    "world_head",
                    {
                        "family_key": delta.world_packet.family_key,
                        "world_name": delta.world_packet.world_name,
                        "status": "surviving",
                        "bridge_status": "proved" if any(job["job_kind"] == "bridge_lemma" and job["replayable"] for job in jobs) else "unknown",
                        "kill_status": "survived" if not any("counterexample" in str(job["result_summary_md"]).lower() for job in jobs) else "pressured",
                        "theorem_status": "open",
                        "yield_score": float(score.replayable_gain + max(0.0, score.novelty_signal)),
                        "latest_artifact_ref": {},
                        "updated_at": utc_now(),
                    },
                )
            )
        if any(job["job_kind"] == "bridge_lemma" and job["replayable"] for job in jobs):
            artifacts.append(
                (
                    "frontier_node",
                    FrontierNode(
                        id=f"{problem.id}-bridge",
                        problem_id=problem.id,
                        node_key="bridge_claim",
                        node_kind="bridge_lemma",
                        title="Bridge claim",
                        statement_md=reduction.bridge_claim,
                        formal_statement=reduction.bridge_claim,
                        status="proved",
                        best_world_id=delta.family_key or None,
                        replay_ref={"replay_certificate": "bridge_claim"},
                        priority=9.0,
                        updated_at=utc_now(),
                    ).to_dict(),
                )
            )
        if any(job["job_kind"] == "local_law" and job["replayable"] for job in jobs):
            artifacts.append(
                (
                    "frontier_node",
                    FrontierNode(
                        id=f"{problem.id}-law",
                        problem_id=problem.id,
                        node_key="local_energy_law",
                        node_kind="local_law",
                        title="Local energy law",
                        statement_md=reduction.local_law,
                        formal_statement=reduction.local_law,
                        status="proved",
                        best_world_id=delta.family_key or None,
                        replay_ref={"replay_certificate": "local_energy_law"},
                        priority=8.0,
                        updated_at=utc_now(),
                    ).to_dict(),
                )
            )
        theorem_status = "blocked"
        replay_ref = {}
        if any(job["job_kind"] == "theorem_skeleton_probe" and job["replayable"] for job in jobs):
            theorem_status = "proved"
            replay_ref = {"replay_certificate": "terminal_form_uniqueness"}
        artifacts.append(
            (
                "frontier_node",
                FrontierNode(
                    id=f"{problem.id}-skeleton",
                    problem_id=problem.id,
                    node_key="terminal_form_uniqueness",
                    node_kind="theorem_skeleton",
                    title="Terminal form uniqueness",
                    statement_md=reduction.theorem_skeleton,
                    formal_statement=reduction.theorem_skeleton,
                    status=theorem_status,
                    blocker_kind="" if theorem_status == "proved" else "missing_uniqueness_lemma",
                    blocker_note_md="" if theorem_status == "proved" else "Need a full canonical balanced-profile lemma.",
                    best_world_id=delta.family_key or None,
                    replay_ref=replay_ref,
                    priority=7.0,
                    updated_at=utc_now(),
                ).to_dict(),
            )
        )
        if not any(node["node_key"] == "replay_closure" for node in self.db.get_frontier_nodes(problem.id)):
            artifacts.append(
                (
                    "frontier_node",
                    FrontierNode(
                        id=f"{problem.id}-replay",
                        problem_id=problem.id,
                        node_key="replay_closure",
                        node_kind="replay_check",
                        title="Replay closure",
                        statement_md="All dependencies replay from clean state.",
                        formal_statement="Replay closure check",
                        status="open",
                        priority=6.0,
                        updated_at=utc_now(),
                    ).to_dict(),
                )
            )
        target = self.db.get_frontier_node(problem.id, "target_theorem")
        if target is not None:
            target_node = FrontierNode(
                id=str(target["id"]),
                problem_id=problem.id,
                node_key="target_theorem",
                node_kind=str(target["node_kind"]),
                title=str(target["title"]),
                statement_md=str(target["statement_md"]),
                formal_statement=str(target["formal_statement"]),
                status="proved" if {"bridge_claim", "local_energy_law", "terminal_form_uniqueness", "replay_closure"}.issubset(
                    {artifact[1]["node_key"] for artifact in artifacts if artifact[0] == "frontier_node" and artifact[1].get("status") == "proved"}
                ) else str(target["status"]),
                dependency_keys=list(target.get("dependency_keys") or []),
                priority=float(target.get("priority") or 10.0),
                replay_ref={"replay_certificate": "target_theorem"} if len(proved_replay) >= 4 else dict(target.get("replay_ref") or {}),
                updated_at=utc_now(),
            )
            artifacts.append(("frontier_node", target_node.to_dict()))
        return artifacts


async def limacore_loop(db: LimaCoreDB, *, interval_sec: int = 300) -> None:
    loop = LimaCoreLoop(db)
    while True:
        await asyncio.sleep(interval_sec)
        db.initialize()
        for problem in db.list_problems():
            if str(problem.get("status") or "active") != "active":
                continue
            loop.run_iteration(str(problem["id"]))
