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
from .frontier_derivation import (
    derive_frontier_updates,
    make_bridge_node,
    make_local_law_node,
    make_replay_node,
)
from .models import DeltaProposal, FrontierNode, ProblemSpec
from .prompting import normalize_problem_prompt
from .program import maybe_accept_program_delta, write_candidate_program_delta
from .proposer import Proposer
from .retriever import Retriever
from .runtime import detect_runtime_status, persist_runtime_status
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

    def create_problem_from_prompt(self, prompt: str) -> dict:
        parsed = normalize_problem_prompt(prompt)
        problem_id, _created = self.db.create_problem(
            slug=parsed.slug,
            title=parsed.title,
            statement_md=parsed.statement_md,
            domain=parsed.domain,
            target_theorem=parsed.normalized_statement_md,
            original_prompt=prompt.strip(),
            normalized_statement_md=parsed.normalized_statement_md,
            runtime_status="booting",
            status_reason_md="Booting: creating normalized theorem and initial world line.",
            autopilot_enabled=True,
        )
        problem = self.db.get_problem(problem_id)
        assert problem is not None
        boot_event = self.db.append_event(
            problem_id,
            "problem_created_from_prompt",
            "accepted",
            summary_md=f"Created from prompt. Domain inferred as {parsed.domain}.",
        )
        ensure_target_frontier(self.db, problem_id, target_statement=parsed.normalized_statement_md)
        gap = self.db.get_frontier_node(problem_id, "target_theorem")
        assert gap is not None
        spec = ProblemSpec(**problem)
        initial_worlds = [self.proposer.worldsmith.propose_world(spec, gap)]
        pref_text = " ".join(parsed.preferences)
        if "hidden-state" in pref_text and initial_worlds[0].family_key != "hidden_state":
            alt = DeltaProposal(
                delta_type="world_delta",
                title="Hidden state line",
                summary_md="Preference-adjusted hidden-state boot line.",
                family_key="hidden_state",
                world_packet=self.proposer.worldsmith.propose_world(spec, gap).world_packet,
                target_node_key="target_theorem",
            )
            if alt.world_packet is not None:
                alt.world_packet.family_key = "hidden_state"
                alt.world_packet.world_name = "Hidden state boot line"
                alt.world_packet.novelty_note = "Boot candidate derived from prompt preference."
                initial_worlds.append(alt)
        for delta in initial_worlds[:2]:
            if delta.world_packet is None:
                continue
            ref = self.db.store_artifact("world_packet", asdict(delta.world_packet))
            self.db.append_event(
                problem_id,
                "delta_proposed",
                "proposed",
                parent_event_id=boot_event,
                artifact_refs=[ref],
                summary_md=f"Boot candidate: {delta.world_packet.world_name}",
            )
            self.db.replace_world_head(
                problem_id,
                {
                    "family_key": delta.world_packet.family_key,
                    "world_name": delta.world_packet.world_name,
                    "status": "boot_candidate",
                    "bridge_status": "unknown",
                    "kill_status": "unknown",
                    "theorem_status": "unknown",
                    "yield_score": float(delta.world_packet.confidence_prior),
                    "updated_at": utc_now(),
                },
            )
        self.db.append_event(
            problem_id,
            "autopilot_started",
            "accepted",
            parent_event_id=boot_event,
            summary_md="Autopilot started automatically after prompt creation.",
        )
        self.db.update_problem_runtime(
            problem_id,
            runtime_status="running",
            status_reason_md="Running: autopilot active.",
            autopilot_enabled=True,
            since_timestamp=utc_now(),
        )
        first_result = self.run_iteration(problem_id)
        problem = self.db.get_problem(problem_id)
        assert problem is not None
        return {
            "problem_slug": str(problem["slug"]),
            "title": str(problem["title"]),
            "status": str(problem.get("runtime_status") or "running"),
            "workspace_url": f"/limacore/{problem['slug']}",
            "first_result": first_result,
        }

    def run_iteration(self, problem_slug_or_id: str, *, forced_delta: DeltaProposal | None = None) -> dict:
        problem_row = self.db.get_problem(problem_slug_or_id)
        if problem_row is None:
            raise KeyError(problem_slug_or_id)
        problem = ProblemSpec(**problem_row)
        if not int(problem.autopilot_enabled):
            self.db.update_problem_runtime(
                problem.id,
                runtime_status="paused",
                status_reason_md="Paused: autopilot disabled.",
                autopilot_enabled=False,
            )
            return {"decision": "paused", "reason": "autopilot disabled"}
        ensure_target_frontier(self.db, problem.id, target_statement=problem.target_theorem or problem.statement_md)
        if solved_checker(self.db, problem.id).solved:
            self.db.update_problem_runtime(
                problem.id,
                runtime_status="solved",
                status_reason_md="Solved: target theorem closed and replay check passed.",
                autopilot_enabled=False,
            )
            return {"decision": "noop", "reason": "already solved", "status": "solved"}
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
            family_key=delta.family_key,
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
            if accepted:
                self.db.update_problem_runtime(problem.id, runtime_status="running", last_gain_at=utc_now())
            persist_runtime_status(self.db, problem.id)
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
            persist_runtime_status(self.db, problem.id)
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
                family_key=delta.family_key,
            )
            structured = [{"artifact_kind": kind, "content": content} for kind, content in artifacts]
            apply_event_artifacts(self.db, problem.id, event_id, structured)
            if score.replayable_gain > 0 or score.fracture_gain > 0:
                self.db.update_problem_runtime(
                    problem.id,
                    runtime_status="running",
                    status_reason_md="Running: autopilot active.",
                    last_gain_at=utc_now(),
                    blocked_node_key="",
                    blocker_kind="",
                    exhausted_family_key="",
                    exhausted_family_since="",
                    stalled_since="",
                )
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
                    "required_delta_md": str(delta.edits.get("required_delta_md") or "Change ontology or supply a materially different bridge before retrying this family."),
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
                family_key=delta.family_key,
            )
            if structured:
                apply_event_artifacts(self.db, problem.id, event_id, structured)
        report = solved_checker(self.db, problem.id)
        if report.solved:
            self.db.append_event(
                problem.id,
                "problem_solved",
                "accepted",
                summary_md="Solved checker passed from clean replayable state.",
            )
            self.db.update_problem_status(problem.id, "solved")
            self.db.update_problem_runtime(
                problem.id,
                runtime_status="solved",
                status_reason_md="Solved: target theorem closed and replay check passed.",
                autopilot_enabled=False,
                last_gain_at=utc_now(),
            )
        else:
            before = self.db.get_problem(problem.id) or {}
            updated = persist_runtime_status(self.db, problem.id)
            after_status = str(updated.get("runtime_status") or "")
            before_status = str(before.get("runtime_status") or "")
            if after_status != before_status:
                event_type = {
                    "blocked": "problem_blocked",
                    "stalled": "problem_stalled",
                    "running": "autopilot_started",
                    "paused": "autopilot_paused",
                    "failed": "problem_failed",
                }.get(after_status)
                if event_type:
                    self.db.append_event(
                        problem.id,
                        event_type,
                        "accepted",
                        summary_md=str(updated.get("status_reason_md") or after_status),
                    )
        return {
            "accepted": score.accepted,
            "delta_type": delta.delta_type,
            "gap": gap["node_key"],
            "score": asdict(score),
            "solved": report.solved,
            "status": str((self.db.get_problem(problem.id) or {}).get("runtime_status") or ""),
        }

    def run_batch(self, problem_slug_or_id: str, *, iterations: int) -> list[dict]:
        results = []
        for _ in range(max(1, iterations)):
            results.append(self.run_iteration(problem_slug_or_id))
            status = str((self.db.get_problem(problem_slug_or_id) or {}).get("runtime_status") or "")
            if status in {"solved", "failed", "paused"}:
                break
        return results

    def _commit_delta(self, problem: ProblemSpec, delta: DeltaProposal, reduction, jobs: list[dict], score) -> list[tuple[str, dict]]:
        """Commit a delta by creating/updating frontier nodes using problem-native derivation.

        Replaces the previous hardcoded frontier generation with derivation based on
        problem context, reduction content, and job results.
        """
        artifacts: list[tuple[str, dict]] = []
        proved_replay = [job for job in jobs if job["replayable"]]

        # Update world head if we have a world packet
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

        # Create bridge and local law nodes from proved jobs
        if any(job["job_kind"] == "bridge_lemma" and job["replayable"] for job in jobs):
            bridge_node = make_bridge_node(problem, reduction, delta.family_key)
            artifacts.append(("frontier_node", bridge_node.to_dict()))

        if any(job["job_kind"] == "local_law" and job["replayable"] for job in jobs):
            law_node = make_local_law_node(problem, reduction, delta.family_key)
            artifacts.append(("frontier_node", law_node.to_dict()))

        # Derive problem-native downstream frontier nodes
        family_key = delta.family_key or (delta.world_packet.family_key if delta.world_packet else "")
        derived = derive_frontier_updates(problem, family_key, reduction, jobs)

        # Add the downstream node if it exists
        if derived.downstream_node is not None:
            artifacts.append(("frontier_node", derived.downstream_node.to_dict()))

        # Create replay closure node if not exists
        if not any(node["node_key"] == "replay_closure" for node in self.db.get_frontier_nodes(problem.id)):
            replay_node = make_replay_node(problem)
            artifacts.append(("frontier_node", replay_node.to_dict()))

        # Update target theorem with dynamic dependencies
        target = self.db.get_frontier_node(problem.id, "target_theorem")
        if target is not None:
            # Collect all proved node keys from artifacts we're about to create
            proved_node_keys = {
                artifact[1]["node_key"]
                for artifact in artifacts
                if artifact[0] == "frontier_node" and artifact[1].get("status") == "proved"
            }
            # Also include already-proved nodes from DB
            for node in self.db.get_frontier_nodes(problem.id):
                if str(node.get("status") or "") == "proved":
                    proved_node_keys.add(str(node.get("node_key") or ""))

            # Determine target status based on dynamic dependencies
            required_deps = set(derived.target_dependencies)
            deps_satisfied = required_deps.issubset(proved_node_keys)
            replay_sufficient = len(proved_replay) >= 2  # At least bridge + local law

            target_status = "proved" if (deps_satisfied and replay_sufficient) else str(target["status"])
            target_replay_ref = {"replay_certificate": "target_theorem"} if deps_satisfied else {}

            target_node = FrontierNode(
                id=str(target["id"]),
                problem_id=problem.id,
                node_key="target_theorem",
                node_kind=str(target["node_kind"]),
                title=str(target["title"]),
                statement_md=str(target["statement_md"]),
                formal_statement=str(target["formal_statement"]),
                status=target_status,
                dependency_keys=list(derived.target_dependencies),
                priority=float(target.get("priority") or 10.0),
                replay_ref=target_replay_ref,
                updated_at=utc_now(),
            )
            artifacts.append(("frontier_node", target_node.to_dict()))

        return artifacts


def _scheduler_pass(db: LimaCoreDB, loop: LimaCoreLoop) -> list[dict]:
    """Run one scheduler pass: iterate all eligible active problems.

    This is a testable helper that performs a single pass over all problems.
    The main limacore_loop calls this repeatedly.

    Returns:
        List of iteration results for each problem processed.
    """
    results = []
    for problem in db.list_problems():
        if str(problem.get("status") or "active") != "active":
            continue
        if int(problem.get("autopilot_enabled", 1) or 0) != 1:
            continue
        if str(problem.get("runtime_status") or "") in {"paused", "solved", "failed"}:
            continue
        result = loop.run_iteration(str(problem["id"]))
        results.append({"problem_id": problem["id"], "slug": problem["slug"], "result": result})
    return results


async def limacore_loop(db: LimaCoreDB, *, interval_sec: int = 300) -> None:
    """Background loop for LimaCore autopilot.

    Runs eligible active problems immediately on startup, then sleeps between passes.
    This avoids the dead zone where the system would wait a full interval before
    the first iteration after startup/deploy.
    """
    loop = LimaCoreLoop(db)
    while True:
        db.initialize()
        # Run immediately on first iteration (and each wakeup)
        _scheduler_pass(db, loop)
        # Sleep until next pass
        await asyncio.sleep(interval_sec)
