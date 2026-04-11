from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .artifacts import utc_now
from .db import LimaCoreDB
from .models import AristotleAgenda, AristotleJobResult, AristotleJobSpec, DeltaProposal, ProblemSpec


class AristotleBackend(Protocol):
    def run_job(self, problem: ProblemSpec, delta: DeltaProposal, spec: AristotleJobSpec) -> AristotleJobResult:
        ...


@dataclass(slots=True)
class LocalAristotleBackend:
    deterministic_seed: int = 7

    def run_job(self, problem: ProblemSpec, delta: DeltaProposal, spec: AristotleJobSpec) -> AristotleJobResult:
        family = delta.family_key or (delta.world_packet.family_key if delta.world_packet else "")
        if problem.slug == "inward-compression-conjecture" and family in {"balancing_world", "order_or_convexity"}:
            if spec.job_kind == "bridge_lemma":
                return AristotleJobResult(
                    job_kind=spec.job_kind,
                    verdict="proved",
                    replayable=True,
                    summary_md="Replayable bridge proved in offset coordinates.",
                    artifact={"replay_certificate": "offset_coordinates_bridge", "status": "proved"},
                )
            if spec.job_kind == "local_law":
                return AristotleJobResult(
                    job_kind=spec.job_kind,
                    verdict="proved",
                    replayable=True,
                    summary_md="Replayable convex energy descent law proved.",
                    artifact={"replay_certificate": "convex_energy_descent", "status": "proved"},
                )
            if spec.job_kind == "theorem_skeleton_probe":
                return AristotleJobResult(
                    job_kind=spec.job_kind,
                    verdict="blocked",
                    replayable=False,
                    summary_md="Blocked on a fully formal canonical balanced-profile uniqueness lemma.",
                    artifact={"blocker": "need_canonical_profile_lemma"},
                )
            return AristotleJobResult(
                job_kind=spec.job_kind,
                verdict="inconclusive",
                replayable=False,
                summary_md="No small counterexample found in bounded search.",
                artifact={"search_bound": 64},
            )
        if problem.slug == "collatz":
            if spec.job_kind == "counterexample_search":
                return AristotleJobResult(
                    job_kind=spec.job_kind,
                    verdict="refuted",
                    replayable=False,
                    summary_md="Counterexample-style blocker found for the quotient heuristic.",
                    artifact={"counterexample": {"n": 27, "note": "quotient prediction drifts"}},
                )
            return AristotleJobResult(
                job_kind=spec.job_kind,
                verdict="blocked",
                replayable=False,
                summary_md="Blocked by missing rigorous control of odd-step expansion.",
                artifact={"blocker": "odd_step_expansion"},
            )
        if delta.delta_type == "kill_delta":
            return AristotleJobResult(
                job_kind=spec.job_kind,
                verdict="refuted" if spec.job_kind == "counterexample_search" else "blocked",
                replayable=False,
                summary_md="Kill probe exposed a stale family weakness.",
                artifact={"counterexample": {"family": family or "unknown"}},
            )
        return AristotleJobResult(
            job_kind=spec.job_kind,
            verdict="inconclusive",
            replayable=False,
            summary_md="Agenda did not produce replayable structure.",
            artifact={"note": "narrative-only"},
        )


@dataclass(slots=True)
class RealAristotleBackend:
    """Placeholder for external integrations."""

    endpoint: str = ""

    def run_job(self, problem: ProblemSpec, delta: DeltaProposal, spec: AristotleJobSpec) -> AristotleJobResult:
        return AristotleJobResult(
            job_kind=spec.job_kind,
            verdict="inconclusive",
            replayable=False,
            summary_md=f"Real backend adapter placeholder for {self.endpoint or 'unconfigured'}",
            artifact={},
        )


def submit_aristotle_jobs(
    db: LimaCoreDB,
    problem: ProblemSpec,
    delta: DeltaProposal,
    agenda: AristotleAgenda,
    backend: AristotleBackend,
    *,
    world_id: str | None = None,
    event_id: str | None = None,
) -> tuple[list[str], list[str]]:
    cohort_ids: list[str] = []
    job_ids: list[str] = []
    for title in [agenda.title]:
        cohort_id = db.create_cohort(
            problem.id,
            world_id=world_id,
            cohort_kind="agenda_fanout",
            title=title,
            total_jobs=len(agenda.job_specs),
            last_event_id=event_id,
        )
        cohort_ids.append(cohort_id)
        for spec in agenda.job_specs:
            ref = db.store_artifact("job_input", {"title": spec.title, "payload": spec.input_payload})
            job_id = db.create_job(
                problem.id,
                cohort_id=cohort_id,
                frontier_node_key=spec.frontier_node_key,
                job_kind=spec.job_kind,
                input_artifact_ref=ref,
            )
            db.set_job_status(job_id, status="running")
            result = backend.run_job(problem, delta, spec)
            out_ref = db.store_artifact("job_output", result.artifact)
            db.set_job_status(
                job_id,
                status=(
                    "succeeded"
                    if result.verdict == "proved"
                    else "failed"
                    if result.verdict in {"refuted", "blocked"}
                    else "succeeded"
                ),
                output_artifact_ref=out_ref,
                result_summary_md=result.summary_md,
                replayable=result.replayable,
            )
            job_ids.append(job_id)
        db.update_cohort_metrics(cohort_id)
    return cohort_ids, job_ids


def poll_results(db: LimaCoreDB, cohort_ids: list[str]) -> list[dict]:
    rows: list[dict] = []
    for cohort_id in cohort_ids:
        rows.extend(db.list_jobs(problem_id=db.get_cohort(cohort_id)["problem_id"], cohort_id=cohort_id))
    return rows
