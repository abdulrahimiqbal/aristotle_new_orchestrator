from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .db import LimaCoreDB


ACTIVE_WORLD_STATUSES = {"surviving", "boot_candidate", "proposed"}
DECISION_EVENT_TYPES = {"frontier_improved", "delta_reverted", "program_updated"}
ROTATION_ORDER = (
    "hidden_state",
    "balancing_world",
    "coordinate_lift",
    "operator_world",
    "order_or_convexity",
    "cocycle",
    "symbolic_dynamics",
    "graph_or_rewrite",
    "other",
)


@dataclass(slots=True)
class ControlSnapshot:
    problem_id: str
    problem_slug: str
    current_family_key: str
    blocked_node_key: str
    blocker_kind: str
    blocker_summary: str
    current_required_delta_md: str
    current_theorem_skeleton_md: str
    exhausted_family_key: str
    exhausted_family_since: str
    exhausted_reason: str
    suggested_family_key: str
    recent_replayable_gain: int
    recent_proof_debt_delta: int
    recent_fracture_gain: int
    recent_reverts: int
    yielded_lemmas: int
    failed_jobs: int
    running_jobs: int
    queued_jobs: int
    succeeded_jobs: int
    total_jobs: int
    failed_cohorts: int
    current_family_failed_cohorts: int
    current_family_failed_jobs: int
    current_family_total_jobs: int
    live_family_count: int
    active_alternative_families: tuple[str, ...]
    same_blocker_persists: bool
    same_family_persists: bool
    current_family_exhausted: bool


def _family_key_for_world_id(world_id: str | None, problem_id: str) -> str:
    if not world_id:
        return ""
    prefix = f"{problem_id}:"
    if world_id.startswith(prefix):
        return world_id[len(prefix) :]
    if ":" in world_id:
        return world_id.rsplit(":", 1)[-1]
    return world_id


def _primary_family_key(worlds: list[dict[str, Any]]) -> str:
    for world in worlds:
        family_key = str(world.get("family_key") or "").strip()
        if family_key:
            return family_key
    return ""


def _blocked_frontier_node(frontier: list[dict[str, Any]], preferred_node_key: str = "") -> dict[str, Any] | None:
    if preferred_node_key:
        for node in frontier:
            if str(node.get("node_key") or "") == preferred_node_key:
                return node
    return next((node for node in frontier if str(node.get("status") or "") == "blocked"), None)


def _recent_events(events: list[dict[str, Any]], window: int) -> list[dict[str, Any]]:
    if window <= 0:
        return []
    return events[-window:]


def _sum_score_delta(events: list[dict[str, Any]], key: str) -> int:
    return sum(int((event.get("score_delta") or {}).get(key, 0)) for event in events)


def _recent_reverts(events: list[dict[str, Any]]) -> int:
    return sum(1 for event in events if str(event.get("decision") or "") == "reverted" or str(event.get("event_type") or "") == "delta_reverted")


def _current_family_failed_jobs(db: LimaCoreDB, problem_id: str, family_key: str) -> tuple[int, int, int]:
    if not family_key:
        return 0, 0, 0
    family_cohorts = [
        cohort
        for cohort in db.list_cohorts(problem_id)
        if _family_key_for_world_id(str(cohort.get("world_id") or ""), problem_id) == family_key
    ]
    failed_jobs = 0
    total_jobs = 0
    failed_cohorts = 0
    for cohort in family_cohorts:
        total_jobs += int(cohort.get("total_jobs") or 0)
        failed_jobs += int(cohort.get("failed_jobs") or 0)
        if int(cohort.get("failed_jobs") or 0) > 0 and int(cohort.get("yielded_lemmas") or 0) == 0:
            failed_cohorts += 1
    return failed_cohorts, failed_jobs, total_jobs


def _live_family_keys(worlds: list[dict[str, Any]], *, current_family_key: str) -> tuple[str, ...]:
    keys = []
    for world in worlds:
        family_key = str(world.get("family_key") or "").strip()
        if not family_key or family_key == current_family_key:
            continue
        if str(world.get("status") or "") in ACTIVE_WORLD_STATUSES:
            keys.append(family_key)
    return tuple(dict.fromkeys(keys))


def suggest_rotation_family(problem_slug: str, current_family_key: str, exhausted_family_key: str = "") -> str:
    if problem_slug == "collatz":
        for candidate in ("hidden_state", "cocycle", "operator_world", "coordinate_lift", "other"):
            if candidate != current_family_key:
                return candidate
    if problem_slug == "inward-compression-conjecture":
        for candidate in ("balancing_world", "order_or_convexity", "coordinate_lift", "other"):
            if candidate != current_family_key:
                return candidate
    for candidate in ROTATION_ORDER:
        if candidate != current_family_key and candidate != exhausted_family_key:
            return candidate
    return exhausted_family_key or current_family_key or "other"


def build_control_snapshot(db: LimaCoreDB, problem_id: str, *, window: int = 10) -> ControlSnapshot:
    problem = db.get_problem(problem_id)
    if problem is None:
        raise KeyError(problem_id)
    frontier = db.get_frontier_nodes(problem_id)
    worlds = db.list_world_heads(problem_id)
    fractures = db.list_fracture_heads(problem_id)
    events = db.list_events(problem_id, limit=max(window, 20))
    jobs = db.list_jobs(problem_id)
    recent = _recent_events(events, window)
    current_family_key = _primary_family_key(worlds)
    blocked_node = _blocked_frontier_node(frontier, str(problem.get("blocked_node_key") or ""))
    blocker_kind = str((blocked_node or {}).get("blocker_kind") or problem.get("blocker_kind") or "")
    blocker_summary = str((blocked_node or {}).get("blocker_note_md") or problem.get("status_reason_md") or "")
    current_required_delta_md = ""
    if current_family_key:
        current_family_fracture = next((row for row in fractures if str(row.get("family_key") or "") == current_family_key), None)
        if current_family_fracture is not None:
            current_required_delta_md = str(current_family_fracture.get("required_delta_md") or "")
    theorem_node = next((node for node in frontier if str(node.get("node_key") or "") == "terminal_form_uniqueness"), None)
    current_theorem_skeleton_md = str((theorem_node or {}).get("formal_statement") or (theorem_node or {}).get("statement_md") or "")
    recent_replayable_gain = _sum_score_delta(recent, "replayable_gain")
    recent_proof_debt_delta = _sum_score_delta(recent, "proof_debt_delta")
    recent_fracture_gain = _sum_score_delta(recent, "fracture_gain")
    recent_reverts = _recent_reverts(recent)
    yielded_lemmas = sum(int(cohort.get("yielded_lemmas") or 0) for cohort in db.list_cohorts(problem_id))
    total_jobs = len(jobs)
    failed_jobs = sum(1 for job in jobs if str(job.get("status") or "") == "failed")
    running_jobs = sum(1 for job in jobs if str(job.get("status") or "") == "running")
    queued_jobs = sum(1 for job in jobs if str(job.get("status") or "") == "queued")
    succeeded_jobs = sum(1 for job in jobs if str(job.get("status") or "") == "succeeded")
    failed_cohorts, current_family_failed_jobs, current_family_total_jobs = _current_family_failed_jobs(db, problem_id, current_family_key)
    active_alternative_families = _live_family_keys(worlds, current_family_key=current_family_key)
    live_family_count = len(active_alternative_families) + (1 if current_family_key else 0)
    current_family_repeat_count = 0
    if current_family_key:
        current_family_repeat_count = max(
            (int(row.get("repeat_count") or 0) for row in fractures if str(row.get("family_key") or "") == current_family_key),
            default=0,
        )
    same_blocker_persists = bool(
        blocked_node
        and (
            str(problem.get("blocked_node_key") or "") == str(blocked_node.get("node_key") or "")
            or str(problem.get("blocker_kind") or "") == blocker_kind
        )
    )
    same_family_persists = bool(worlds and current_family_key and current_family_key == str(worlds[0].get("family_key") or ""))
    no_verified_progress = recent_replayable_gain <= 0 and recent_proof_debt_delta >= 0
    current_family_exhausted = bool(
        current_family_key
        and same_blocker_persists
        and no_verified_progress
        and yielded_lemmas == 0
        and (
            failed_cohorts >= 2
            or current_family_failed_jobs >= 4
            or (current_family_repeat_count >= 2 and recent_reverts >= 2)
        )
    )
    exhausted_family_key = current_family_key if current_family_exhausted else ""
    exhausted_reason = ""
    if current_family_exhausted:
        exhausted_reason = (
            f"Family {current_family_key} is exhausted after repeated failed cohorts, "
            f"zero replayable gain, and the same blocker persisting."
        )
    return ControlSnapshot(
        problem_id=problem_id,
        problem_slug=str(problem.get("slug") or ""),
        current_family_key=current_family_key,
        blocked_node_key=str((blocked_node or {}).get("node_key") or ""),
        blocker_kind=blocker_kind,
        blocker_summary=blocker_summary,
        current_required_delta_md=current_required_delta_md,
        current_theorem_skeleton_md=current_theorem_skeleton_md,
        exhausted_family_key=exhausted_family_key,
        exhausted_family_since=str(problem.get("exhausted_family_since") or ""),
        exhausted_reason=exhausted_reason,
        suggested_family_key=suggest_rotation_family(str(problem.get("slug") or ""), current_family_key, exhausted_family_key),
        recent_replayable_gain=recent_replayable_gain,
        recent_proof_debt_delta=recent_proof_debt_delta,
        recent_fracture_gain=recent_fracture_gain,
        recent_reverts=recent_reverts,
        yielded_lemmas=yielded_lemmas,
        failed_jobs=failed_jobs,
        running_jobs=running_jobs,
        queued_jobs=queued_jobs,
        succeeded_jobs=succeeded_jobs,
        total_jobs=total_jobs,
        failed_cohorts=failed_cohorts,
        current_family_failed_cohorts=failed_cohorts,
        current_family_failed_jobs=current_family_failed_jobs,
        current_family_total_jobs=current_family_total_jobs,
        live_family_count=live_family_count,
        active_alternative_families=active_alternative_families,
        same_blocker_persists=same_blocker_persists,
        same_family_persists=same_family_persists,
        current_family_exhausted=current_family_exhausted,
    )


def family_exhausted(snapshot: ControlSnapshot) -> bool:
    return snapshot.current_family_exhausted


def is_duplicate_churn(
    snapshot: ControlSnapshot,
    *,
    family_key: str,
    blocked_node_key: str,
    blocker_kind: str,
    required_delta_md: str,
    theorem_skeleton_md: str,
    replayable_gain: int,
    proof_debt_delta: int,
    yielded_lemmas: int,
) -> bool:
    return (
        replayable_gain == 0
        and proof_debt_delta == 0
        and yielded_lemmas == 0
        and family_key == snapshot.current_family_key
        and blocked_node_key == snapshot.blocked_node_key
        and blocker_kind == snapshot.blocker_kind
        and required_delta_md.strip() == snapshot.current_required_delta_md.strip()
        and theorem_skeleton_md.strip() == snapshot.current_theorem_skeleton_md.strip()
    )


def is_actionable_fracture(
    snapshot: ControlSnapshot,
    *,
    family_key: str,
    blocked_node_key: str,
    blocker_kind: str,
    required_delta_md: str,
    theorem_skeleton_md: str,
    next_cohort_plan: str = "",
) -> bool:
    _ = family_key
    _ = theorem_skeleton_md
    if blocked_node_key != snapshot.blocked_node_key:
        return True
    if blocker_kind != snapshot.blocker_kind:
        return True
    if not snapshot.current_required_delta_md.strip() and not next_cohort_plan.strip():
        return False
    if required_delta_md.strip() != snapshot.current_required_delta_md.strip():
        return True
    if next_cohort_plan.strip():
        return True
    return False
