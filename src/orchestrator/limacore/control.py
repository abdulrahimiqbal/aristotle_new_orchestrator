from __future__ import annotations

from collections import Counter
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
    # Recent current-family specific metrics (not lifetime problem-wide)
    recent_current_family_yielded_lemmas: int
    recent_current_family_replayable_gain: int
    recent_current_family_failed_jobs: int
    recent_current_family_failed_cohorts: int
    recent_current_family_total_jobs: int
    recent_current_family_accepts: int
    recent_current_family_reverts: int
    recent_current_family_counterexamples: int
    recent_current_family_last_gain_at: str
    # NEW: Repeated cohort pattern detection
    repeated_cohort_pattern_detected: bool
    repeated_cohort_signature: str
    recent_accept_count: int
    recent_revert_count: int
    # NEW: Current-line KPIs
    current_line_replayable_gain_rate: float
    window_size: int
    current_line_node_key: str = ""
    current_line_key: str = ""
    recent_current_family_proof_debt_delta: int = 0
    recent_current_family_repeated_signature_count: int = 0
    recent_current_line_yielded_lemmas: int = 0
    recent_current_line_replayable_gain: int = 0
    recent_current_line_proof_debt_delta: int = 0
    recent_current_line_failed_jobs: int = 0
    recent_current_line_failed_cohorts: int = 0
    recent_current_line_total_jobs: int = 0
    recent_current_line_accepts: int = 0
    recent_current_line_reverts: int = 0
    recent_current_line_counterexamples: int = 0
    recent_current_line_last_gain_at: str = ""
    recent_current_line_repeated_signature_count: int = 0
    current_line_exhausted: bool = False


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


def _current_line_key(family_key: str, frontier_node_key: str) -> str:
    if not family_key or not frontier_node_key:
        return ""
    return f"{family_key}:{frontier_node_key}"


def _current_frontier_node_key(frontier: list[dict[str, Any]], blocked_node: dict[str, Any] | None) -> str:
    if blocked_node is not None:
        key = str(blocked_node.get("node_key") or "")
        if key:
            return key
    target = next((node for node in frontier if str(node.get("node_key") or "") == "target_theorem"), None)
    if target is not None:
        return str(target.get("node_key") or "")
    open_node = next((node for node in frontier if str(node.get("status") or "") == "open"), None)
    if open_node is not None:
        return str(open_node.get("node_key") or "")
    return str(frontier[0].get("node_key") or "") if frontier else ""


def _current_family_failed_jobs(
    db: LimaCoreDB,
    problem_id: str,
    family_key: str,
    frontier_node_key: str = "",
) -> tuple[int, int, int]:
    if not family_key:
        return 0, 0, 0
    family_cohorts = [
        cohort
        for cohort in db.list_cohorts(problem_id)
        if _family_key_for_world_id(str(cohort.get("world_id") or ""), problem_id) == family_key
    ]
    if frontier_node_key:
        family_cohorts = [
            cohort
            for cohort in family_cohorts
            if any(
                str(job.get("frontier_node_key") or "") == frontier_node_key
                for job in db.list_jobs(problem_id, cohort_id=str(cohort.get("id") or ""))
            )
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


def _line_signature(
    family_key: str,
    frontier_node_key: str,
    cohort: dict[str, Any],
) -> str:
    return "|".join(
        [
            family_key,
            frontier_node_key,
            str(cohort.get("title") or ""),
            str(int(cohort.get("yielded_lemmas") or 0)),
            str(int(cohort.get("failed_jobs") or 0)),
            str(int(cohort.get("succeeded_jobs") or 0)),
        ]
    )


def _recent_current_line_metrics(
    db: LimaCoreDB,
    problem_id: str,
    family_key: str,
    frontier_node_key: str,
    recent_events: list[dict[str, Any]],
    window: int = 10,
) -> dict[str, Any]:
    if not family_key:
        return {
            "yielded_lemmas": 0,
            "replayable_gain": 0,
            "proof_debt_delta": 0,
            "failed_jobs": 0,
            "failed_cohorts": 0,
            "total_jobs": 0,
            "accepts": 0,
            "reverts": 0,
            "counterexamples": 0,
            "last_gain_at": "",
            "repeated_signature_count": 0,
            "repeated_signature": "",
        }

    all_cohorts = db.list_cohorts(problem_id)
    line_cohorts = [
        cohort
        for cohort in all_cohorts
        if _family_key_for_world_id(str(cohort.get("world_id") or ""), problem_id) == family_key
    ]
    if frontier_node_key:
        line_cohorts = [
            cohort
            for cohort in line_cohorts
            if any(
                str(job.get("frontier_node_key") or "") == frontier_node_key
                for job in db.list_jobs(problem_id, cohort_id=str(cohort.get("id") or ""))
            )
        ]
    line_cohorts.sort(key=lambda c: str(c.get("updated_at") or ""), reverse=True)
    recent_line_cohorts = line_cohorts[:window]

    yielded_lemmas = sum(int(c.get("yielded_lemmas") or 0) for c in recent_line_cohorts)
    failed_jobs = sum(int(c.get("failed_jobs") or 0) for c in recent_line_cohorts)
    total_jobs = sum(int(c.get("total_jobs") or 0) for c in recent_line_cohorts)
    counterexamples = sum(int(c.get("yielded_counterexamples") or 0) for c in recent_line_cohorts)
    failed_cohorts = sum(
        1
        for c in recent_line_cohorts
        if int(c.get("failed_jobs") or 0) > 0 and int(c.get("yielded_lemmas") or 0) == 0
    )

    signatures = [
        _line_signature(family_key, frontier_node_key, cohort)
        for cohort in recent_line_cohorts
        if int(cohort.get("total_jobs") or 0) > 0
    ]
    signature = ""
    signature_count = 0
    if signatures:
        counts = Counter(signatures)
        signature, signature_count = counts.most_common(1)[0]

    accepts = 0
    reverts = 0
    last_gain_at = ""
    for event in recent_events:
        event_family = str(event.get("family_key") or "")
        if event_family == family_key:
            decision = str(event.get("decision") or "")
            if decision == "accepted":
                accepts += 1
                score_delta = event.get("score_delta") or {}
                if int(score_delta.get("replayable_gain", 0)) > 0:
                    last_gain_at = str(event.get("created_at") or last_gain_at)
            elif decision == "reverted":
                reverts += 1

    replayable_gain = sum(
        int((event.get("score_delta") or {}).get("replayable_gain", 0))
        for event in recent_events
        if str(event.get("family_key") or "") == family_key
    )
    proof_debt_delta = sum(
        int((event.get("score_delta") or {}).get("proof_debt_delta", 0))
        for event in recent_events
        if str(event.get("family_key") or "") == family_key
    )

    return {
        "yielded_lemmas": yielded_lemmas,
        "replayable_gain": replayable_gain,
        "proof_debt_delta": proof_debt_delta,
        "failed_jobs": failed_jobs,
        "failed_cohorts": failed_cohorts,
        "total_jobs": total_jobs,
        "accepts": accepts,
        "reverts": reverts,
        "counterexamples": counterexamples,
        "last_gain_at": last_gain_at,
        "repeated_signature_count": signature_count,
        "repeated_signature": signature,
    }


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
    
    current_line_node_key = _current_frontier_node_key(frontier, blocked_node)
    current_line_key = _current_line_key(current_family_key, current_line_node_key)

    # Recent metrics (problem-wide window)
    recent_replayable_gain = _sum_score_delta(recent, "replayable_gain")
    recent_proof_debt_delta = _sum_score_delta(recent, "proof_debt_delta")
    recent_fracture_gain = _sum_score_delta(recent, "fracture_gain")
    recent_reverts = _recent_reverts(recent)

    # Lifetime problem-wide totals (for backward compatibility, but not used for control decisions)
    yielded_lemmas = sum(int(cohort.get("yielded_lemmas") or 0) for cohort in db.list_cohorts(problem_id))
    total_jobs = len(jobs)
    failed_jobs = sum(1 for job in jobs if str(job.get("status") or "") == "failed")
    running_jobs = sum(1 for job in jobs if str(job.get("status") or "") == "running")
    queued_jobs = sum(1 for job in jobs if str(job.get("status") or "") == "queued")
    succeeded_jobs = sum(1 for job in jobs if str(job.get("status") or "") == "succeeded")

    # Line-specific metrics: current family + current frontier node
    current_line_metrics = _recent_current_line_metrics(
        db,
        problem_id,
        current_family_key,
        current_line_node_key,
        recent,
        window,
    )
    failed_cohorts, current_family_failed_jobs, current_family_total_jobs = _current_family_failed_jobs(
        db,
        problem_id,
        current_family_key,
        current_line_node_key,
    )

    pattern_detected, pattern_signature, pattern_count = _detect_repeated_cohort_pattern(
        db,
        problem_id,
        current_family_key,
        current_line_node_key,
        window,
    )

    accept_count = _recent_accept_count(events, window)
    revert_count = _recent_revert_count(events, window)
    gain_rate = _current_line_replayable_gain_rate(current_line_metrics, window)

    active_alternative_families = _live_family_keys(worlds, current_family_key=current_family_key)
    live_family_count = len(active_alternative_families) + (1 if current_family_key else 0)
    same_blocker_persists = bool(
        blocked_node
        and (
            str(problem.get("blocked_node_key") or "") == str(blocked_node.get("node_key") or "")
            or str(problem.get("blocker_kind") or "") == blocker_kind
        )
    )
    same_family_persists = bool(worlds and current_family_key and current_family_key == str(worlds[0].get("family_key") or ""))

    line_no_progress = (
        current_line_metrics["replayable_gain"] <= 0
        and current_line_metrics["accepts"] == 0
        and current_line_metrics["proof_debt_delta"] >= 0
    )
    line_exhausted_by_pattern = (
        current_line_metrics["repeated_signature_count"] >= 3
        or current_line_metrics["failed_cohorts"] >= 2
        or current_line_metrics["failed_jobs"] >= 4
    )
    current_line_exhausted_flag = bool(
        current_line_key
        and same_blocker_persists
        and line_no_progress
        and line_exhausted_by_pattern
        and same_family_persists
    )

    exhausted_family_key = current_family_key if current_line_exhausted_flag else ""
    exhausted_reason = ""
    if current_line_exhausted_flag:
        exhausted_reason = (
            f"Current line {current_line_key} is exhausted after {current_line_metrics['failed_cohorts']} failed cohorts "
            f"({current_line_metrics['failed_jobs']} failed jobs), zero replayable gain in the recent window, "
            f"and repeated identical maintenance."
        )
    current_line_replayable_gain_rate = _current_line_replayable_gain_rate(current_line_metrics, window)
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
        current_family_failed_cohorts=current_line_metrics["failed_cohorts"],
        current_family_failed_jobs=current_line_metrics["failed_jobs"],
        current_family_total_jobs=current_line_metrics["total_jobs"],
        live_family_count=live_family_count,
        active_alternative_families=active_alternative_families,
        same_blocker_persists=same_blocker_persists,
        same_family_persists=same_family_persists,
        current_family_exhausted=current_line_exhausted_flag,
        recent_current_family_yielded_lemmas=current_line_metrics["yielded_lemmas"],
        recent_current_family_replayable_gain=current_line_metrics["replayable_gain"],
        recent_current_family_failed_jobs=current_line_metrics["failed_jobs"],
        recent_current_family_failed_cohorts=current_line_metrics["failed_cohorts"],
        recent_current_family_total_jobs=current_line_metrics["total_jobs"],
        recent_current_family_accepts=current_line_metrics["accepts"],
        recent_current_family_reverts=current_line_metrics["reverts"],
        recent_current_family_counterexamples=current_line_metrics["counterexamples"],
        recent_current_family_last_gain_at=current_line_metrics["last_gain_at"],
        repeated_cohort_pattern_detected=pattern_detected,
        repeated_cohort_signature=pattern_signature or str(current_line_metrics["repeated_signature"] or ""),
        recent_accept_count=accept_count,
        recent_revert_count=revert_count,
        current_line_replayable_gain_rate=current_line_replayable_gain_rate,
        window_size=window,
        current_line_node_key=current_line_node_key,
        current_line_key=current_line_key,
        recent_current_family_proof_debt_delta=current_line_metrics["proof_debt_delta"],
        recent_current_family_repeated_signature_count=current_line_metrics["repeated_signature_count"],
        recent_current_line_yielded_lemmas=current_line_metrics["yielded_lemmas"],
        recent_current_line_replayable_gain=current_line_metrics["replayable_gain"],
        recent_current_line_proof_debt_delta=current_line_metrics["proof_debt_delta"],
        recent_current_line_failed_jobs=current_line_metrics["failed_jobs"],
        recent_current_line_failed_cohorts=current_line_metrics["failed_cohorts"],
        recent_current_line_total_jobs=current_line_metrics["total_jobs"],
        recent_current_line_accepts=current_line_metrics["accepts"],
        recent_current_line_reverts=current_line_metrics["reverts"],
        recent_current_line_counterexamples=current_line_metrics["counterexamples"],
        recent_current_line_last_gain_at=current_line_metrics["last_gain_at"],
        recent_current_line_repeated_signature_count=pattern_count or int(current_line_metrics["repeated_signature_count"] or 0),
        current_line_exhausted=current_line_exhausted_flag,
    )


def family_exhausted(snapshot: ControlSnapshot) -> bool:
    return snapshot.current_line_exhausted


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
    current_line_node_key: str = "",
) -> bool:
    line_node_key = current_line_node_key or blocked_node_key
    snapshot_line_node_key = snapshot.current_line_node_key or snapshot.blocked_node_key
    same_line = (
        family_key == snapshot.current_family_key
        and line_node_key == snapshot_line_node_key
    )

    had_recent_success = (
        snapshot.recent_current_line_accepts > 0
        and snapshot.recent_current_line_replayable_gain > 0
    )

    if had_recent_success:
        return False

    has_recent_family_attempts = (
        snapshot.recent_current_line_total_jobs > 0
        or snapshot.recent_current_line_reverts > 0
        or snapshot.recent_current_line_accepts > 0
    )

    if has_recent_family_attempts:
        recent_family_no_yield = snapshot.recent_current_line_yielded_lemmas == 0
        recent_family_no_gain = snapshot.recent_current_line_replayable_gain <= 0
        recent_family_failing = (
            snapshot.recent_current_line_reverts >= 1
            or snapshot.recent_current_line_failed_cohorts >= 1
            or (snapshot.recent_current_line_accepts > 0 and snapshot.recent_current_line_replayable_gain <= 0)
        )
    else:
        recent_family_no_yield = yielded_lemmas == 0
        recent_family_no_gain = replayable_gain == 0
        recent_family_failing = False

    same_family_and_blocker = (
        same_line
        and blocked_node_key == snapshot.blocked_node_key
        and blocker_kind == snapshot.blocker_kind
    )

    no_progress = (
        replayable_gain == 0
        and proof_debt_delta == 0
        and yielded_lemmas == 0
    )

    required_delta_similar = (
        required_delta_md.strip() == snapshot.current_required_delta_md.strip()
        or (required_delta_md.strip() and not snapshot.current_required_delta_md.strip())
    )

    theorem_skeleton_similar = (
        theorem_skeleton_md.strip() == snapshot.current_theorem_skeleton_md.strip()
        or not snapshot.current_theorem_skeleton_md.strip()
    )

    base_churn = (
        no_progress
        and same_family_and_blocker
        and required_delta_similar
        and theorem_skeleton_similar
    )

    repeated_signature = (
        snapshot.repeated_cohort_signature
        and snapshot.recent_current_line_repeated_signature_count >= 3
    )
    stale_line = (
        snapshot.current_line_exhausted
        or (snapshot.recent_current_line_replayable_gain <= 0 and snapshot.recent_current_line_accepts == 0)
    )

    return bool(
        base_churn
        and stale_line
        and (repeated_signature or recent_family_no_yield or recent_family_no_gain or recent_family_failing)
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
    # If delta provides a required_delta_md and snapshot doesn't have one, it's actionable
    if required_delta_md.strip() and not snapshot.current_required_delta_md.strip():
        return True
    if required_delta_md.strip() != snapshot.current_required_delta_md.strip():
        return True
    if next_cohort_plan.strip():
        return True
    # Only not actionable if both are empty and no other change
    if not snapshot.current_required_delta_md.strip() and not required_delta_md.strip():
        return False
    return False


def _detect_repeated_cohort_pattern(
    db: LimaCoreDB,
    problem_id: str,
    current_family_key: str,
    frontier_node_key: str,
    window: int = 10,
) -> tuple[bool, str, int]:
    if not current_family_key:
        return False, "", 0
    metrics = _recent_current_line_metrics(
        db,
        problem_id,
        current_family_key,
        frontier_node_key,
        recent_events=[],
        window=window,
    )
    signature = str(metrics["repeated_signature"] or "")
    count = int(metrics["repeated_signature_count"] or 0)
    return (count >= 3, signature, count)


def _recent_accept_count(events: list[dict[str, Any]], window: int = 10) -> int:
    """Count recent accepts in the event window."""
    recent = _recent_events(events, window)
    return sum(1 for e in recent if str(e.get("decision") or "") == "accepted")


def _recent_revert_count(events: list[dict[str, Any]], window: int = 10) -> int:
    """Count recent reverts in the event window."""
    recent = _recent_events(events, window)
    return sum(1 for e in recent if str(e.get("decision") or "") == "reverted")


def _current_line_replayable_gain_rate(
    recent_family_metrics: dict[str, Any],
    window: int = 10,
) -> float:
    """Calculate replayable gain rate per iteration in the recent window."""
    gain = recent_family_metrics.get("replayable_gain", 0)
    return gain / window if window > 0 else 0.0


def current_line_exhausted(snapshot: ControlSnapshot) -> bool:
    no_recent_progress = (
        snapshot.recent_current_line_replayable_gain <= 0
        and snapshot.recent_current_line_accepts == 0
        and snapshot.recent_current_line_proof_debt_delta >= 0
    )
    repeated_maintenance = (
        snapshot.recent_current_line_repeated_signature_count >= 3
        or snapshot.repeated_cohort_pattern_detected
    )
    failed_line = snapshot.recent_current_line_failed_cohorts >= 2 or snapshot.recent_current_line_failed_jobs >= 4
    return bool(snapshot.current_line_key and no_recent_progress and (repeated_maintenance or failed_line))


def materially_changed_required_delta(current: str, proposed: str) -> bool:
    """Check if proposed required_delta is materially different from current."""
    current_clean = current.strip().lower()
    proposed_clean = proposed.strip().lower()
    
    if not current_clean and proposed_clean:
        return True
    if current_clean == proposed_clean:
        return False
    
    # Check for substantial content differences (not just punctuation/whitespace)
    import re
    current_words = set(re.findall(r'\b\w+\b', current_clean))
    proposed_words = set(re.findall(r'\b\w+\b', proposed_clean))
    
    # If significant word overlap changed, it's material
    if not current_words:
        return bool(proposed_words)
    
    common = current_words & proposed_words
    total_unique = current_words | proposed_words
    
    if not total_unique:
        return False
    
    # If less than 50% word overlap, consider it materially changed
    similarity = len(common) / len(total_unique)
    return similarity < 0.5


def materially_changed_theorem_skeleton(current: str, proposed: str) -> bool:
    """Check if proposed theorem skeleton is materially different from current."""
    return materially_changed_required_delta(current, proposed)


def maintenance_churn_penalty(snapshot: ControlSnapshot) -> float:
    """Calculate penalty for repeated maintenance churn.
    
    Returns a penalty value (0.0 to 1.0) based on how much the current
    line is exhibiting maintenance churn behavior.
    """
    penalty = 0.0

    if snapshot.repeated_cohort_pattern_detected or snapshot.recent_current_line_repeated_signature_count >= 3:
        penalty += 0.4

    if snapshot.recent_current_line_replayable_gain <= 0:
        penalty += 0.3

    if snapshot.recent_current_line_reverts > snapshot.recent_current_line_accepts:
        penalty += 0.2

    if snapshot.current_line_exhausted:
        penalty += 0.3

    return min(penalty, 1.0)


def current_line_stagnant(snapshot: ControlSnapshot, threshold: int = 3) -> bool:
    """Check if the current line is stagnant based on recent metrics.
    
    A line is stagnant if:
    - No replayable gain in recent window
    - No yielded lemmas in recent window
    - Failed cohorts exceed threshold
    """
    no_gain = snapshot.recent_current_line_replayable_gain <= 0
    no_lemmas = snapshot.recent_current_line_yielded_lemmas == 0
    failing = snapshot.recent_current_line_failed_cohorts >= threshold
    repeated = snapshot.recent_current_line_repeated_signature_count >= threshold

    return no_gain and no_lemmas and (failing or repeated or snapshot.recent_current_line_accepts == 0)
