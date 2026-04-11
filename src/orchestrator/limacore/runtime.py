from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .artifacts import utc_now
from .control import build_control_snapshot
from .db import LimaCoreDB
from .frontier import proof_debt
from .solved import solved_checker


STATUS_PRIORITY = {
    "solved": 6,
    "failed": 5,
    "blocked": 4,
    "stalled": 3,
    "running": 2,
    "booting": 1,
    "paused": 0,
}


@dataclass(slots=True)
class RuntimeStatusView:
    status: str
    reason: str
    blocked_node_key: str = ""
    blocker_kind: str = ""
    blocker_summary: str = ""
    exhausted_family_key: str = ""
    suggested_family_key: str = ""
    stalled_iteration_window: int = 10
    stalled_since: str = ""
    last_gain_at: str = ""
    replayable_gain_rate: int = 0
    last_meaningful_change_at: str = ""
    # NEW: Current-line KPIs
    current_family_key: str = ""
    current_family_exhausted: bool = False
    repeated_cohort_pattern_detected: bool = False
    repeated_cohort_signature: str = ""
    recent_current_family_replayable_gain: int = 0
    recent_current_family_yielded_lemmas: int = 0
    recent_accept_count: int = 0
    recent_revert_count: int = 0


def detect_runtime_status(db: LimaCoreDB, problem_id: str, *, stall_window: int = 10) -> RuntimeStatusView:
    problem = db.get_problem(problem_id)
    if problem is None:
        raise KeyError(problem_id)
    stored_status = str(problem.get("runtime_status") or "")
    snapshot = build_control_snapshot(db, problem_id, window=stall_window)
    events = db.list_events(problem_id, limit=max(stall_window, 20))
    frontier = db.get_frontier_nodes(problem_id)
    solved = solved_checker(db, problem_id)
    last_meaningful_change = str(problem.get("last_gain_at") or problem.get("updated_at") or problem.get("created_at") or "")
    replayable_gain_rate = sum(int((event.get("score_delta") or {}).get("replayable_gain", 0)) for event in events[-stall_window:])
    # Helper dict for common current-line KPIs
    current_line_kpis = {
        "current_family_key": snapshot.current_family_key,
        "current_family_exhausted": snapshot.current_family_exhausted,
        "repeated_cohort_pattern_detected": snapshot.repeated_cohort_pattern_detected,
        "repeated_cohort_signature": snapshot.repeated_cohort_signature,
        "recent_current_family_replayable_gain": snapshot.recent_current_family_replayable_gain,
        "recent_current_family_yielded_lemmas": snapshot.recent_current_family_yielded_lemmas,
        "recent_accept_count": snapshot.recent_accept_count,
        "recent_revert_count": snapshot.recent_revert_count,
    }
    
    if solved.solved:
        return RuntimeStatusView(
            status="solved",
            reason="Solved: target theorem closed and replay check passed.",
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            last_gain_at=last_meaningful_change,
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            **current_line_kpis,
        )
    if str(problem.get("runtime_status") or "") == "failed":
        return RuntimeStatusView(
            status="failed",
            reason=str(problem.get("status_reason_md") or "Failed: unexpected internal error."),
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            **current_line_kpis,
        )
    if int(problem.get("autopilot_enabled", 1) or 0) == 0:
        return RuntimeStatusView(
            status="paused",
            reason=str(problem.get("status_reason_md") or "Paused: autopilot disabled."),
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            **current_line_kpis,
        )
    blocked_node = next((node for node in frontier if str(node.get("status") or "") == "blocked"), None)
    if stored_status == "running" and not events and blocked_node is None:
        return RuntimeStatusView(
            status="running",
            reason=str(problem.get("status_reason_md") or "Running: autopilot active."),
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            last_gain_at=str(problem.get("last_gain_at") or ""),
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            **current_line_kpis,
        )
    # FIXED: Blocked detection now uses recent current-family metrics
    current_family_recently_dead = (
        snapshot.recent_current_family_replayable_gain <= 0
        and snapshot.recent_current_family_yielded_lemmas == 0
        and snapshot.recent_current_family_failed_jobs >= 4
        and snapshot.recent_current_family_failed_cohorts >= 2
    )
    
    blocked_now = bool(
        blocked_node
        and (
            snapshot.current_family_exhausted  # Uses recent family metrics now
            or snapshot.same_blocker_persists
            or current_family_recently_dead  # KEY FIX: recent family deadness
            or (
                snapshot.recent_replayable_gain <= 0
                and snapshot.recent_proof_debt_delta >= 0
                and snapshot.recent_fracture_gain <= 0
                and snapshot.recent_current_family_replayable_gain <= 0  # Also check recent family
            )
        )
    )
    if blocked_now or (stored_status == "blocked" and blocked_node is not None):
        reason = str(problem.get("status_reason_md") or "Blocked: current frontier cannot advance.")
        # Add pattern info to reason if detected
        if snapshot.repeated_cohort_pattern_detected:
            reason += f" (Repeated pattern: {snapshot.repeated_cohort_signature})"
        return RuntimeStatusView(
            status="blocked",
            reason=reason,
            blocked_node_key=str(problem.get("blocked_node_key") or snapshot.blocked_node_key or ""),
            blocker_kind=str(problem.get("blocker_kind") or snapshot.blocker_kind or ""),
            blocker_summary=str(problem.get("status_reason_md") or snapshot.blocker_summary or ""),
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            **current_line_kpis,
        )
    # FIXED: Use recent current-family metrics for stall detection, not lifetime totals
    # This allows a currently dead line to be marked stalled even if the problem had earlier successes
    stalled_now = bool(
        snapshot.recent_replayable_gain <= 0
        and snapshot.recent_proof_debt_delta >= 0
        and snapshot.recent_fracture_gain <= 0
        and snapshot.recent_current_family_replayable_gain <= 0  # KEY FIX: recent family gain
        and snapshot.recent_current_family_yielded_lemmas == 0  # KEY FIX: recent family yield
        and snapshot.recent_current_family_failed_jobs >= 2  # KEY FIX: recent family failures
        and not blocked_now
    )
    if stalled_now:
        reason = f"Stalled: no replayable formal gain in the last {stall_window} iterations."
        # Add pattern detection info
        if snapshot.repeated_cohort_pattern_detected:
            reason += f" Repeated maintenance pattern: {snapshot.repeated_cohort_signature}."
        return RuntimeStatusView(
            status="stalled",
            reason=reason,
            stalled_iteration_window=stall_window,
            stalled_since=str(problem.get("stalled_since") or events[-1]["created_at"] if events else problem.get("created_at") or ""),
            last_gain_at=str(problem.get("last_gain_at") or ""),
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            **current_line_kpis,
        )
    if stored_status == "stalled" and str(problem.get("stalled_since") or ""):
        return RuntimeStatusView(
            status="stalled",
            reason=str(problem.get("status_reason_md") or f"Stalled: no replayable formal gain in the last {stall_window} iterations."),
            stalled_iteration_window=stall_window,
            stalled_since=str(problem.get("stalled_since") or ""),
            last_gain_at=str(problem.get("last_gain_at") or ""),
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            **current_line_kpis,
        )
    if stored_status == "running" and snapshot.recent_replayable_gain > 0:
        return RuntimeStatusView(
            status="running",
            reason=str(problem.get("status_reason_md") or "Running: autopilot active."),
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            last_gain_at=str(problem.get("last_gain_at") or ""),
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            **current_line_kpis,
        )
    if not events and not blocked_node:
        return RuntimeStatusView(
            status="booting",
            reason="Booting: creating normalized theorem and initial world line.",
            replayable_gain_rate=0,
            last_meaningful_change_at=str(problem.get("created_at") or ""),
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            **current_line_kpis,
        )
    # FIXED: Multiple live families only count as healthy if there is recent real movement
    # Use recent current-family signals, not just historical success
    has_recent_real_movement = (
        snapshot.recent_replayable_gain > 0
        or snapshot.recent_current_family_replayable_gain > 0  # Current family making progress
        or snapshot.recent_current_family_accepts > 0  # Current family has recent accepts
    )
    
    if snapshot.live_family_count >= 2 and has_recent_real_movement:
        return RuntimeStatusView(
            status="running",
            reason="Running: autopilot active with recent progress.",
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            last_gain_at=str(problem.get("last_gain_at") or ""),
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            **current_line_kpis,
        )
    
    # Only say "multiple non-stale world lines are active" if there's actual recent activity
    if snapshot.live_family_count >= 2 and not snapshot.current_family_exhausted and has_recent_real_movement:
        return RuntimeStatusView(
            status="running",
            reason="Running: multiple non-stale world lines are active.",
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            **current_line_kpis,
        )
    if snapshot.current_family_exhausted and blocked_node is not None:
        return RuntimeStatusView(
            status="blocked",
            reason=str(problem.get("status_reason_md") or f"Blocked: family {snapshot.current_family_key} is exhausted."),
            blocked_node_key=snapshot.blocked_node_key,
            blocker_kind=snapshot.blocker_kind,
            blocker_summary=snapshot.blocker_summary,
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            **current_line_kpis,
        )
    if stored_status == "running" and (events or frontier) and snapshot.recent_replayable_gain > 0:
        return RuntimeStatusView(
            status="running",
            reason=str(problem.get("status_reason_md") or "Running: autopilot active."),
            last_gain_at=str(problem.get("last_gain_at") or ""),
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            **current_line_kpis,
        )
    runtime_status = str(problem.get("runtime_status") or "")
    if runtime_status == "booting":
        return RuntimeStatusView(
            status="booting",
            reason="Booting: creating normalized theorem and initial world line.",
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=str(problem.get("created_at") or ""),
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            **current_line_kpis,
        )
    return RuntimeStatusView(
        status="stalled" if snapshot.recent_replayable_gain <= 0 else "running",
        reason="Stalled: no replayable formal gain in the last window." if snapshot.recent_replayable_gain <= 0 else "Running: autopilot active.",
        replayable_gain_rate=replayable_gain_rate,
        last_meaningful_change_at=last_meaningful_change,
        last_gain_at=str(problem.get("last_gain_at") or ""),
        exhausted_family_key=snapshot.exhausted_family_key,
        suggested_family_key=snapshot.suggested_family_key,
        **current_line_kpis,
    )


def persist_runtime_status(db: LimaCoreDB, problem_id: str, *, stall_window: int = 10) -> dict[str, Any]:
    view = detect_runtime_status(db, problem_id, stall_window=stall_window)
    since_timestamp = utc_now()
    current = db.get_problem(problem_id) or {}
    if str(current.get("runtime_status") or "") == view.status and str(current.get("since_timestamp") or ""):
        since_timestamp = str(current.get("since_timestamp"))
    update = db.update_problem_runtime(
        problem_id,
        runtime_status=view.status,
        status_reason_md=view.reason if view.status != "blocked" else view.blocker_summary or view.reason,
        blocked_node_key=view.blocked_node_key,
        blocker_kind=view.blocker_kind,
        exhausted_family_key=view.exhausted_family_key,
        exhausted_family_since=utc_now() if view.exhausted_family_key else "",
        stalled_since=view.stalled_since if view.status == "stalled" else "",
        last_gain_at=view.last_gain_at if view.last_gain_at else str(current.get("last_gain_at") or ""),
        since_timestamp=since_timestamp,
    )
    assert update is not None
    return update


def frontier_debt_and_jobs(db: LimaCoreDB, problem_id: str) -> tuple[int, int]:
    frontier = db.get_frontier_nodes(problem_id)
    jobs = db.list_jobs(problem_id)
    return proof_debt(frontier), sum(1 for job in jobs if str(job.get("status") or "") in {"queued", "running"})
