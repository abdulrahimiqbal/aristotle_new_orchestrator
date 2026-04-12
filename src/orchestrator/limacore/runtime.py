from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from orchestrator import config as app_config

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
    current_family_key: str = ""
    current_family_exhausted: bool = False
    repeated_cohort_pattern_detected: bool = False
    repeated_cohort_signature: str = ""
    recent_current_family_replayable_gain: int = 0
    recent_current_family_yielded_lemmas: int = 0
    recent_accept_count: int = 0
    recent_revert_count: int = 0
    scheduler_status: str = "not_started"
    scheduler_healthy: bool = True
    scheduler_stale: bool = False
    scheduler_initialized: bool = False
    scheduler_last_pass_started_at: str = ""
    scheduler_last_pass_completed_at: str = ""
    scheduler_last_successful_problem_id: str = ""
    scheduler_last_error_at: str = ""
    scheduler_last_error_md: str = ""
    scheduler_pass_count: int = 0
    scheduler_failure_count: int = 0
    scheduler_current_problem_id: str = ""
    scheduler_current_pass_problem_count: int = 0
    scheduler_age_seconds: int = 0
    scheduler_expected_next_pass_at: str = ""
    scheduler_status_reason: str = ""
    scheduler_name: str = "limacore_autopilot"


def _parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _scheduler_health_reason(state: dict[str, Any], *, stale: bool, initialized: bool, age_seconds: int) -> str:
    if stale:
        last_completed = str(state.get("last_pass_completed_at") or "never")
        return f"Autopilot unhealthy: scheduler heartbeat stale. Last completed pass: {last_completed}. Age: {age_seconds}s."
    if initialized:
        if int(state.get("failure_count") or 0) > 0 and str(state.get("last_error_md") or ""):
            return f"Autopilot healthy. Last scheduler error: {state['last_error_md']}"
        return "Autopilot healthy: scheduler heartbeat fresh."
    return "Autopilot awaiting first scheduler pass."


def get_scheduler_health_view(
    db: LimaCoreDB,
    *,
    scheduler_name: str = "limacore_autopilot",
    interval_sec: int | None = None,
) -> dict[str, Any]:
    state = db.get_scheduler_state(scheduler_name)
    interval = max(1, int(interval_sec or app_config.LIMACORE_LOOP_INTERVAL_SEC or 300))
    last_started = _parse_timestamp(str(state.get("last_pass_started_at") or ""))
    last_completed = _parse_timestamp(str(state.get("last_pass_completed_at") or ""))
    last_activity = last_completed or last_started
    now = datetime.now(timezone.utc)
    age_seconds = int((now - last_activity).total_seconds()) if last_activity else 0
    initialized = bool(
        int(state.get("pass_count") or 0) > 0
        or str(state.get("last_pass_started_at") or "")
        or str(state.get("last_pass_completed_at") or "")
    )
    stale = bool(last_activity and age_seconds > (2 * interval))
    healthy = not stale
    status = "not_started"
    if stale:
        status = "stale"
    elif initialized and int(state.get("currently_running") or 0):
        status = "running"
    elif initialized:
        status = "healthy"
    next_pass_at = ""
    if last_completed is not None:
        next_pass_at = (last_completed + timedelta(seconds=interval)).astimezone(timezone.utc).isoformat()
    elif last_started is not None:
        next_pass_at = (last_started + timedelta(seconds=interval)).astimezone(timezone.utc).isoformat()
    return {
        "scheduler_name": scheduler_name,
        "scheduler_status": status,
        "scheduler_healthy": healthy,
        "scheduler_stale": stale,
        "scheduler_initialized": initialized,
        "scheduler_last_pass_started_at": str(state.get("last_pass_started_at") or ""),
        "scheduler_last_pass_completed_at": str(state.get("last_pass_completed_at") or ""),
        "scheduler_last_successful_problem_id": str(state.get("last_successful_problem_id") or ""),
        "scheduler_last_error_at": str(state.get("last_error_at") or ""),
        "scheduler_last_error_md": str(state.get("last_error_md") or ""),
        "scheduler_pass_count": int(state.get("pass_count") or 0),
        "scheduler_failure_count": int(state.get("failure_count") or 0),
        "scheduler_current_problem_id": str(state.get("current_problem_id") or ""),
        "scheduler_current_pass_problem_count": int(state.get("current_pass_problem_count") or 0),
        "scheduler_age_seconds": age_seconds,
        "scheduler_expected_next_pass_at": next_pass_at,
        "scheduler_status_reason": _scheduler_health_reason(state, stale=stale, initialized=initialized, age_seconds=age_seconds),
    }


def detect_runtime_status(db: LimaCoreDB, problem_id: str, *, stall_window: int = 10) -> RuntimeStatusView:
    problem = db.get_problem(problem_id)
    if problem is None:
        raise KeyError(problem_id)

    stored_status = str(problem.get("runtime_status") or "")
    snapshot = build_control_snapshot(db, problem_id, window=stall_window)
    events = db.list_events(problem_id, limit=max(stall_window, 20))
    frontier = db.get_frontier_nodes(problem_id)
    solved = solved_checker(db, problem_id)
    last_meaningful_change = str(
        problem.get("last_gain_at") or problem.get("updated_at") or problem.get("created_at") or ""
    )
    replayable_gain_rate = sum(int((event.get("score_delta") or {}).get("replayable_gain", 0)) for event in events[-stall_window:])
    scheduler_view = get_scheduler_health_view(db, interval_sec=app_config.LIMACORE_LOOP_INTERVAL_SEC)
    current_line_kpis = {
        "current_family_key": snapshot.current_family_key,
        "current_family_exhausted": snapshot.current_family_exhausted,
        "repeated_cohort_pattern_detected": snapshot.repeated_cohort_pattern_detected,
        "repeated_cohort_signature": snapshot.repeated_cohort_signature,
        "recent_current_family_replayable_gain": snapshot.recent_current_line_replayable_gain,
        "recent_current_family_yielded_lemmas": snapshot.recent_current_line_yielded_lemmas,
        "recent_accept_count": snapshot.recent_current_line_accepts,
        "recent_revert_count": snapshot.recent_current_line_reverts,
    }

    def _view(**kwargs: Any) -> RuntimeStatusView:
        return RuntimeStatusView(
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            **scheduler_view,
            **current_line_kpis,
            **kwargs,
        )

    if solved.solved:
        return _view(
            status="solved",
            reason="Solved: target theorem closed and replay check passed.",
        )

    if str(problem.get("runtime_status") or "") == "failed":
        return _view(
            status="failed",
            reason=str(problem.get("status_reason_md") or "Failed: unexpected internal error."),
        )

    if int(problem.get("autopilot_enabled", 1) or 0) == 0:
        return _view(
            status="paused",
            reason=str(problem.get("status_reason_md") or "Paused: autopilot disabled."),
        )

    blocked_node = next((node for node in frontier if str(node.get("status") or "") == "blocked"), None)
    current_line_recently_dead = (
        snapshot.recent_current_line_replayable_gain <= 0
        and snapshot.recent_current_line_yielded_lemmas == 0
        and snapshot.recent_current_line_failed_jobs >= 4
        and snapshot.recent_current_line_failed_cohorts >= 2
    )
    line_is_stale = snapshot.current_line_exhausted or current_line_recently_dead

    blocked_now = bool(
        blocked_node
        and (
            line_is_stale
            or snapshot.same_blocker_persists
            or (
                snapshot.recent_replayable_gain <= 0
                and snapshot.recent_proof_debt_delta >= 0
                and snapshot.recent_fracture_gain <= 0
                and snapshot.recent_current_line_replayable_gain <= 0
            )
        )
    )
    if blocked_now or (stored_status == "blocked" and blocked_node is not None):
        reason = str(problem.get("status_reason_md") or "Blocked: current frontier cannot advance.")
        if snapshot.repeated_cohort_pattern_detected:
            reason += f" (Repeated pattern: {snapshot.repeated_cohort_signature})"
        return _view(
            status="blocked",
            reason=reason,
            blocked_node_key=str(problem.get("blocked_node_key") or snapshot.blocked_node_key or ""),
            blocker_kind=str(problem.get("blocker_kind") or snapshot.blocker_kind or ""),
            blocker_summary=str(problem.get("status_reason_md") or snapshot.blocker_summary or ""),
        )

    if scheduler_view["scheduler_stale"]:
        reason = scheduler_view["scheduler_status_reason"]
        return _view(
            status="stalled",
            reason=reason,
            stalled_iteration_window=stall_window,
            stalled_since=str(problem.get("stalled_since") or events[-1]["created_at"] if events else problem.get("created_at") or ""),
            last_gain_at=str(problem.get("last_gain_at") or ""),
        )

    if stored_status == "running" and not line_is_stale:
        return _view(
            status="running",
            reason=str(problem.get("status_reason_md") or "Running: autopilot active."),
            last_gain_at=str(problem.get("last_gain_at") or ""),
        )

    runtime_status = str(problem.get("runtime_status") or "")
    if runtime_status == "booting":
        return _view(
            status="booting",
            reason="Booting: creating normalized theorem and initial world line.",
            last_meaningful_change_at=str(problem.get("created_at") or ""),
        )

    if runtime_status == "stalled" and not line_is_stale:
        return _view(
            status="stalled",
            reason=str(problem.get("status_reason_md") or "Stalled: no replayable formal gain in the last window."),
            stalled_iteration_window=stall_window,
            stalled_since=str(problem.get("stalled_since") or events[-1]["created_at"] if events else problem.get("created_at") or ""),
            last_gain_at=str(problem.get("last_gain_at") or ""),
        )

    has_recent_real_movement = (
        snapshot.recent_replayable_gain > 0
        or snapshot.recent_current_line_replayable_gain > 0
        or snapshot.recent_current_line_accepts > 0
    )

    if snapshot.live_family_count >= 2 and has_recent_real_movement:
        return _view(
            status="running",
            reason="Running: autopilot active with recent progress.",
            last_gain_at=str(problem.get("last_gain_at") or ""),
        )

    if snapshot.live_family_count >= 2 and not line_is_stale and has_recent_real_movement:
        return _view(
            status="running",
            reason="Running: multiple non-stale world lines are active.",
        )

    if line_is_stale and blocked_node is not None:
        return _view(
            status="blocked",
            reason=str(problem.get("status_reason_md") or f"Blocked: current line {snapshot.current_line_key} is exhausted."),
            blocked_node_key=snapshot.blocked_node_key,
            blocker_kind=snapshot.blocker_kind,
            blocker_summary=snapshot.blocker_summary,
        )

    if stored_status == "running" and (events or frontier) and not line_is_stale:
        return _view(
            status="running",
            reason=str(problem.get("status_reason_md") or "Running: autopilot active."),
            last_gain_at=str(problem.get("last_gain_at") or ""),
        )

    return _view(
        status="stalled" if line_is_stale else "running",
        reason=(
            "Stalled: no replayable formal gain on the current line in the last window."
            if line_is_stale
            else "Running: autopilot active."
        ),
        last_gain_at=str(problem.get("last_gain_at") or ""),
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
