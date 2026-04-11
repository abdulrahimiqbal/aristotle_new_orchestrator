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
    if solved.solved:
        return RuntimeStatusView(
            status="solved",
            reason="Solved: target theorem closed and replay check passed.",
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            last_gain_at=last_meaningful_change,
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
        )
    if str(problem.get("runtime_status") or "") == "failed":
        return RuntimeStatusView(
            status="failed",
            reason=str(problem.get("status_reason_md") or "Failed: unexpected internal error."),
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
        )
    if int(problem.get("autopilot_enabled", 1) or 0) == 0:
        return RuntimeStatusView(
            status="paused",
            reason=str(problem.get("status_reason_md") or "Paused: autopilot disabled."),
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
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
        )
    blocked_now = bool(
        blocked_node
        and (
            snapshot.current_family_exhausted
            or snapshot.same_blocker_persists
            or snapshot.current_family_failed_jobs >= 4
            or snapshot.current_family_failed_cohorts >= 2
            or (
                snapshot.recent_replayable_gain <= 0
                and snapshot.recent_proof_debt_delta >= 0
                and snapshot.recent_fracture_gain <= 0
            )
        )
    )
    if blocked_now or (stored_status == "blocked" and blocked_node is not None):
        return RuntimeStatusView(
            status="blocked",
            reason=str(problem.get("status_reason_md") or "Blocked: current frontier cannot advance."),
            blocked_node_key=str(problem.get("blocked_node_key") or snapshot.blocked_node_key or ""),
            blocker_kind=str(problem.get("blocker_kind") or snapshot.blocker_kind or ""),
            blocker_summary=str(problem.get("status_reason_md") or snapshot.blocker_summary or ""),
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
        )
    stalled_now = bool(
        snapshot.recent_replayable_gain <= 0
        and snapshot.recent_proof_debt_delta >= 0
        and snapshot.recent_fracture_gain <= 0
        and snapshot.yielded_lemmas == 0
        and snapshot.current_family_failed_jobs >= 2
        and not blocked_now
    )
    if stalled_now:
        return RuntimeStatusView(
            status="stalled",
            reason=f"Stalled: no replayable formal gain in the last {stall_window} iterations.",
            stalled_iteration_window=stall_window,
            stalled_since=str(problem.get("stalled_since") or events[-1]["created_at"] if events else problem.get("created_at") or ""),
            last_gain_at=str(problem.get("last_gain_at") or ""),
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
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
        )
    if not events and not blocked_node:
        return RuntimeStatusView(
            status="booting",
            reason="Booting: creating normalized theorem and initial world line.",
            replayable_gain_rate=0,
            last_meaningful_change_at=str(problem.get("created_at") or ""),
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
        )
    if snapshot.live_family_count >= 2 and snapshot.recent_replayable_gain > 0:
        return RuntimeStatusView(
            status="running",
            reason="Running: autopilot active.",
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            last_gain_at=str(problem.get("last_gain_at") or ""),
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
        )
    if snapshot.live_family_count >= 2 and not snapshot.current_family_exhausted:
        return RuntimeStatusView(
            status="running",
            reason="Running: multiple non-stale world lines are active.",
            replayable_gain_rate=replayable_gain_rate,
            last_meaningful_change_at=last_meaningful_change,
            exhausted_family_key=snapshot.exhausted_family_key,
            suggested_family_key=snapshot.suggested_family_key,
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
        )
    return RuntimeStatusView(
        status="stalled" if snapshot.recent_replayable_gain <= 0 else "running",
        reason="Stalled: no replayable formal gain in the last window." if snapshot.recent_replayable_gain <= 0 else "Running: autopilot active.",
        replayable_gain_rate=replayable_gain_rate,
        last_meaningful_change_at=last_meaningful_change,
        last_gain_at=str(problem.get("last_gain_at") or ""),
        exhausted_family_key=snapshot.exhausted_family_key,
        suggested_family_key=snapshot.suggested_family_key,
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
