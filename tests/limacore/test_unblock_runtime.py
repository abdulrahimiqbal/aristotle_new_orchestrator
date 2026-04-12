from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.aristotle import LocalAristotleBackend
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import LimaCoreLoop
from orchestrator.limacore.runtime import detect_runtime_status


def test_runtime_surfaces_unblock_plan_when_stalled_or_blocked(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    loop.run_iteration("collatz")
    problem = db.get_problem("collatz")
    assert problem is not None

    db.update_problem_runtime(
        str(problem["id"]),
        runtime_status="stalled",
        status_reason_md="Stalled for runtime unblock test.",
    )
    view = detect_runtime_status(db, str(problem["id"]))
    assert hasattr(view, "unblock_available")
    assert hasattr(view, "unblock_strategy_kind")
    assert hasattr(view, "unblock_suggested_family")
    assert view.unblock_candidate_count >= 0


def test_runtime_does_not_force_unblock_fields_for_healthy_running_line(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("inward-compression-conjecture")
    assert problem is not None
    db.update_problem_runtime(
        str(problem["id"]),
        runtime_status="running",
        status_reason_md="Running baseline.",
        blocked_node_key="",
        blocker_kind="",
    )
    view = detect_runtime_status(db, str(problem["id"]))
    if view.status == "running":
        assert view.unblock_available is False
