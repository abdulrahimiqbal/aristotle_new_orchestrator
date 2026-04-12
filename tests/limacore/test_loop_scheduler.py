"""Tests for LimaCore scheduler pass behavior."""

from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import run_scheduler_pass


def _collect_processed_problem_ids(db: LimaCoreDB, monkeypatch) -> list[str]:  # type: ignore[no-untyped-def]
    calls: list[str] = []

    def fake_run_iteration(self, problem_slug_or_id: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(str(problem_slug_or_id))
        return {"status": "ok"}

    monkeypatch.setattr("orchestrator.limacore.loop.LimaCoreLoop.run_iteration", fake_run_iteration)
    run_scheduler_pass(db)
    return calls


def test_scheduler_pass_processes_seeded_problems(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "test.db"))
    db.initialize()
    calls = _collect_processed_problem_ids(db, monkeypatch)
    slugs = {str((db.get_problem(problem_id) or {}).get("slug") or "") for problem_id in calls}
    assert "collatz" in slugs
    assert "inward-compression-conjecture" in slugs


def test_scheduler_pass_runs_new_active_problem_immediately(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "test.db"))
    db.initialize()
    _collect_processed_problem_ids(db, monkeypatch)
    problem_id, _ = db.create_problem(
        slug="new-active-problem",
        title="New Active Problem",
        statement_md="Test statement",
        runtime_status="running",
        autopilot_enabled=True,
    )
    calls = _collect_processed_problem_ids(db, monkeypatch)
    assert problem_id in calls


def test_scheduler_pass_skips_paused_solved_failed(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "test.db"))
    db.initialize()
    paused_id, _ = db.create_problem(
        slug="paused-problem",
        title="Paused Problem",
        statement_md="Test statement",
        runtime_status="paused",
        autopilot_enabled=False,
    )
    solved_id, _ = db.create_problem(
        slug="solved-problem",
        title="Solved Problem",
        statement_md="Test statement",
        runtime_status="solved",
        autopilot_enabled=False,
    )
    failed_id, _ = db.create_problem(
        slug="failed-problem",
        title="Failed Problem",
        statement_md="Test statement",
        runtime_status="failed",
        autopilot_enabled=True,
    )
    calls = _collect_processed_problem_ids(db, monkeypatch)
    assert paused_id not in calls
    assert solved_id not in calls
    assert failed_id not in calls
