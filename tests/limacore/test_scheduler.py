from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from orchestrator import app as app_mod
from orchestrator.limacore.artifacts import utc_now
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import limacore_loop, run_scheduler_pass
from orchestrator.limacore.presenter import build_index_context, build_workspace_context
from orchestrator.limacore.runtime import detect_runtime_status


async def _noop_loop(*_args, **_kwargs) -> None:
    return None


def _set_scheduler_state_stale(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE limacore_scheduler_state
            SET last_pass_started_at = ?,
                last_pass_completed_at = ?,
                last_error_at = ?,
                last_error_md = ?,
                pass_count = 3,
                failure_count = 1,
                currently_running = 0
            WHERE scheduler_name = 'limacore_autopilot'
            """,
            (
                "2026-04-11T00:00:00+00:00",
                "2026-04-11T00:00:00+00:00",
                "2026-04-11T00:00:00+00:00",
                "stale heartbeat",
            ),
        )
        conn.commit()


def test_scheduler_pass_isolated_failures_and_skips(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    collatz = db.get_problem("collatz")
    inward = db.get_problem("inward-compression-conjecture")
    assert collatz is not None and inward is not None
    skipped_id, _ = db.create_problem(
        slug="skipped-problem",
        title="Skipped problem",
        statement_md="A problem that should not run.",
        domain="number theory",
    )
    db.update_problem_runtime(
        skipped_id,
        runtime_status="paused",
        status_reason_md="Paused for skip test.",
        autopilot_enabled=False,
    )

    calls: list[str] = []

    def fake_run_iteration(self, problem_slug_or_id: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(problem_slug_or_id)
        if problem_slug_or_id == str(collatz["id"]):
            raise RuntimeError("boom")
        return {"status": "ok"}

    monkeypatch.setattr("orchestrator.limacore.loop.LimaCoreLoop.run_iteration", fake_run_iteration)

    summary = run_scheduler_pass(db)

    assert summary["problems_seen"] == 2
    assert summary["problems_ran"] == 1
    assert summary["problems_failed"] == 1
    assert str(collatz["id"]) in calls
    assert str(inward["id"]) in calls
    assert skipped_id not in calls
    failed_problem = db.get_problem(str(collatz["id"]))
    assert failed_problem is not None
    assert failed_problem["runtime_status"] == "failed"
    events = db.list_events(str(collatz["id"]))
    assert any(event["event_type"] == "autopilot_iteration_failed" for event in events)
    scheduler = db.get_scheduler_state()
    assert int(scheduler["pass_count"]) >= 1
    assert int(scheduler["failure_count"]) >= 1
    assert scheduler["last_error_md"]


def test_scheduler_loop_recovers_after_scheduler_error(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    calls: list[str] = []
    sleep_calls = 0

    def fake_run_scheduler_pass(db_obj, *, backend=None):  # type: ignore[no-untyped-def]
        calls.append("pass")
        if len(calls) == 1:
            raise RuntimeError("scheduler boom")
        return {"problems_seen": 0, "problems_ran": 0, "problems_failed": 0, "started_at": utc_now(), "completed_at": utc_now()}

    async def fake_sleep(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls >= 2:
            raise asyncio.CancelledError

    monkeypatch.setattr("orchestrator.limacore.loop.run_scheduler_pass", fake_run_scheduler_pass)
    monkeypatch.setattr("orchestrator.limacore.loop.asyncio.sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(limacore_loop(db, interval_sec=1))

    assert len(calls) >= 2
    scheduler = db.get_scheduler_state()
    assert int(scheduler["failure_count"]) >= 1
    assert "Autopilot scheduler error" in str(scheduler["last_error_md"])


def test_scheduler_heartbeat_controls_runtime_and_ui(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    assert problem is not None
    problem_id = str(problem["id"])

    db.update_problem_runtime(problem_id, runtime_status="running", status_reason_md="Running: autopilot active.")
    db.record_scheduler_pass_start()
    db.record_scheduler_pass_complete(last_successful_problem_id=problem_id)

    fresh_status = detect_runtime_status(db, problem_id)
    assert fresh_status.status == "running"
    assert fresh_status.scheduler_healthy is True
    assert fresh_status.scheduler_stale is False

    _set_scheduler_state_stale(db.path)

    stale_status = detect_runtime_status(db, problem_id)
    assert stale_status.status == "stalled"
    assert stale_status.scheduler_stale is True
    assert stale_status.scheduler_healthy is False

    workspace = build_workspace_context(db, "collatz")
    assert workspace["status_view"]["scheduler_stale"] is True
    assert workspace["alert_banner"] is not None
    assert workspace["alert_banner"]["kind"] == "scheduler_unhealthy"
    index_ctx = build_index_context(db)
    card = next(card for card in index_ctx["cards"] if str(card["problem"]["slug"]) == "collatz")
    assert "unhealthy" in card["summary_text"].lower()


def test_limacore_ops_endpoint_exposes_scheduler_state(tmp_path: Path, monkeypatch) -> None:
    limacore_db = LimaCoreDB(str(tmp_path / "limacore.db"))
    limacore_db.initialize()
    monkeypatch.setattr(app_mod, "limacore_db", limacore_db)
    monkeypatch.setattr(app_mod, "manager_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "shadow_global_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "supershadow_global_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "lima_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "limacore_loop", _noop_loop)

    limacore_db.record_scheduler_pass_start()
    limacore_db.record_scheduler_pass_complete(last_successful_problem_id="collatz")
    limacore_db.record_scheduler_error("scheduler wobble")
    collatz = limacore_db.get_problem("collatz")
    assert collatz is not None
    limacore_db.append_event(str(collatz["id"]), "manager_tick", "planned", summary_md="manager summary")

    with TestClient(app_mod.app) as client:
        resp = client.get("/api/limacore/ops")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["scheduler_state"]["scheduler_name"] == "limacore_autopilot"
        assert payload["scheduler_state"]["pass_count"] >= 1
        assert payload["scheduler_state"]["last_error_md"] == "scheduler wobble"
        assert "scheduler_headline" in payload
        assert "manager_latest_events" in payload
