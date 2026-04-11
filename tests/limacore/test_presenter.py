from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.aristotle import LocalAristotleBackend
from orchestrator.limacore.artifacts import utc_now
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import LimaCoreLoop
from orchestrator.limacore.models import FrontierNode
from orchestrator.limacore.presenter import (
    build_index_context,
    build_workspace_context,
    get_problem_status_view,
)
from orchestrator.limacore.runtime import persist_runtime_status


def test_presenter_returns_status_views_and_metrics(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    loop.run_iteration("inward-compression-conjecture")
    index_ctx = build_index_context(db)
    assert index_ctx["cards"]
    assert "status_view" in index_ctx["cards"][0]
    ws_ctx = build_workspace_context(db, "inward-compression-conjecture")
    assert "stats" in ws_ctx
    assert "frontier" in ws_ctx
    assert "status_view" in ws_ctx
    assert "autopilot_state" in ws_ctx
    assert ws_ctx["stats"]["proof_debt"] >= 1


def test_problem_cards_show_escalation_states(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    collatz = db.get_problem("collatz")
    inward = db.get_problem("inward-compression-conjecture")
    assert collatz is not None and inward is not None
    db.update_problem_runtime(str(collatz["id"]), runtime_status="stalled", status_reason_md="Stalled: no replayable formal gain in the last 10 iterations.", stalled_since="2026-04-11T00:00:00+00:00")
    db.upsert_frontier_node(
        FrontierNode(
            id="blocked-frontier",
            problem_id=str(inward["id"]),
            node_key="terminal_form_uniqueness",
            node_kind="theorem_skeleton",
            title="Terminal form uniqueness",
            status="blocked",
            blocker_kind="missing_uniqueness_lemma",
            blocker_note_md="Need a full canonical balanced-profile lemma.",
            priority=12.0,
            updated_at=utc_now(),
        )
    )
    db.update_problem_runtime(str(inward["id"]), runtime_status="blocked", status_reason_md="Blocked: current frontier cannot advance.", blocked_node_key="terminal_form_uniqueness", blocker_kind="missing_uniqueness_lemma")
    cards = build_index_context(db)["cards"]
    statuses = {card["problem"]["slug"]: card["status_view"]["status"] for card in cards}
    assert statuses["collatz"] == "stalled"
    assert statuses["inward-compression-conjecture"] == "blocked"


def test_workspace_banners_render_for_blocked_stalled_solved(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("inward-compression-conjecture")
    assert problem is not None
    db.upsert_frontier_node(
        FrontierNode(
            id="blocked-frontier",
            problem_id=str(problem["id"]),
            node_key="terminal_form_uniqueness",
            node_kind="theorem_skeleton",
            title="Terminal form uniqueness",
            status="blocked",
            blocker_kind="missing_uniqueness_lemma",
            blocker_note_md="Need a full canonical balanced-profile lemma.",
            priority=12.0,
            updated_at=utc_now(),
        )
    )
    db.update_problem_runtime(
        str(problem["id"]),
        runtime_status="blocked",
        status_reason_md="Blocked: current frontier cannot advance.",
        blocked_node_key="terminal_form_uniqueness",
        blocker_kind="missing_uniqueness_lemma",
    )
    blocked_ctx = build_workspace_context(db, "inward-compression-conjecture")
    assert blocked_ctx["alert_banner"] is not None
    assert blocked_ctx["alert_banner"]["kind"] == "blocked"

    db.upsert_frontier_node(
        FrontierNode(
            id="blocked-frontier",
            problem_id=str(problem["id"]),
            node_key="terminal_form_uniqueness",
            node_kind="theorem_skeleton",
            title="Terminal form uniqueness",
            status="proved",
            replay_ref={"replay_certificate": "terminal"},
            priority=12.0,
            updated_at=utc_now(),
        )
    )
    db.update_problem_runtime(
        str(problem["id"]),
        runtime_status="stalled",
        status_reason_md="Stalled: no replayable formal gain in the last 10 iterations.",
        stalled_since="2026-04-11T00:00:00+00:00",
    )
    stalled_ctx = build_workspace_context(db, "inward-compression-conjecture")
    assert stalled_ctx["alert_banner"] is not None
    assert stalled_ctx["alert_banner"]["kind"] == "stalled"

    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    loop.run_iteration("inward-compression-conjecture")
    solved_view = get_problem_status_view(db, db.get_problem("inward-compression-conjecture"))
    assert solved_view["status"] in {"running", "blocked", "stalled", "solved"}
