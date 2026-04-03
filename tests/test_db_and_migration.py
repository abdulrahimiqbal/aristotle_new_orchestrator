from __future__ import annotations

import sqlite3
from pathlib import Path

from orchestrator.db import Database


def test_initialize_creates_ledger_and_parsed_columns(tmp_path: Path) -> None:
    path = tmp_path / "o.db"
    db = Database(str(path))
    db.initialize()
    conn = sqlite3.connect(str(path))
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='lemma_ledger'"
        )
        assert cur.fetchone() is not None
        cur = conn.execute("PRAGMA table_info(experiments)")
        cols = {r[1] for r in cur.fetchall()}
        assert "parsed_proved_lemmas_json" in cols
        assert "parsed_blockers_json" in cols
        assert "result_structured_json" in cols
        assert "parse_source" in cols
        assert "parse_warnings_json" in cols
        cur = conn.execute("PRAGMA table_info(campaigns)")
        ccols = {r[1] for r in cur.fetchall()}
        assert "problem_map_json" in ccols
        assert "problem_refs_json" in ccols
        assert "research_packet_json" in ccols
        assert "mathlib_knowledge" in ccols
        cur = conn.execute("PRAGMA table_info(experiments)")
        ecols = {r[1] for r in cur.fetchall()}
        assert "move_kind" in ecols
        assert "move_note" in ecols
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='campaign_map_node_acks'"
        )
        assert cur.fetchone() is not None
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='shadow_epistemic_state'"
        )
        assert cur.fetchone() is not None
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='shadow_global_state'"
        )
        assert cur.fetchone() is not None
        cur = conn.execute("PRAGMA table_info(shadow_global_hypothesis)")
        gh_cols = {r[1] for r in cur.fetchall()}
        assert "score_0_100" in gh_cols
        assert "groundability_tier" in gh_cols
        assert "kill_test" in gh_cols
        uv = conn.execute("PRAGMA user_version").fetchone()[0]
        assert int(uv) >= 10
    finally:
        conn.close()


def test_shadow_tables_commit_and_promote(tmp_path: Path) -> None:
    root = tmp_path / "ws"
    db = Database(str(tmp_path / "s.db"))
    db.initialize()
    cid = db.create_campaign(
        "test collatz angle",
        workspace_root=str(root),
        workspace_template="minimal",
    )
    tid = db.add_targets(cid, ["prove a toy lemma"])[0]
    db.ensure_shadow_state_row(cid)
    run_id = db.shadow_commit_run(
        cid,
        trigger_kind="manual",
        summary="probe",
        response_obj={"ok": True},
        new_stance_json='{"summary": "x"}',
        new_policy_json='{"exploration_bias": 0.9}',
        hypotheses=[
            {
                "kind": "exploration",
                "title": "toy",
                "body_md": "body",
                "lean_snippet": "",
                "evidence": [{"experiment_id": None, "target_id": tid, "note": "seed"}],
            }
        ],
        promotions=[
            {
                "kind": "new_experiment",
                "target_id": tid,
                "objective": "Try a bounded search",
                "move_kind": "explore",
                "move_note": "shadow:test",
            }
        ],
    )
    assert run_id
    promos = db.list_shadow_promotion_requests(cid, status="pending")
    assert len(promos) == 1
    pid = promos[0]["id"]
    ok, msg, extra = db.apply_shadow_promotion(pid)
    assert ok, msg
    assert "experiment_id" in extra


def test_get_experiment_for_submit_joins_workspace(tmp_path: Path) -> None:
    root = tmp_path / "ws"
    db = Database(str(tmp_path / "e.db"))
    db.initialize()
    cid = db.create_campaign(
        "hello",
        workspace_root=str(root),
        workspace_template="minimal",
    )
    tid = db.add_targets(cid, ["lemma"])[0]
    eid = db.create_experiment(
        cid, tid, "try a proof", move_kind="explore", move_note="t"
    )
    row = db.get_experiment_for_submit(eid)
    assert row is not None
    assert row["status"] == "pending"
    assert row["workspace_dir"]
    assert cid in str(row["workspace_dir"])


def test_create_campaign_per_workspace_dir(tmp_path: Path) -> None:
    root = tmp_path / "wsroot"
    db = Database(str(tmp_path / "c.db"))
    db.initialize()
    cid = db.create_campaign(
        "hello",
        workspace_root=str(root),
        workspace_template="minimal",
        mathlib_knowledge=True,
    )
    state = db.get_campaign_state(cid)
    assert cid in state.campaign.workspace_dir
    assert Path(state.campaign.workspace_dir).resolve().parent == root.resolve()
    assert state.campaign.mathlib_knowledge is True


def test_campaign_research_packet_roundtrip(tmp_path: Path) -> None:
    root = tmp_path / "wsroot"
    db = Database(str(tmp_path / "packet.db"))
    db.initialize()
    cid = db.create_campaign(
        "hello",
        workspace_root=str(root),
        workspace_template="minimal",
        research_packet_json='{"summary":"focus on route A","attack_families":[{"title":"Route A","status":"primary"}]}',
    )
    state = db.get_campaign_state(cid)
    assert "focus on route A" in state.campaign.research_packet_json
    db.update_campaign_research_packet(cid, "plain text packet")
    updated = db.get_campaign_state(cid)
    assert updated.campaign.research_packet_json == '{"summary": "plain text packet"}'
