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
        assert "mathlib_knowledge" in ccols
        cur = conn.execute("PRAGMA table_info(experiments)")
        ecols = {r[1] for r in cur.fetchall()}
        assert "move_kind" in ecols
        assert "move_note" in ecols
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='campaign_map_node_acks'"
        )
        assert cur.fetchone() is not None
        uv = conn.execute("PRAGMA user_version").fetchone()[0]
        assert int(uv) >= 6
    finally:
        conn.close()


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
