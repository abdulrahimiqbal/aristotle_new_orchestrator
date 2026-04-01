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
    finally:
        conn.close()


def test_create_campaign_per_workspace_dir(tmp_path: Path) -> None:
    root = tmp_path / "wsroot"
    db = Database(str(tmp_path / "c.db"))
    db.initialize()
    cid = db.create_campaign("hello", workspace_root=str(root), workspace_template="minimal")
    state = db.get_campaign_state(cid)
    assert cid in state.campaign.workspace_dir
    assert Path(state.campaign.workspace_dir).resolve().parent == root.resolve()
