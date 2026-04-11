from __future__ import annotations

import sqlite3
from pathlib import Path

from orchestrator.limacore.db import LimaCoreDB


def test_schema_initializes_and_concise_tables_exist(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()

    conn = sqlite3.connect(str(tmp_path / "limacore.db"))
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for name in (
            "problems",
            "events",
            "frontier_nodes",
            "world_heads",
            "fracture_heads",
            "cohorts",
            "aristotle_jobs",
            "artifacts",
            "program_state",
        ):
            assert name in tables
        indices = {row[1] for row in conn.execute("PRAGMA index_list(frontier_nodes)")}
        assert "idx_limacore_frontier_problem_key" in indices
    finally:
        conn.close()


def test_artifacts_deduplicate_by_hash(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    ref1 = db.store_artifact("example", {"a": 1})
    ref2 = db.store_artifact("example", {"a": 1})
    assert ref1["hash"] == ref2["hash"]

    conn = sqlite3.connect(str(tmp_path / "limacore.db"))
    try:
        count = conn.execute("SELECT COUNT(*) FROM artifacts WHERE hash = ?", (ref1["hash"],)).fetchone()[0]
        assert int(count) == 1
    finally:
        conn.close()
