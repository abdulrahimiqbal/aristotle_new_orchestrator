from __future__ import annotations

import sqlite3
from pathlib import Path

from orchestrator.lima_db import LimaDatabase
from orchestrator.lima_literature import LocalFileLiteratureBackend
from orchestrator.lima_models import LimaObligationSpec
from orchestrator.lima_obligations import (
    approve_formal_review,
    queue_formal_review,
    run_queued_obligation_checks,
)


def test_lima_migrates_legacy_obligation_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy-lima.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(
            """
            CREATE TABLE lima_problem (
                id TEXT PRIMARY KEY,
                slug TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                statement_md TEXT NOT NULL DEFAULT '',
                domain TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                default_goal_text TEXT NOT NULL DEFAULT '',
                seed_packet_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE lima_state (
                problem_id TEXT PRIMARY KEY,
                revision INTEGER NOT NULL DEFAULT 0,
                frontier_summary_md TEXT NOT NULL DEFAULT '',
                frontier_json TEXT NOT NULL DEFAULT '{}',
                pressure_map_json TEXT NOT NULL DEFAULT '{}',
                worldview_json TEXT NOT NULL DEFAULT '{}',
                policy_json TEXT NOT NULL DEFAULT '{}',
                generation_priors_json TEXT NOT NULL DEFAULT '{}',
                rupture_policy_json TEXT NOT NULL DEFAULT '{}',
                literature_policy_json TEXT NOT NULL DEFAULT '{}',
                formal_policy_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE lima_obligation (
                id TEXT PRIMARY KEY,
                problem_id TEXT NOT NULL,
                universe_id TEXT NOT NULL,
                claim_id TEXT,
                obligation_kind TEXT NOT NULL DEFAULT 'bridge_lemma',
                title TEXT NOT NULL DEFAULT '',
                statement_md TEXT NOT NULL DEFAULT '',
                lean_goal TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'queued',
                priority INTEGER NOT NULL DEFAULT 3,
                aristotle_ref_json TEXT NOT NULL DEFAULT '{}',
                result_summary_md TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.commit()
    finally:
        conn.close()

    LimaDatabase(str(db_path)).initialize()
    conn = sqlite3.connect(str(db_path))
    try:
        obligation_columns = {row[1] for row in conn.execute("PRAGMA table_info(lima_obligation)")}
        problem_columns = {row[1] for row in conn.execute("PRAGMA table_info(lima_problem)")}
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    finally:
        conn.close()

    assert "routing_policy_json" in problem_columns
    assert "why_exists_md" in obligation_columns
    assert "canonical_hash" in obligation_columns
    assert "formal_payload_json" in obligation_columns
    assert "lima_formal_review_queue" in tables


def test_lima_formal_review_queue_is_zero_live_authority(tmp_path: Path) -> None:
    db = LimaDatabase(str(tmp_path / "lima.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    db.update_obligation_result  # keep mypy/linters aware the DB helper exists
    conn = sqlite3.connect(str(tmp_path / "lima.db"))
    try:
        conn.execute(
            """
            INSERT INTO lima_obligation (
                id, problem_id, universe_id, obligation_kind, title, statement_md,
                lean_goal, status, priority, created_at, updated_at
            )
            VALUES ('obl1', ?, 'univ1', 'lean_goal', 'Odd transfer', 'Define odd transfer.', 'forall n : Nat, True', 'queued_formal_review', 4, 'now', 'now')
            """,
            (problem["id"],),
        )
        conn.commit()
    finally:
        conn.close()

    queued = queue_formal_review(db, obligation_id="obl1")
    approved = approve_formal_review(db, obligation_id="obl1")
    obligation = db.get_obligation("obl1")
    reviews = db.list_formal_reviews(problem["id"])

    assert queued["ok"] is True
    assert approved["ok"] is True
    assert obligation["status"] == "approved_for_formal"
    assert reviews
    assert approved["backend_result"]["live_aristotle_job_created"] is False


def test_lima_local_file_literature_backend(tmp_path: Path) -> None:
    notes = tmp_path / "literature"
    notes.mkdir()
    (notes / "goldbach.md").write_text(
        "# Goldbach local note\nLemma: parity and circle-method reductions are prior-art pressure.",
        encoding="utf-8",
    )

    records = LocalFileLiteratureBackend(str(notes)).search(
        problem={"slug": "goldbach", "title": "Goldbach conjecture"},
        queries=["Goldbach parity"],
        limit=5,
    )

    assert len(records) == 1
    assert records[0].source_type == "local_file"
    assert records[0].extracts[0]["extract_kind"] == "lemma"


def test_lima_legacy_queued_status_runs_local_check(tmp_path: Path) -> None:
    db = LimaDatabase(str(tmp_path / "lima.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    conn = sqlite3.connect(str(tmp_path / "lima.db"))
    try:
        conn.execute(
            """
            INSERT INTO lima_obligation (
                id, problem_id, universe_id, obligation_kind, title, statement_md,
                lean_goal, status, priority, created_at, updated_at
            )
            VALUES ('obl2', ?, 'univ2', 'finite_check', 'Residue descent scan modulo 16', 'Compute exact scan.', '', 'queued', 4, 'now', 'now')
            """,
            (problem["id"],),
        )
        conn.commit()
    finally:
        conn.close()

    result = run_queued_obligation_checks(db, problem_id=problem["id"])
    obligation = db.get_obligation("obl2")

    assert result["checked"] == ["obl2"]
    assert obligation["status"] == "verified_local"


def test_lima_obligation_status_normalization() -> None:
    assert LimaObligationSpec(status="queued").status == "queued_local"
    assert LimaObligationSpec(status="checked").status == "verified_local"
    assert LimaObligationSpec(status="falsified").status == "refuted_local"
