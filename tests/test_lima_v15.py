from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

from orchestrator import config as app_config
from orchestrator.lima_agent import _build_reference_points
from orchestrator.lima_db import LimaDatabase
from orchestrator.lima_literature import LocalFileLiteratureBackend, make_literature_backend
from orchestrator.lima_models import LimaObligationSpec
from orchestrator.lima_obligations import (
    approve_formal_review,
    archive_obligation,
    queue_formal_review,
    rerun_local_obligation,
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
            INSERT INTO lima_obligation (
                id, problem_id, universe_id, obligation_kind, title, statement_md,
                status, priority, created_at, updated_at
            )
            VALUES
                ('legacy-queued', 'legacy-problem', 'legacy-universe', 'finite_check', 'Queued', '', 'queued', 3, 'now', 'now'),
                ('legacy-checked', 'legacy-problem', 'legacy-universe', 'finite_check', 'Checked', '', 'checked', 3, 'now', 'now'),
                ('legacy-falsified', 'legacy-problem', 'legacy-universe', 'finite_check', 'Falsified', '', 'falsified', 3, 'now', 'now');
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
        formal_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(lima_formal_review_queue)")
        }
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        migrated_statuses = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT id, status FROM lima_obligation WHERE id LIKE 'legacy-%'"
            )
        }
    finally:
        conn.close()

    assert "routing_policy_json" in problem_columns
    assert "why_exists_md" in obligation_columns
    assert "canonical_hash" in obligation_columns
    assert "formal_payload_json" in obligation_columns
    assert "formal_submission_ref_json" in obligation_columns
    assert "review_note" in obligation_columns
    assert "reviewed_at" in obligation_columns
    assert "source_run_id" in obligation_columns
    assert "source_claim_ids_json" in obligation_columns
    assert "estimated_formalization_value" in obligation_columns
    assert "estimated_execution_cost" in obligation_columns
    assert "estimated_value" in obligation_columns
    assert "estimated_cost" in obligation_columns
    assert "lima_formal_review_queue" in tables
    assert "family_id" in formal_columns
    assert "claim_ids_json" in formal_columns
    assert "lineage_json" in formal_columns
    assert migrated_statuses == {
        "legacy-queued": "queued_local",
        "legacy-checked": "verified_local",
        "legacy-falsified": "refuted_local",
    }


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
                    lean_goal, status, priority, why_exists_md, prove_or_kill_md, lineage_json,
                    estimated_formalization_value, estimated_execution_cost, created_at, updated_at
                )
            VALUES (
                'obl1', ?, 'univ1', 'lean_goal', 'Odd transfer', 'Define odd transfer.',
                'forall n : Nat, True', 'queued_formal_review', 4,
                'Bridge target from odd-transfer claim.', 'Failure blocks the quotient bridge.',
                '{"source_run_id":"run1","source_claim_id":"claim1","claim_ids":["claim1"],"rupture_summary":"Prior-art fracture pressure."}',
                4.5, 4.0, 'now', 'now'
            )
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
    assert "no remote Lean/Aristotle work" in obligation["formal_submission_ref_json"]
    assert obligation["reviewed_at"]
    assert reviews
    assert reviews[0]["claim_ids_json"] == '["claim1"]'
    assert "Prior-art fracture pressure" in reviews[0]["rupture_summary_md"]
    assert "run1" in reviews[0]["lineage_json"]
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
    assert records[0].source_type == "localfile"
    assert records[0].extracts[0]["extract_kind"] == "lemma"


def test_lima_literature_backend_all_degrades_to_localfile(tmp_path: Path, monkeypatch) -> None:
    notes = tmp_path / "literature"
    notes.mkdir()
    (notes / "goldbach.json").write_text(
        '{"title":"Goldbach local theorem","abstract_md":"Theorem: local note."}',
        encoding="utf-8",
    )
    monkeypatch.setattr(app_config, "LIMA_LITERATURE_LOCALFILE_DIR", str(notes))
    monkeypatch.setattr(app_config, "LIMA_ENABLE_ARXIV_BACKEND", False)
    monkeypatch.setattr(app_config, "LIMA_ENABLE_SEMANTIC_SCHOLAR_BACKEND", False)
    monkeypatch.setattr(app_config, "LIMA_ENABLE_CROSSREF_BACKEND", False)

    backend = make_literature_backend("all")
    records = backend.search(
        problem={"slug": "goldbach", "title": "Goldbach conjecture"},
        queries=["Goldbach theorem"],
        limit=5,
    )

    assert any(record.source_type == "localfile" for record in records)

    direct_backend = make_literature_backend("localfile")
    direct_records = direct_backend.search(
        problem={"slug": "goldbach", "title": "Goldbach conjecture"},
        queries=["Goldbach theorem"],
        limit=5,
    )
    assert any(record.source_type == "localfile" for record in direct_records)


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
            VALUES ('obl2', ?, 'univ2', 'finite_check', 'Residue descent scan modulo 16', 'Compute exact scan.', '', 'queued_local', 4, 'now', 'now')
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


def test_lima_rerun_and_archive_obligation_are_zero_live_authority(tmp_path: Path) -> None:
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
            VALUES ('obl3', ?, 'univ3', 'finite_check', 'Residue descent scan modulo 16', 'Compute exact scan.', '', 'verified_local', 4, 'now', 'now')
            """,
            (problem["id"],),
        )
        conn.commit()
    finally:
        conn.close()

    rerun = rerun_local_obligation(db, obligation_id="obl3")
    archived = archive_obligation(db, obligation_id="obl3")
    obligation = db.get_obligation("obl3")

    assert rerun["ok"] is True
    assert rerun["status"] == "verified_local"
    assert archived == {"ok": True, "obligation_id": "obl3", "status": "archived"}
    assert obligation["status"] == "archived"


def test_lima_obligation_status_normalization() -> None:
    assert LimaObligationSpec(status="queued").status == "queued_local"
    assert LimaObligationSpec(status="checked").status == "verified_local"
    assert LimaObligationSpec(status="falsified").status == "refuted_local"
    assert LimaObligationSpec(status="archived").status == "archived"


def test_lima_reference_ingestion_uses_problem_routing_not_collatz_defaults() -> None:
    class FakeMainDb:
        def __init__(self) -> None:
            self.shadow_goal = ""
            self.supershadow_goal = ""

        def get_all_campaigns(self):
            return [
                SimpleNamespace(id="c1", prompt="Goldbach conjecture parity campaign", status=None),
                SimpleNamespace(id="c2", prompt="Collatz 3x+1 campaign", status=None),
            ]

        def list_shadow_global_hypotheses(self, goal_id, limit=12):
            self.shadow_goal = goal_id
            return [{"id": "s1", "claim": "goldbach shadow"}]

        def list_supershadow_concepts(self, goal_id, limit=12):
            self.supershadow_goal = goal_id
            return [{"id": "ss1", "claim": "goldbach supershadow"}]

    fake = FakeMainDb()
    refs = _build_reference_points(
        fake,
        {
            "slug": "goldbach",
            "title": "Goldbach conjecture",
            "seed_packet_json": "{}",
            "routing_policy_json": (
                '{"retrieval_keywords":["Goldbach","parity"],'
                '"shadow_goal_id":"global_goldbach",'
                '"supershadow_goal_id":"global_goldbach_supershadow"}'
            ),
        },
    )

    campaign_ids = {ref["external_id"] for ref in refs if ref["reference_kind"] == "campaign"}
    assert campaign_ids == {"c1"}
    assert fake.shadow_goal == "global_goldbach"
    assert fake.supershadow_goal == "global_goldbach_supershadow"
