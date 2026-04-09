from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from orchestrator import config as app_config
from orchestrator.db import Database
from orchestrator.lima_agent import _build_reference_points, _local_generation
from orchestrator.lima_db import LimaDatabase, _canonical_hash
from orchestrator.lima_literature import LocalFileLiteratureBackend, make_literature_backend
from orchestrator.lima_meta import analyze_and_update_policy, compute_stagnation_controller
from orchestrator.lima_models import LimaObligationSpec, LimaUniverseSpec, safe_json_loads
from orchestrator.lima_presenter import _repair_loop_summary
import orchestrator.lima_obligations as lima_obligations_mod
from orchestrator.lima_obligations import (
    AristotleFormalBackend,
    approve_formal_review_async,
    approve_formal_review,
    archive_obligation,
    compile_obligations_for_universe,
    queue_formal_review,
    rerun_local_obligation,
    run_queued_obligation_checks,
    strict_aristotle_eligibility,
    submit_promising_formal_obligations,
    sync_lima_aristotle_results,
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
        family_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(lima_universe_family)")
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
    assert "search_action" in family_columns
    assert "search_reason_md" in family_columns
    assert "required_delta_json" in family_columns
    assert "repeat_failure_count" in family_columns
    assert "last_failure_type" in family_columns
    assert "lima_formal_review_queue" in tables
    assert "lima_event" in tables
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


def _seed_lima_formal_candidate(
    db: LimaDatabase,
    *,
    universe_status: str = "promising",
    fracture_type: str = "",
    formal_status: str = "queued_formal_review",
    formal_value: float = 4.5,
) -> dict[str, str]:
    problem = db.get_problem("collatz")
    conn = sqlite3.connect(db.path)
    try:
        conn.execute(
            """
            INSERT INTO lima_run (
                id, problem_id, trigger_kind, mode, run_summary_md,
                frontier_snapshot_json, pressure_snapshot_json, policy_snapshot_json,
                response_json, created_at
            )
            VALUES ('run-strict', ?, 'manual', 'forge', 'strict test run', '{}', '{}', '{}', '{}', 'now')
            """,
            (problem["id"],),
        )
        conn.execute(
            """
            INSERT INTO lima_universe_family (
                id, problem_id, family_key, family_kind, thesis_md, last_seen_at
            )
            VALUES ('fam-strict', ?, 'strict_family', 'test', 'test family', 'now')
            """,
            (problem["id"],),
        )
        conn.execute(
            """
            INSERT INTO lima_universe (
                id, run_id, problem_id, family_id, title, universe_status,
                branch_of_math, solved_world, why_problem_is_easy_here, core_story_md,
                compression_score, fit_score, novelty_score, falsifiability_score,
                bridgeability_score, formalizability_score, theorem_yield_score,
                literature_novelty_score, created_at, updated_at
            )
            VALUES (
                'univ-strict', 'run-strict', ?, 'fam-strict', 'Strict survivor',
                ?, 'number theory', '', '', '',
                4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 'now', 'now'
            )
            """,
            (problem["id"], universe_status),
        )
        if fracture_type:
            conn.execute(
                """
                INSERT INTO lima_fracture (
                    id, problem_id, family_id, universe_id, failure_type,
                    breakpoint_md, confidence, created_at
                )
                VALUES ('fracture-strict', ?, 'fam-strict', 'univ-strict', ?, 'prior art pressure', 0.7, 'now')
                """,
                (problem["id"], fracture_type),
            )
        conn.execute(
            """
            INSERT INTO lima_obligation (
                id, problem_id, universe_id, obligation_kind, title, statement_md,
                lean_goal, status, priority, source_run_id, source_universe_id,
                canonical_hash, estimated_formalization_value, estimated_execution_cost,
                estimated_value, estimated_cost, created_at, updated_at
            )
            VALUES (
                'obl-local', ?, 'univ-strict', 'finite_check', 'Local residue check',
                'Compute exact residue scan.', '', 'verified_local', 4,
                'run-strict', 'univ-strict', 'local-hash', 3.0, 1.0, 3.0, 1.0, 'now', 'now'
            )
            """,
            (problem["id"],),
        )
        conn.execute(
            """
            INSERT INTO lima_obligation (
                id, problem_id, universe_id, obligation_kind, title, statement_md,
                lean_goal, status, priority, why_exists_md, prove_or_kill_md,
                source_run_id, source_universe_id, canonical_hash,
                estimated_formalization_value, estimated_execution_cost,
                estimated_value, estimated_cost, created_at, updated_at
            )
            VALUES (
                'obl-formal', ?, 'univ-strict', 'lean_goal', 'Strict bridge lemma',
                'Prove the strict survivor bridge.', 'forall n : Nat, True',
                ?, 5, 'formal survivor', 'failure kills bridge',
                'run-strict', 'univ-strict', ?, ?, 4.0, ?, 4.0, 'now', 'now'
            )
            """,
            (
                problem["id"],
                formal_status,
                _canonical_hash(
                    [
                        problem["id"],
                        "lean_goal",
                        "Strict bridge lemma",
                        "Prove the strict survivor bridge.",
                        "forall n : Nat, True",
                    ]
                ),
                formal_value,
                formal_value,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return {"problem_id": str(problem["id"]), "obligation_id": "obl-formal"}


def test_lima_strict_threshold_rejects_weakened_prior_art_candidate(tmp_path: Path) -> None:
    db = LimaDatabase(str(tmp_path / "lima.db"))
    db.initialize()
    seeded = _seed_lima_formal_candidate(
        db,
        universe_status="weakened",
        fracture_type="prior_art",
    )

    obligation = db.get_obligation(seeded["obligation_id"])
    assert obligation is not None
    eligibility = strict_aristotle_eligibility(db, obligation)

    assert eligibility["eligible"] is False
    assert any("not a strict survivor" in reason for reason in eligibility["reasons"])
    assert any("prior-art fracture" in reason for reason in eligibility["reasons"])


def test_lima_fracture_pressure_sets_family_search_control(tmp_path: Path) -> None:
    db = LimaDatabase(str(tmp_path / "lima.db"))
    db.initialize()
    seeded = _seed_lima_formal_candidate(
        db,
        universe_status="weakened",
        fracture_type="prior_art",
    )
    conn = sqlite3.connect(db.path)
    try:
        for idx in range(2):
            conn.execute(
                """
                INSERT INTO lima_fracture (
                    id, problem_id, family_id, universe_id, failure_type,
                    breakpoint_md, confidence, created_at
                )
                VALUES (?, ?, 'fam-strict', 'univ-strict', 'prior_art', 'repeated prior art', 0.7, ?)
                """,
                (f"fracture-extra-{idx}", seeded["problem_id"], f"now-{idx}"),
            )
        conn.commit()
    finally:
        conn.close()

    meta = analyze_and_update_policy(db, problem_id=seeded["problem_id"])
    constraints = db.list_family_search_constraints(seeded["problem_id"])
    strict_family = [c for c in constraints if c["family_key"] == "strict_family"][0]

    assert meta["family_search_controls"]
    assert strict_family["search_action"] == "cooldown"
    assert strict_family["status"] == "cooled_down"
    assert strict_family["repeat_failure_count"] == 3
    assert "literature-distinct" in strict_family["required_delta_json"]


def test_lima_underparameterized_family_cools_down_after_repeats(tmp_path: Path) -> None:
    db = LimaDatabase(str(tmp_path / "lima.db"))
    db.initialize()
    seeded = _seed_lima_formal_candidate(
        db,
        universe_status="weakened",
        fracture_type="underparameterized_state",
    )
    conn = sqlite3.connect(db.path)
    try:
        conn.execute(
            """
            INSERT INTO lima_fracture (
                id, problem_id, family_id, universe_id, failure_type,
                breakpoint_md, confidence, created_at
            )
            VALUES (?, ?, 'fam-strict', 'univ-strict', 'underparameterized_state', 'missing companion object', 0.7, ?)
            """,
            ("fracture-extra-underparam", seeded["problem_id"], "now-underparam"),
        )
        conn.commit()
    finally:
        conn.close()

    meta = analyze_and_update_policy(db, problem_id=seeded["problem_id"])
    constraints = db.list_family_search_constraints(seeded["problem_id"])
    strict_family = [c for c in constraints if c["family_key"] == "strict_family"][0]

    assert meta["family_search_controls"]
    assert strict_family["search_action"] == "cooldown"
    assert strict_family["status"] == "cooled_down"
    assert strict_family["repeat_failure_count"] == 2
    assert "companion coordinate" in strict_family["required_delta_json"]


def test_local_generation_prefers_chip_firing_frontier_for_boundary_spill_problem() -> None:
    generated = _local_generation(
        problem={
            "title": "Synthesized Problem 2",
            "slug": "synthesized_problem_2",
            "statement_md": (
                "A move at position i is allowed if ai >= 2. One unit disappears off the boundary. "
                "A state is stable if every entry is 0 or 1, and the final stable state should be order-independent."
            ),
            "domain": "number_theory",
        },
        mode="forge",
        pressure_map={"search_constraints": []},
        literature_refresh={},
        current_universes=[
            {
                "title": "Chip-Firing with Boundary Sinks",
                "family_key": "chip_firing_boundary_sinks",
                "universe_status": "promising",
                "branch_of_math": "chip-firing and abelian sandpiles",
                "solved_world": "Boundary loss becomes sinked chip-firing.",
                "why_problem_is_easy_here": "Abelian stabilization should explain order independence.",
                "fit_score": 4.0,
                "compression_score": 4.0,
                "formalizability_score": 4.0,
                "bridgeability_score": 4.0,
            }
        ],
    )

    universe = generated.universes[0]

    assert universe.title == "Chip-Firing with Boundary Sinks"
    assert universe.family_key == "chip_firing_boundary_sinks"
    assert universe.ontology_class() == "graph_stabilization"
    assert any(
        target.title == "boundary_spill_move_equals_sinked_firing"
        for target in universe.formalization_targets
    )
    assert generated.selection_meta["problem_aware_family_selected"] is True


def test_local_generation_boundary_spill_problem_without_frontier_emits_chip_firing_family() -> None:
    generated = _local_generation(
        problem={
            "title": "Synthesized Problem 2",
            "slug": "synthesized_problem_2",
            "statement_md": (
                "A move at position i is allowed if ai >= 2. One unit disappears off the boundary. "
                "A state is stable if every entry is 0 or 1, and the final stable state should be order-independent."
            ),
            "domain": "discrete_dynamics",
        },
        mode="forge",
        pressure_map={"search_constraints": []},
        literature_refresh={},
        current_universes=[],
    )

    universe = generated.universes[0]

    assert universe.title == "Chip-Firing with Boundary Sinks"
    assert universe.family_key == "chip_firing_boundary_sinks"
    assert universe.ontology_class() == "graph_stabilization"
    assert generated.selection_meta["overrode_prior_frontier"] is False


def test_local_generation_overrides_generic_frontier_for_boundary_problem() -> None:
    generated = _local_generation(
        problem={
            "title": "Synthesized Problem 2",
            "slug": "synthesized_problem_2",
            "statement_md": (
                "Boundary spill should stabilize to the same final state regardless of legal firing order."
            ),
            "domain": "discrete_dynamics",
        },
        mode="forge",
        pressure_map={"search_constraints": []},
        literature_refresh={},
        current_universes=[
            {
                "title": "Synthesized Problem 2 bridge-obligation atlas",
                "family_key": "minimal_bridge_obligation_atlas",
                "universe_status": "promising",
                "branch_of_math": "discrete_dynamics",
                "solved_world": "Generic atlas of regimes.",
                "why_problem_is_easy_here": "Local reductions might help.",
                "fit_score": 5.0,
                "compression_score": 4.0,
                "formalizability_score": 4.0,
                "bridgeability_score": 4.0,
            }
        ],
    )

    universe = generated.universes[0]

    assert universe.title == "Chip-Firing with Boundary Sinks"
    assert universe.family_key == "chip_firing_boundary_sinks"
    assert generated.selection_meta["overrode_prior_frontier"] is True
    assert generated.selection_meta["prior_frontier_family_key"] == "minimal_bridge_obligation_atlas"


def test_stagnation_controller_detects_repeated_frontier_and_blocker() -> None:
    runs = []
    for idx in range(4):
        runs.append(
            {
                "id": f"run-{idx}",
                "response_json": {
                    "output": {
                        "universes": [
                            {
                                "title": "Chip-Firing with Boundary Sinks",
                                "family_key": "chip_firing_boundary_sinks",
                            }
                        ]
                    },
                    "rupture_reports": [
                        {
                            "universe_title": "Weighted Height Function with Boundary Deficit",
                            "attacks": [
                                {
                                    "failure_type": "underparameterized_state",
                                    "confidence": 0.66,
                                }
                            ],
                        }
                    ],
                },
            }
        )

    controller = compute_stagnation_controller(
        runs=runs,
        families=[
            {
                "family_key": "chip_firing_boundary_sinks",
                "formal_win_count": 0,
                "survival_count": 0,
                "repeat_failure_count": 0,
                "last_failure_type": "",
            },
            {
                "family_key": "height_deficit_lift",
                "formal_win_count": 0,
                "survival_count": 0,
                "repeat_failure_count": 3,
                "last_failure_type": "underparameterized_state",
            },
        ],
        fractures=[
            {"failure_type": "underparameterized_state"},
            {"failure_type": "underparameterized_state"},
            {"failure_type": "underparameterized_state"},
        ],
        obligations=[],
    )

    assert controller["active"] is True
    assert controller["mode_shift"] == "bridge_first"
    assert controller["top_family_key"] == "chip_firing_boundary_sinks"
    assert controller["dominant_blocker"] == "underparameterized_state"
    assert "height_deficit_lift" in controller["avoid_family_keys"]
    assert controller["repair_loop"]["active"] is True
    assert controller["repair_loop"]["strategy"] == "companion_state_search"
    assert controller["repair_loop"]["target_family_key"] == "chip_firing_boundary_sinks"
    assert controller["repair_loop"]["next_hypothesis_keys"][:2] == [
        "boundary_debt_ledger",
        "boundary_context_tag",
    ]


def test_stagnation_controller_marks_already_tried_repairs() -> None:
    runs = []
    for idx in range(4):
        output = {
            "universes": [
                {
                    "title": "Chip-Firing with Boundary Sinks",
                    "family_key": "chip_firing_boundary_sinks",
                }
            ]
        }
        if idx == 0:
            output["universes"].insert(
                0,
                {
                    "title": "Boundary Debt Ledger Repair",
                    "family_key": "chip_firing_boundary_sinks_boundary_debt_ledger",
                    "repair_hypothesis_key": "boundary_debt_ledger",
                },
            )
        runs.append(
            {
                "id": f"run-repair-{idx}",
                "response_json": {
                    "output": output,
                    "rupture_reports": [
                        {
                            "universe_title": "Chip-Firing with Boundary Sinks",
                            "attacks": [
                                {
                                    "failure_type": "underparameterized_state",
                                    "confidence": 0.7,
                                }
                            ],
                        }
                    ],
                },
            }
        )

    controller = compute_stagnation_controller(
        runs=runs,
        families=[
            {
                "family_key": "chip_firing_boundary_sinks",
                "formal_win_count": 0,
                "survival_count": 0,
                "repeat_failure_count": 0,
                "last_failure_type": "",
            }
        ],
        fractures=[
            {"failure_type": "underparameterized_state"},
            {"failure_type": "underparameterized_state"},
            {"failure_type": "underparameterized_state"},
        ],
        obligations=[],
    )

    tried = {
        hypothesis["key"]: hypothesis["status"]
        for hypothesis in controller["repair_loop"]["hypotheses"]
    }
    assert controller["repair_loop"]["attempts_used"] == 1
    assert tried["boundary_debt_ledger"] == "tried"
    assert "boundary_debt_ledger" not in controller["repair_loop"]["next_hypothesis_keys"]
    assert controller["repair_loop"]["next_hypothesis_keys"][0] == "boundary_context_tag"


def test_local_generation_uses_companion_mutation_when_stagnation_demands_it() -> None:
    generated = _local_generation(
        problem={
            "title": "Synthetic plateau problem",
            "slug": "synthetic_plateau_problem",
            "statement_md": "A scalar energy seems useful but keeps failing to explain the system.",
            "domain": "discrete_dynamics",
        },
        mode="forge",
        pressure_map={
            "search_constraints": [],
            "stagnation_controller": {
                "active": True,
                "dominant_blocker": "underparameterized_state",
            },
        },
        literature_refresh={},
        current_universes=[
            {
                "title": "Weighted Height Guess",
                "family_key": "height_deficit_lift",
                "universe_status": "promising",
                "branch_of_math": "discrete dynamics",
                "solved_world": "Scalar energy should work.",
                "why_problem_is_easy_here": "Energy seems to decrease.",
                "fit_score": 4.0,
                "compression_score": 4.0,
                "formalizability_score": 3.0,
                "bridgeability_score": 3.0,
            }
        ],
    )

    universe = generated.universes[0]

    assert universe.family_key == "defect_augmented_bridge"
    assert any(
        target.title == "defect_augmented_transition_law"
        for target in universe.formalization_targets
    )


def test_local_generation_emits_chip_firing_repair_universes_when_repair_loop_is_active() -> None:
    generated = _local_generation(
        problem={
            "title": "Synthesized Problem 2",
            "slug": "synthesized_problem_2",
            "statement_md": (
                "Boundary spill should stabilize to the same final state regardless of legal firing order."
            ),
            "domain": "discrete_dynamics",
        },
        mode="forge",
        pressure_map={
            "search_constraints": [],
            "stagnation_controller": {
                "active": True,
                "dominant_blocker": "underparameterized_state",
                "repair_loop": {
                    "active": True,
                    "strategy": "companion_state_search",
                    "target_family_key": "chip_firing_boundary_sinks",
                    "failure_type": "underparameterized_state",
                    "attempt_budget": 4,
                    "attempts_used": 1,
                    "attempts_remaining": 3,
                    "next_hypothesis_keys": ["boundary_context_tag", "sink_parity_cocycle"],
                    "hypotheses": [
                        {
                            "key": "boundary_debt_ledger",
                            "title": "Boundary debt ledger",
                            "status": "tried",
                            "description": "Track sink loss as a companion coordinate.",
                            "check_focus": "Look for exact local bridge updates.",
                        },
                        {
                            "key": "boundary_context_tag",
                            "title": "Boundary context tag",
                            "status": "queued",
                            "description": "Remember which local wall context is active.",
                            "check_focus": "Run small commutation checks with a tagged boundary state.",
                        },
                        {
                            "key": "sink_parity_cocycle",
                            "title": "Sink parity cocycle",
                            "status": "queued",
                            "description": "Use a parity cocycle to repair the chip-firing bridge.",
                            "check_focus": "Test if the cocycle restores exact endpoint projection.",
                        },
                    ],
                },
            },
        },
        literature_refresh={},
        current_universes=[
            {
                "title": "Chip-Firing with Boundary Sinks",
                "family_key": "chip_firing_boundary_sinks",
                "universe_status": "promising",
                "branch_of_math": "chip-firing and abelian sandpiles",
                "solved_world": "Boundary loss becomes sinked chip-firing.",
                "why_problem_is_easy_here": "Abelian stabilization should explain order independence.",
                "core_story_md": "Boundary dissipation becomes sink completion.",
                "fit_score": 4.0,
                "compression_score": 4.0,
                "formalizability_score": 4.0,
                "bridgeability_score": 4.0,
            }
        ],
    )

    family_keys = [universe.family_key for universe in generated.universes]
    repair_keys = [getattr(universe, "repair_hypothesis_key", "") for universe in generated.universes]

    assert family_keys == [
        "chip_firing_boundary_sinks_boundary_context_tag",
        "chip_firing_boundary_sinks_sink_parity_cocycle",
    ]
    assert repair_keys == ["boundary_context_tag", "sink_parity_cocycle"]
    assert all(universe.repair_strategy == "companion_state_search" for universe in generated.universes)
    assert all(
        any("commutation" in target.title for target in universe.formalization_targets)
        for universe in generated.universes
    )


def test_repair_loop_summary_surfaces_recent_attempt_artifacts() -> None:
    state = {
        "pressure_map_json": {
            "stagnation_controller": {
                "repair_loop": {
                    "active": True,
                    "strategy": "companion_state_search",
                    "target_family_key": "chip_firing_boundary_sinks",
                    "failure_type": "underparameterized_state",
                    "attempt_budget": 4,
                    "attempts_used": 2,
                    "attempts_remaining": 2,
                    "summary_md": "Lima is trying explicit repaired-state hypotheses.",
                    "next_hypothesis_keys": ["sink_parity_cocycle"],
                    "hypotheses": [
                        {
                            "key": "boundary_debt_ledger",
                            "title": "Boundary debt ledger",
                            "status": "tried",
                            "description": "Track sink loss directly.",
                            "check_focus": "Verify commutation on small examples.",
                        },
                        {
                            "key": "sink_parity_cocycle",
                            "title": "Sink parity cocycle",
                            "status": "queued",
                            "description": "Repair missing parity information.",
                            "check_focus": "Check endpoint projection after repairs.",
                        },
                    ],
                }
            }
        }
    }
    artifacts = [
        {
            "artifact_kind": "repair_attempt",
            "content_json": {
                "repair_hypothesis_key": "boundary_debt_ledger",
                "repair_focus": "Audit the boundary debt ledger on small commutation cases.",
            },
        }
    ]

    summary = _repair_loop_summary(state, artifacts)

    assert summary["active"] is True
    assert summary["headline"] == "Repair loop 2/4"
    assert summary["next_hypothesis_keys"] == ["sink_parity_cocycle"]
    assert summary["recent_attempts"][0]["key"] == "boundary_debt_ledger"
    assert "Audit the boundary debt ledger" in summary["recent_attempts"][0]["focus"]


def test_compile_obligations_adds_problem_native_chip_firing_suite() -> None:
    universe = _local_generation(
        problem={
            "title": "Synthesized Problem 2",
            "slug": "synthesized_problem_2",
            "statement_md": (
                "A move at position i is allowed if ai >= 2. One unit disappears off the boundary. "
                "The final stable state should be order-independent."
            ),
            "domain": "discrete_dynamics",
        },
        mode="forge",
        pressure_map={"search_constraints": []},
        literature_refresh={},
        current_universes=[],
    ).universes[0]

    obligations = compile_obligations_for_universe(universe)
    titles = {obligation.title: obligation for obligation in obligations}

    assert "boundary_spill_move_equals_sinked_firing" in titles
    assert titles["boundary_spill_move_equals_sinked_firing"].status == "queued_local"
    assert "firing_commutation_local" in titles
    assert "quadratic_potential_descent" in titles
    assert "stabilization_terminates" in titles
    assert "local_confluence_or_abelianity" in titles
    assert "sink_stabilization_implies_unique_endpoint" in titles


def test_problem_native_local_checker_verifies_boundary_chip_firing_obligation(
    tmp_path: Path,
) -> None:
    db = LimaDatabase(str(tmp_path / "lima.db"))
    db.initialize()
    problem_id, _ = db.create_problem(
        slug="synthesized_problem_2",
        title="Synthesized Problem 2",
        statement_md=(
            "A move at position i is allowed if ai >= 2. One unit disappears off the boundary. "
            "The final stable state should be order-independent."
        ),
        domain="discrete_dynamics",
        default_goal_text="Check sinked chip-firing.",
    )
    conn = sqlite3.connect(str(tmp_path / "lima.db"))
    try:
        conn.execute(
            """
            INSERT INTO lima_obligation (
                id, problem_id, universe_id, obligation_kind, title, statement_md,
                status, priority, lineage_json, created_at, updated_at
            )
            VALUES (
                'obl-chip', ?, 'univ-chip', 'finite_check', 'firing_commutation_local',
                'Verify on bounded path-graph states that adjacent legal firings commute.',
                'queued_local', 5,
                '{"source_family_key":"chip_firing_boundary_sinks","source_universe_title":"Chip-Firing with Boundary Sinks"}',
                'now', 'now'
            )
            """,
            (problem_id,),
        )
        conn.commit()
    finally:
        conn.close()

    result = run_queued_obligation_checks(db, problem_id=problem_id)
    obligation = db.get_obligation("obl-chip")
    artifacts = db.list_artifacts(problem_id, limit=20)
    latest_artifact = artifacts[0]

    assert result["checked"] == ["obl-chip"]
    assert obligation["status"] == "verified_local"
    assert "commutation violations" in obligation["result_summary_md"]
    assert safe_json_loads(latest_artifact["content_json"], {})["artifact"]["checker_path"] == "boundary_chip_firing"


def test_lima_suppresses_repeated_weakened_handoff_without_material_delta(tmp_path: Path) -> None:
    db = LimaDatabase(str(tmp_path / "lima.db"))
    db.initialize()
    seeded = _seed_lima_formal_candidate(
        db,
        universe_status="weakened",
        fracture_type="prior_art",
    )
    db.update_family_search_control(
        problem_id=seeded["problem_id"],
        family_key="strict_family",
        search_action="cooldown",
        reason_md="Repeated prior-art pressure requires a material delta.",
        required_delta=["introduce a literature-distinct mathematical object"],
        repeat_failure_count=3,
        last_failure_type="prior_art",
    )
    before = len(db.list_handoffs(seeded["problem_id"], limit=100))
    universe = LimaUniverseSpec(
        title="Strict survivor repeat",
        family_key="strict_family",
        family_kind="adjacent",
        branch_of_math="number theory",
        core_story_md="Repeat the same formal target.",
        formalization_targets=[
            LimaObligationSpec(
                obligation_kind="lean_goal",
                title="Strict bridge lemma",
                statement_md="Prove the strict survivor bridge.",
                lean_goal="forall n : Nat, True",
                status="queued_formal_review",
                priority=5,
                estimated_formalization_value=4.5,
                estimated_execution_cost=4.0,
            )
        ],
    )
    run_id = db.commit_run(
        problem_id=seeded["problem_id"],
        trigger_kind="manual",
        mode="forge",
        run_summary_md="repeat",
        frontier_snapshot={},
        pressure_snapshot={},
        policy_snapshot={},
        response_obj={},
        universes=[universe],
        rupture_reports=[
            {
                "universe_title": "Strict survivor repeat",
                "verdict": "weakened",
                "summary_md": "repeated prior-art pressure",
                "fractures": [
                    {
                        "failure_type": "prior_art",
                        "breakpoint_md": "same prior art",
                        "confidence": 0.7,
                    }
                ],
            }
        ],
    )
    after = len(db.list_handoffs(seeded["problem_id"], limit=100))
    artifacts = db.list_artifacts(seeded["problem_id"], limit=20)

    assert run_id
    assert after == before
    assert any(a["artifact_kind"] == "search_control_suppression" for a in artifacts)


def test_lima_aristotle_auto_submit_accepts_strict_survivor(
    tmp_path: Path, monkeypatch
) -> None:
    lima = LimaDatabase(str(tmp_path / "lima.db"))
    lima.initialize()
    seeded = _seed_lima_formal_candidate(lima)
    main_db = Database(str(tmp_path / "main.db"))
    main_db.initialize()
    monkeypatch.setattr(app_config, "LIMA_FORMAL_AUTO_SUBMIT", True)
    monkeypatch.setattr(app_config, "LIMA_ARISTOTLE_AUTO_SUBMIT", True)
    monkeypatch.setattr(app_config, "LIMA_ARISTOTLE_MAX_ACTIVE", 2)
    monkeypatch.setattr(app_config, "LIMA_ARISTOTLE_MAX_DAILY_SUBMISSIONS", 10)
    monkeypatch.setattr(app_config, "WORKSPACE_ROOT", str(tmp_path / "workspaces"))

    async def fake_submit(objective: str, project_dir: str) -> tuple[str, str]:
        assert "Strict bridge lemma" in objective
        assert Path(project_dir).exists()
        return "11111111-1111-1111-1111-111111111111", ""

    monkeypatch.setattr(lima_obligations_mod, "submit", fake_submit)

    result = asyncio.run(
        submit_promising_formal_obligations(
            lima,
            main_db,
            problem_id=seeded["problem_id"],
        )
    )
    obligation = lima.get_obligation(seeded["obligation_id"])
    ref = safe_json_loads(obligation["formal_submission_ref_json"], {})

    assert result["submitted"] == [seeded["obligation_id"]]
    assert obligation["status"] == "submitted_formal"
    assert ref["backend"] == "aristotle_formal"
    assert ref["campaign_id"]
    assert ref["target_id"]
    assert ref["aristotle_experiment_id"]
    assert ref["aristotle_job_id"] == "11111111-1111-1111-1111-111111111111"
    assert main_db.count_campaign_experiments_by_statuses(ref["campaign_id"], ["submitted"]) == 1


def test_lima_aristotle_caps_and_duplicate_block_submission(
    tmp_path: Path, monkeypatch
) -> None:
    lima = LimaDatabase(str(tmp_path / "lima.db"))
    lima.initialize()
    seeded = _seed_lima_formal_candidate(lima)
    main_db = Database(str(tmp_path / "main.db"))
    main_db.initialize()
    monkeypatch.setattr(app_config, "LIMA_ARISTOTLE_MAX_ACTIVE", 0)
    monkeypatch.setattr(app_config, "WORKSPACE_ROOT", str(tmp_path / "workspaces"))
    backend = AristotleFormalBackend(lima_db=lima, main_db=main_db)

    result = asyncio.run(
        approve_formal_review_async(
            lima,
            obligation_id=seeded["obligation_id"],
            backend=backend,
            main_db=main_db,
        )
    )
    assert result["ok"] is False
    assert result["error"] == "budget_exhausted"
    assert lima.get_obligation(seeded["obligation_id"])["status"] == "queued_formal_review"

    monkeypatch.setattr(app_config, "LIMA_ARISTOTLE_MAX_ACTIVE", 2)
    conn = sqlite3.connect(lima.path)
    try:
        conn.execute(
            """
            UPDATE lima_obligation
            SET formal_submission_ref_json = '{"aristotle_experiment_id":"existing"}'
            WHERE id = 'obl-formal'
            """
        )
        conn.commit()
    finally:
        conn.close()
    duplicate = strict_aristotle_eligibility(lima, lima.get_obligation("obl-formal"))
    assert duplicate["eligible"] is False
    assert "obligation already has a Lima Aristotle submission reference" in duplicate["reasons"]


def test_lima_aristotle_result_sync_maps_verdicts(tmp_path: Path) -> None:
    lima = LimaDatabase(str(tmp_path / "lima.db"))
    lima.initialize()
    seeded = _seed_lima_formal_candidate(lima, formal_status="submitted_formal")
    main_db = Database(str(tmp_path / "main.db"))
    main_db.initialize()
    campaign_id = main_db.create_campaign("sync campaign", workspace_root=str(tmp_path / "ws"))
    target_id = main_db.add_targets(campaign_id, ["sync target"])[0]
    experiment_id = main_db.create_experiment(campaign_id, target_id, "sync objective")
    main_db.update_experiment_submitted(experiment_id, "22222222-2222-2222-2222-222222222222")
    main_db.update_experiment_completed(
        experiment_id,
        result_raw="raw",
        result_summary="proved bridge lemma",
        verdict="proved",
        parsed_proved_lemmas=["strict_bridge"],
        parsed_generated_lemmas=[],
        parsed_unsolved_goals=[],
        parsed_blockers=[],
        parsed_counterexamples=[],
        parsed_error_message="",
        parse_warnings=[],
    )
    conn = sqlite3.connect(lima.path)
    try:
        conn.execute(
            """
            UPDATE lima_obligation
            SET formal_submission_ref_json = ?
            WHERE id = 'obl-formal'
            """,
            (
                '{"backend":"aristotle_formal","aristotle_experiment_id":"'
                + experiment_id
                + '","aristotle_job_id":"22222222-2222-2222-2222-222222222222"}',
            ),
        )
        conn.commit()
    finally:
        conn.close()

    result = sync_lima_aristotle_results(lima, main_db, problem_id=seeded["problem_id"])
    obligation = lima.get_obligation(seeded["obligation_id"])
    ref = safe_json_loads(obligation["formal_submission_ref_json"], {})

    assert result["synced"] == [seeded["obligation_id"]]
    assert obligation["status"] == "verified_formal"
    assert ref["verdict"] == "proved"
    assert ref["parsed_proved_lemmas"] == ["strict_bridge"]
