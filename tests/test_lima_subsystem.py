from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from orchestrator import app as app_mod
from orchestrator import config as app_config
from orchestrator.db import Database
from orchestrator.lima_agent import run_lima
from orchestrator.lima_db import LimaDatabase
from orchestrator.lima_literature import (
    ArxivLiteratureBackend,
    CrossrefLiteratureBackend,
    SemanticScholarLiteratureBackend,
    refresh_literature,
)
from orchestrator.lima_meta import analyze_and_update_policy
from orchestrator.lima_models import (
    LimaClaimSpec,
    LimaObjectSpec,
    LimaObligationSpec,
    LimaUniverseSpec,
    coerce_lima_generation_response,
    infer_ontology_class_from_universe,
)
from orchestrator.lima_obligations import compile_obligations_for_universe
from orchestrator.lima_rupture import rupture_universe


async def _noop_loop(*_args, **_kwargs) -> None:
    return None


def test_lima_db_initialization_and_schema(tmp_path: Path) -> None:
    db = LimaDatabase(str(tmp_path / "lima.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    assert problem["slug"] == "collatz"

    conn = sqlite3.connect(str(tmp_path / "lima.db"))
    try:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        for table in (
            "lima_problem",
            "lima_state",
            "lima_run",
            "lima_universe_family",
            "lima_universe",
            "lima_claim",
            "lima_obligation",
            "lima_rupture_run",
            "lima_fracture",
            "lima_reference",
            "lima_literature_source",
            "lima_literature_extract",
            "lima_universe_literature_link",
            "lima_meta_run",
            "lima_policy_revision",
            "lima_policy_layer",
            "lima_transfer_metric",
            "lima_handoff_request",
            "lima_artifact",
        ):
            assert table in tables
    finally:
        conn.close()


def test_lima_universe_schema_coercion() -> None:
    parsed, warnings = coerce_lima_generation_response(
        {
            "frontier_summary_md": "frontier",
            "pressure_map": {"tensions": ["gap"]},
            "run_summary_md": "summary",
            "concepts": [
                {
                    "title": "Odd quotient",
                    "branch_of_math": "symbolic dynamics",
                    "solved_world": "quotient world",
                    "core_objects": [{"object_kind": "operator", "name": "T"}],
                    "laws": [{"claim_kind": "law", "title": "law"}],
                    "kill_tests": [{"claim_kind": "kill_test", "title": "break"}],
                    "formalization_targets": [
                        {"obligation_kind": "lean_goal", "title": "goal"}
                    ],
                }
            ],
        }
    )
    assert "concepts_alias_used" in warnings
    assert parsed.universes[0].title == "Odd quotient"
    assert parsed.universes[0].core_objects[0].object_kind == "operator"


def test_lima_rupture_generates_verdict_and_fracture() -> None:
    universe = LimaUniverseSpec(
        title="Vacuous descent universe",
        branch_of_math="number theory",
        solved_world="Assume Collatz is true and every trajectory descends.",
        why_problem_is_easy_here="Because Collatz is true by the conjecture.",
        core_story_md="This should be rejected.",
        core_objects=[LimaObjectSpec(object_kind="invariant", name="height")],
        laws=[
            LimaClaimSpec(
                claim_kind="law",
                title="strict descent",
                statement_md="Every step is strict descent.",
            )
        ],
        kill_tests=[
            LimaClaimSpec(
                claim_kind="kill_test",
                title="find odd increase",
                statement_md="strict descent every step",
            )
        ],
        formalization_targets=[
            LimaObligationSpec(
                obligation_kind="finite_check",
                title="bounded counterexample",
                statement_md="Find n with next n greater than n.",
            )
        ],
    )
    report = rupture_universe(universe, literature_context=[])
    assert report["verdict"] == "collapsed"
    failure_types = {f["failure_type"] for f in report["fractures"]}
    assert "vacuity" in failure_types
    assert "bounded_counterexample" in failure_types


def test_lima_policy_layers_and_family_governance_are_scoped(tmp_path: Path) -> None:
    db = LimaDatabase(str(tmp_path / "lima.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    global_layer = db.set_policy_layer(
        scope="global",
        policy={"generation": {"habit": "prefer explicit bridges"}},
        imposed_by="test",
        reason_md="global research habit",
    )
    benchmark_layer = db.set_policy_layer(
        scope="benchmark",
        problem_id=problem["id"],
        policy={"generation": {"hard_bans": ["stale_family"]}},
        imposed_by="test",
        reason_md="temporary benchmark lock",
        meta_mutable=False,
    )
    assert global_layer != benchmark_layer
    layers = db.list_policy_layers(problem["id"])
    assert [layer["scope"] for layer in layers] == ["global", "benchmark"]

    universe = LimaUniverseSpec(
        title="Temporary stale family",
        family_key="stale_family",
        branch_of_math="symbolic dynamics",
        core_story_md="A state space with no new bridge.",
        core_objects=[LimaObjectSpec(object_kind="state_space", name="S")],
    )
    run_id = db.commit_run(
        problem_id=problem["id"],
        trigger_kind="test",
        mode="balanced",
        run_summary_md="seed stale family",
        frontier_snapshot={},
        pressure_snapshot={},
        policy_snapshot={},
        response_obj={},
        universes=[universe],
        rupture_reports=[{"universe_title": universe.title, "verdict": "weakened", "fractures": []}],
    )
    assert run_id
    ok, _ = db.update_family_search_control(
        problem_id=problem["id"],
        family_key="stale_family",
        search_action="hard_ban",
        reason_md="benchmark-scoped hard ban",
        required_delta=[],
        repeat_failure_count=5,
        last_failure_type="weak_explanation",
        scope="benchmark",
        imposed_by="test",
        meta_mutable=False,
    )
    assert ok
    analyze_and_update_policy(db, problem_id=problem["id"])
    controls = db.list_family_search_constraints(problem["id"])
    stale = next(row for row in controls if row["family_key"] == "stale_family")
    assert stale["governance_state"] == "hard_ban"
    assert stale["governance_scope"] == "benchmark"
    assert stale["governance_meta_mutable"] == 0


def test_lima_ontology_class_and_canonical_obligations() -> None:
    universe = LimaUniverseSpec(
        title="Coordinate lift proof ontology",
        family_key="coordinate_lift_proof",
        branch_of_math="dynamical systems",
        solved_world="A lifted coordinate representation with two cases.",
        core_story_md="Define a unique decomposition, derive transition laws by case, and prove an energy descent bridge.",
        core_objects=[
            LimaObjectSpec(object_kind="state_space", name="LiftedState"),
            LimaObjectSpec(object_kind="operator", name="LiftedStep"),
            LimaObjectSpec(object_kind="potential", name="Energy"),
        ],
        laws=[
            LimaClaimSpec(
                claim_kind="law",
                title="case transitions",
                statement_md="The operator has exact transition laws in two regimes.",
            )
        ],
        backward_translation=["Project lifted termination to the surface system."],
        bridge_lemmas=[
            LimaClaimSpec(
                claim_kind="bridge_lemma",
                title="surface bridge",
                statement_md="Lifted termination implies surface termination.",
            )
        ],
    )
    assert infer_ontology_class_from_universe(universe) == "coordinate_lift"
    obligations = compile_obligations_for_universe(universe)
    titles = {obligation.title for obligation in obligations}
    assert "uniqueness_of_representation" in titles
    assert "exact_transition_law_case_A" in titles
    assert "exact_transition_law_case_B" in titles
    assert "ranking_or_lexicographic_descent" in titles
    assert "bridge_to_surface_system" in titles


def test_lima_transfer_metric_persistence(tmp_path: Path) -> None:
    db = LimaDatabase(str(tmp_path / "lima.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    metric_id = db.record_transfer_metric(
        problem_id=problem["id"],
        run_id="run123",
        benchmark_id="holdout_synthetic",
        metric={
            "duplicate_family_rate": 0.25,
            "ontology_class_distribution": {"coordinate_lift": 1, "rewrite_system": 1},
            "benchmark_leakage_risk": 0,
        },
    )
    assert metric_id
    rows = db.list_transfer_metrics(problem["id"])
    assert rows[0]["benchmark_id"] == "holdout_synthetic"
    assert "duplicate_family_rate" in rows[0]["metric_json"]


def test_lima_problem_pause_resume_and_schedulable_filter(tmp_path: Path) -> None:
    db = LimaDatabase(str(tmp_path / "lima.db"))
    db.initialize()
    twin_id, _created = db.create_problem(
        slug="twin_primes",
        title="Twin primes",
        statement_md="Infinitely many primes p have p+2 prime.",
        domain="number_theory",
        default_goal_text="Search for good universes.",
    )
    active_slugs = [row["slug"] for row in db.list_schedulable_problems()]
    assert "collatz" in active_slugs
    assert "twin_primes" in active_slugs
    paused = db.update_problem_status("collatz", status="paused")
    assert paused["status"] == "paused"
    active_slugs = [row["slug"] for row in db.list_schedulable_problems()]
    assert "collatz" not in active_slugs
    assert "twin_primes" in active_slugs
    resumed = db.update_problem_status(twin_id, status="archived")
    assert resumed["status"] == "archived"
    active_slugs = [row["slug"] for row in db.list_schedulable_problems()]
    assert "twin_primes" not in active_slugs
    db.update_problem_status("collatz", status="active")
    assert "collatz" in [row["slug"] for row in db.list_schedulable_problems()]


def test_lima_literature_and_meta_policy_persistence(tmp_path: Path) -> None:
    db = LimaDatabase(str(tmp_path / "lima.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    res = refresh_literature(
        db,
        problem=problem,
        pressure_map={"tensions": ["2-adic Collatz completion"]},
    )
    assert res["source_count"] >= 1
    sources = db.list_literature_sources(problem["id"], limit=10)
    assert sources
    extracts = db.list_literature_extracts([sources[0]["id"]])
    assert extracts

    meta = analyze_and_update_policy(db, problem_id=problem["id"])
    assert meta["revision_id"]
    revisions = db.list_policy_revisions(problem["id"], limit=10)
    assert revisions
    assert "zero-live-authority" in revisions[0]["change_reason_md"] or "zero-live-authority".replace("-", " ") in revisions[0]["change_reason_md"].lower()


def test_lima_real_literature_backends_parse_http_payloads(monkeypatch) -> None:
    class FakeResponse:
        def __init__(self, *, text: str = "", payload: dict | None = None) -> None:
            self.text = text
            self._payload = payload or {}

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self._payload

    def fake_get(url, **kwargs):
        if "export.arxiv.org" in url:
            return FakeResponse(
                text="""<?xml version="1.0" encoding="UTF-8"?>
                <feed xmlns="http://www.w3.org/2005/Atom">
                  <entry>
                    <id>http://arxiv.org/abs/2401.00001v1</id>
                    <title>Collatz residue dynamics</title>
                    <summary>A note about residue dynamics.</summary>
                    <published>2024-01-01T00:00:00Z</published>
                    <author><name>A. Author</name></author>
                  </entry>
                </feed>"""
            )
        if "semanticscholar" in url:
            return FakeResponse(
                payload={
                    "data": [
                        {
                            "paperId": "p1",
                            "title": "Semantic Collatz paper",
                            "authors": [{"name": "B. Author"}],
                            "year": 2025,
                            "venue": "Venue",
                            "abstract": "Abstract",
                            "url": "https://example.com/p1",
                            "externalIds": {"DOI": "10.1/test", "ArXiv": "2501.1"},
                        }
                    ]
                }
            )
        return FakeResponse(
            payload={
                "message": {
                    "items": [
                        {
                            "title": ["Crossref Collatz paper"],
                            "author": [{"given": "C.", "family": "Author"}],
                            "published-online": {"date-parts": [[2023, 1, 1]]},
                            "container-title": ["Journal"],
                            "DOI": "10.2/test",
                            "URL": "https://doi.org/10.2/test",
                            "abstract": "<jats:p>Crossref abstract.</jats:p>",
                        }
                    ]
                }
            }
        )

    monkeypatch.setattr("orchestrator.lima_literature.httpx.get", fake_get)
    problem = {"slug": "collatz", "title": "Collatz conjecture"}
    queries = ["Collatz residue dynamics"]
    assert ArxivLiteratureBackend().search(problem=problem, queries=queries, limit=1)[0].arxiv_id == "2401.00001v1"
    assert SemanticScholarLiteratureBackend().search(problem=problem, queries=queries, limit=1)[0].doi == "10.1/test"
    crossref = CrossrefLiteratureBackend().search(problem=problem, queries=queries, limit=1)[0]
    assert crossref.title == "Crossref Collatz paper"
    assert crossref.year == 2023


def test_lima_run_persists_memory_without_live_main_queue(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(app_config, "LLM_API_KEY", "")
    main_db = Database(str(tmp_path / "main.db"))
    main_db.initialize()
    main_db.create_campaign(
        "Collatz residue campaign",
        workspace_root=str(tmp_path / "ws"),
        workspace_template="minimal",
    )
    lima = LimaDatabase(str(tmp_path / "lima.db"))
    lima.initialize()

    result = asyncio.run(
        run_lima(lima, main_db, problem_slug="collatz", trigger_kind="manual", mode="forge")
    )
    assert result["ok"] is True
    problem = lima.get_problem("collatz")
    assert lima.list_universes(problem["id"])
    assert lima.list_fractures(problem["id"])
    obligations = lima.list_obligations(problem["id"])
    assert obligations
    assert any(o["status"] == "verified_local" for o in obligations)
    assert lima.list_handoffs(problem["id"], status="pending")

    conn = sqlite3.connect(str(tmp_path / "main.db"))
    try:
        assert int(conn.execute("SELECT COUNT(*) FROM experiments").fetchone()[0]) == 0
    finally:
        conn.close()


def test_lima_dashboard_run_and_handoff_routes(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(app_config, "LLM_API_KEY", "")
    monkeypatch.setattr(app_mod, "db", Database(str(tmp_path / "main.db")))
    monkeypatch.setattr(app_mod, "lima_db", LimaDatabase(str(tmp_path / "lima.db")))
    monkeypatch.setattr(app_mod, "manager_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "shadow_global_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "supershadow_global_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "lima_loop", _noop_loop)

    with TestClient(app_mod.app) as client:
        resp = client.get("/lima")
        assert resp.status_code == 200
        assert "Lima Lab" in resp.text
        assert "zero live authority" in resp.text
        assert "Start a Lima problem" in resp.text
        assert "Add a new Lima problem" in resp.text
        assert "Open workspace" in resp.text
        assert "Aristotle auto-submit" in resp.text

        start_resp = client.post(
            "/api/lima/start",
            data={
                "prompt": "Twin prime conjecture\nExplore falsification-first ontologies for infinitely many prime gaps of 2.",
                "title": "Twin prime conjecture",
                "mode": "balanced",
                "run_now": "1",
            },
        )
        assert start_resp.status_code == 200
        assert "Twin prime conjecture" in start_resp.text
        assert "All Lima problems" in start_resp.text
        assert app_mod.lima_db.get_problem("twin_prime_conjecture")["title"] == "Twin prime conjecture"

        create_resp = client.post(
            "/api/lima/problem",
            data={
                "title": "Goldbach conjecture",
                "slug": "goldbach",
                "statement_md": "Every even integer greater than 2 is a sum of two primes.",
                "domain": "number_theory",
                "default_goal_text": "Stress-test additive-number-theory universes.",
            },
        )
        assert create_resp.status_code == 200
        assert "Goldbach conjecture" in create_resp.text
        assert "All Lima problems" in create_resp.text

        detail_resp = client.get("/lima/goldbach")
        assert detail_resp.status_code == 200
        assert "Goldbach conjecture" in detail_resp.text
        assert "All Lima problems" in detail_resp.text

        run_resp = client.post(
            "/api/lima/run",
            data={"problem_slug": "goldbach", "mode": "balanced"},
        )
        assert run_resp.status_code == 200
        assert "Goldbach conjecture bridge-obligation atlas" in run_resp.text
        assert "Formal obligations" in run_resp.text
        assert "Hold for obligations" in run_resp.text

        problem = app_mod.lima_db.get_problem("goldbach")
        formal_obligations = [
            o
            for o in app_mod.lima_db.list_obligations(problem["id"])
            if o["status"] == "queued_formal_review"
        ]
        assert formal_obligations
        formal_resp = client.post(
            f"/api/lima/obligation/{formal_obligations[0]['id']}/approve-formal"
        )
        assert formal_resp.status_code == 200
        assert "No live Aristotle job was created" in formal_resp.text
        assert "Submit to Aristotle anyway" in formal_resp.text
        handoffs = app_mod.lima_db.list_handoffs(problem["id"], status="pending")
        assert handoffs
        hold_resp = client.post(f"/api/lima/handoff/{handoffs[0]['id']}/hold")
        assert hold_resp.status_code == 200
        assert "held for obligation review" in hold_resp.text
        approve_resp = client.post(f"/api/lima/handoff/{handoffs[0]['id']}/approve")
        assert approve_resp.status_code == 200
        assert "no live Aristotle job was created" in approve_resp.text

    conn = sqlite3.connect(str(tmp_path / "main.db"))
    try:
        assert int(conn.execute("SELECT COUNT(*) FROM experiments").fetchone()[0]) == 0
    finally:
        conn.close()
