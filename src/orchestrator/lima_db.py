from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from orchestrator import config as app_config
from orchestrator.lima_models import LimaUniverseSpec, safe_json_loads, slugify


def _new_id() -> str:
    return uuid4().hex[:12]


def _now() -> str:
    return datetime.utcnow().isoformat()


def _json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False)


def _routing_policy_from_seed(seed: Any, *, slug: str = "") -> dict[str, Any]:
    parsed = seed if isinstance(seed, dict) else safe_json_loads(seed, {})
    if not isinstance(parsed, dict):
        parsed = {}
    routing = parsed.get("routing_policy") if isinstance(parsed.get("routing_policy"), dict) else {}
    policy = dict(routing)
    for key in (
        "retrieval_keywords",
        "campaign_tags",
        "shadow_goal_id",
        "supershadow_goal_id",
        "literature_defaults",
        "bounded_test_templates",
    ):
        if key in parsed and key not in policy:
            policy[key] = parsed[key]
    if slug and "shadow_goal_id" not in policy:
        policy["shadow_goal_id"] = f"global_{slug}"
    if slug and "supershadow_goal_id" not in policy:
        policy["supershadow_goal_id"] = f"global_{slug}_supershadow"
    return policy


def _canonical_hash(parts: list[Any]) -> str:
    return hashlib.sha256(_json(parts).encode("utf-8")).hexdigest()


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    if column not in _column_names(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


class LimaDatabase:
    """Separate SQLite persistence for Lima.

    Lima intentionally stores typed external references and snapshots instead of
    foreign keys into the main orchestrator DB.
    """

    def __init__(self, path: str, *, reference_database_path: str | None = None) -> None:
        self.path = path
        self.reference_database_path = reference_database_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS lima_problem (
                    id TEXT PRIMARY KEY,
                    slug TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    statement_md TEXT NOT NULL DEFAULT '',
                    domain TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'active',
                    default_goal_text TEXT NOT NULL DEFAULT '',
                    seed_packet_json TEXT NOT NULL DEFAULT '{}',
                    routing_policy_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_problem_status ON lima_problem(status);

                CREATE TABLE IF NOT EXISTS lima_state (
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
                CREATE INDEX IF NOT EXISTS idx_lima_state_problem ON lima_state(problem_id);

                CREATE TABLE IF NOT EXISTS lima_run (
                    id TEXT PRIMARY KEY,
                    problem_id TEXT NOT NULL,
                    trigger_kind TEXT NOT NULL DEFAULT 'manual',
                    mode TEXT NOT NULL DEFAULT 'balanced',
                    run_summary_md TEXT NOT NULL DEFAULT '',
                    frontier_snapshot_json TEXT NOT NULL DEFAULT '{}',
                    pressure_snapshot_json TEXT NOT NULL DEFAULT '{}',
                    policy_snapshot_json TEXT NOT NULL DEFAULT '{}',
                    response_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_run_problem ON lima_run(problem_id);
                CREATE INDEX IF NOT EXISTS idx_lima_run_created ON lima_run(created_at);
                CREATE INDEX IF NOT EXISTS idx_lima_run_problem_created ON lima_run(problem_id, created_at);

                CREATE TABLE IF NOT EXISTS lima_universe_family (
                    id TEXT PRIMARY KEY,
                    problem_id TEXT NOT NULL,
                    family_key TEXT NOT NULL,
                    family_kind TEXT NOT NULL DEFAULT 'new',
                    parent_family_id TEXT,
                    thesis_md TEXT NOT NULL DEFAULT '',
                    novelty_prior REAL NOT NULL DEFAULT 0,
                    saturation_penalty REAL NOT NULL DEFAULT 0,
                    survival_count INTEGER NOT NULL DEFAULT 0,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    formal_win_count INTEGER NOT NULL DEFAULT 0,
                    last_seen_at TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    UNIQUE(problem_id, family_key)
                );
                CREATE INDEX IF NOT EXISTS idx_lima_family_problem ON lima_universe_family(problem_id);
                CREATE INDEX IF NOT EXISTS idx_lima_family_status ON lima_universe_family(status);
                CREATE INDEX IF NOT EXISTS idx_lima_family_last_seen ON lima_universe_family(last_seen_at);

                CREATE TABLE IF NOT EXISTS lima_universe (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    problem_id TEXT NOT NULL,
                    family_id TEXT,
                    parent_universe_id TEXT,
                    title TEXT NOT NULL DEFAULT '',
                    branch_of_math TEXT NOT NULL DEFAULT '',
                    solved_world TEXT NOT NULL DEFAULT '',
                    why_problem_is_easy_here TEXT NOT NULL DEFAULT '',
                    core_story_md TEXT NOT NULL DEFAULT '',
                    universe_status TEXT NOT NULL DEFAULT 'proposed',
                    compression_score REAL NOT NULL DEFAULT 0,
                    fit_score REAL NOT NULL DEFAULT 0,
                    novelty_score REAL NOT NULL DEFAULT 0,
                    falsifiability_score REAL NOT NULL DEFAULT 0,
                    bridgeability_score REAL NOT NULL DEFAULT 0,
                    formalizability_score REAL NOT NULL DEFAULT 0,
                    theorem_yield_score REAL NOT NULL DEFAULT 0,
                    literature_novelty_score REAL NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_universe_problem ON lima_universe(problem_id);
                CREATE INDEX IF NOT EXISTS idx_lima_universe_run ON lima_universe(run_id);
                CREATE INDEX IF NOT EXISTS idx_lima_universe_family ON lima_universe(family_id);
                CREATE INDEX IF NOT EXISTS idx_lima_universe_status ON lima_universe(universe_status);
                CREATE INDEX IF NOT EXISTS idx_lima_universe_created ON lima_universe(created_at);

                CREATE TABLE IF NOT EXISTS lima_universe_object (
                    id TEXT PRIMARY KEY,
                    universe_id TEXT NOT NULL,
                    object_kind TEXT NOT NULL DEFAULT 'state_space',
                    name TEXT NOT NULL DEFAULT '',
                    description_md TEXT NOT NULL DEFAULT '',
                    formal_shape TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_lima_object_universe ON lima_universe_object(universe_id);
                CREATE INDEX IF NOT EXISTS idx_lima_object_kind ON lima_universe_object(object_kind);

                CREATE TABLE IF NOT EXISTS lima_claim (
                    id TEXT PRIMARY KEY,
                    universe_id TEXT NOT NULL,
                    claim_kind TEXT NOT NULL DEFAULT 'law',
                    title TEXT NOT NULL DEFAULT '',
                    statement_md TEXT NOT NULL DEFAULT '',
                    formal_statement TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'open',
                    priority INTEGER NOT NULL DEFAULT 3,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_claim_universe ON lima_claim(universe_id);
                CREATE INDEX IF NOT EXISTS idx_lima_claim_status ON lima_claim(status);
                CREATE INDEX IF NOT EXISTS idx_lima_claim_kind ON lima_claim(claim_kind);

                CREATE TABLE IF NOT EXISTS lima_claim_edge (
                    id TEXT PRIMARY KEY,
                    from_claim_id TEXT NOT NULL,
                    to_claim_id TEXT NOT NULL,
                    edge_kind TEXT NOT NULL DEFAULT 'depends_on',
                    note TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_lima_claim_edge_from ON lima_claim_edge(from_claim_id);
                CREATE INDEX IF NOT EXISTS idx_lima_claim_edge_to ON lima_claim_edge(to_claim_id);
                CREATE INDEX IF NOT EXISTS idx_lima_claim_edge_kind ON lima_claim_edge(edge_kind);

                CREATE TABLE IF NOT EXISTS lima_obligation (
                    id TEXT PRIMARY KEY,
                    problem_id TEXT NOT NULL,
                    universe_id TEXT NOT NULL,
                    claim_id TEXT,
                    obligation_kind TEXT NOT NULL DEFAULT 'bridge_lemma',
                    title TEXT NOT NULL DEFAULT '',
                    statement_md TEXT NOT NULL DEFAULT '',
                    lean_goal TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'queued_local',
                    priority INTEGER NOT NULL DEFAULT 3,
                    why_exists_md TEXT NOT NULL DEFAULT '',
                    prove_or_kill_md TEXT NOT NULL DEFAULT '',
                    why_this_exists TEXT NOT NULL DEFAULT '',
                    what_success_would_show TEXT NOT NULL DEFAULT '',
                    what_failure_would_kill TEXT NOT NULL DEFAULT '',
                    source_run_id TEXT NOT NULL DEFAULT '',
                    source_universe_id TEXT NOT NULL DEFAULT '',
                    source_claim_id TEXT NOT NULL DEFAULT '',
                    source_claim_ids_json TEXT NOT NULL DEFAULT '[]',
                    lineage_json TEXT NOT NULL DEFAULT '{}',
                    canonical_hash TEXT NOT NULL DEFAULT '',
                    review_status TEXT NOT NULL DEFAULT 'not_reviewed',
                    formal_backend TEXT NOT NULL DEFAULT '',
                    formal_payload_json TEXT NOT NULL DEFAULT '{}',
                    formal_submission_ref_json TEXT NOT NULL DEFAULT '{}',
                    review_note TEXT NOT NULL DEFAULT '',
                    reviewed_at TEXT,
                    estimated_formalization_value REAL NOT NULL DEFAULT 0,
                    estimated_execution_cost REAL NOT NULL DEFAULT 0,
                    estimated_value REAL NOT NULL DEFAULT 0,
                    estimated_cost REAL NOT NULL DEFAULT 0,
                    aristotle_ref_json TEXT NOT NULL DEFAULT '{}',
                    result_summary_md TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_obligation_problem ON lima_obligation(problem_id);
                CREATE INDEX IF NOT EXISTS idx_lima_obligation_universe ON lima_obligation(universe_id);
                CREATE INDEX IF NOT EXISTS idx_lima_obligation_claim ON lima_obligation(claim_id);
                CREATE INDEX IF NOT EXISTS idx_lima_obligation_status ON lima_obligation(status);
                CREATE INDEX IF NOT EXISTS idx_lima_obligation_created ON lima_obligation(created_at);
                CREATE TABLE IF NOT EXISTS lima_formal_review_queue (
                    id TEXT PRIMARY KEY,
                    problem_id TEXT NOT NULL,
                    obligation_id TEXT NOT NULL,
                    universe_id TEXT,
                    claim_id TEXT,
                    family_id TEXT,
                    claim_ids_json TEXT NOT NULL DEFAULT '[]',
                    rupture_summary_md TEXT NOT NULL DEFAULT '',
                    literature_links_json TEXT NOT NULL DEFAULT '[]',
                    policy_revision_id TEXT NOT NULL DEFAULT '',
                    lineage_json TEXT NOT NULL DEFAULT '{}',
                    backend_kind TEXT NOT NULL DEFAULT 'local_stub',
                    status TEXT NOT NULL DEFAULT 'queued_formal_review',
                    review_decision TEXT NOT NULL DEFAULT 'pending',
                    packet_json TEXT NOT NULL DEFAULT '{}',
                    backend_result_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    reviewed_at TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_formal_problem ON lima_formal_review_queue(problem_id);
                CREATE INDEX IF NOT EXISTS idx_lima_formal_obligation ON lima_formal_review_queue(obligation_id);
                CREATE INDEX IF NOT EXISTS idx_lima_formal_status ON lima_formal_review_queue(status);

                CREATE TABLE IF NOT EXISTS lima_rupture_run (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    universe_id TEXT NOT NULL,
                    attack_suite_json TEXT NOT NULL DEFAULT '{}',
                    summary_md TEXT NOT NULL DEFAULT '',
                    verdict TEXT NOT NULL DEFAULT 'inconclusive',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_rupture_run_run ON lima_rupture_run(run_id);
                CREATE INDEX IF NOT EXISTS idx_lima_rupture_run_universe ON lima_rupture_run(universe_id);
                CREATE INDEX IF NOT EXISTS idx_lima_rupture_run_verdict ON lima_rupture_run(verdict);
                CREATE INDEX IF NOT EXISTS idx_lima_rupture_run_created ON lima_rupture_run(created_at);

                CREATE TABLE IF NOT EXISTS lima_fracture (
                    id TEXT PRIMARY KEY,
                    problem_id TEXT NOT NULL,
                    family_id TEXT,
                    universe_id TEXT NOT NULL,
                    rupture_run_id TEXT,
                    failure_type TEXT NOT NULL DEFAULT 'weak_explanation',
                    breakpoint_md TEXT NOT NULL DEFAULT '',
                    smallest_counterexample_json TEXT NOT NULL DEFAULT '{}',
                    boundary_region_json TEXT NOT NULL DEFAULT '{}',
                    reusable_negative_theorem_md TEXT NOT NULL DEFAULT '',
                    surviving_fragment_md TEXT NOT NULL DEFAULT '',
                    confidence REAL NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_fracture_problem ON lima_fracture(problem_id);
                CREATE INDEX IF NOT EXISTS idx_lima_fracture_family ON lima_fracture(family_id);
                CREATE INDEX IF NOT EXISTS idx_lima_fracture_universe ON lima_fracture(universe_id);
                CREATE INDEX IF NOT EXISTS idx_lima_fracture_failure ON lima_fracture(failure_type);
                CREATE INDEX IF NOT EXISTS idx_lima_fracture_created ON lima_fracture(created_at);

                CREATE TABLE IF NOT EXISTS lima_reference (
                    id TEXT PRIMARY KEY,
                    problem_id TEXT NOT NULL,
                    universe_id TEXT,
                    reference_kind TEXT NOT NULL DEFAULT 'manual_note',
                    external_source TEXT NOT NULL DEFAULT '',
                    external_id TEXT NOT NULL DEFAULT '',
                    snapshot_json TEXT NOT NULL DEFAULT '{}',
                    note TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_reference_problem ON lima_reference(problem_id);
                CREATE INDEX IF NOT EXISTS idx_lima_reference_universe ON lima_reference(universe_id);
                CREATE INDEX IF NOT EXISTS idx_lima_reference_kind ON lima_reference(reference_kind);
                CREATE INDEX IF NOT EXISTS idx_lima_reference_external ON lima_reference(external_source, external_id);

                CREATE TABLE IF NOT EXISTS lima_literature_source (
                    id TEXT PRIMARY KEY,
                    problem_id TEXT NOT NULL,
                    source_type TEXT NOT NULL DEFAULT 'manual',
                    title TEXT NOT NULL DEFAULT '',
                    authors_json TEXT NOT NULL DEFAULT '[]',
                    year INTEGER,
                    venue TEXT NOT NULL DEFAULT '',
                    doi TEXT NOT NULL DEFAULT '',
                    arxiv_id TEXT NOT NULL DEFAULT '',
                    url TEXT NOT NULL DEFAULT '',
                    abstract_md TEXT NOT NULL DEFAULT '',
                    bibtex_json TEXT NOT NULL DEFAULT '{}',
                    fetched_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_literature_source_problem ON lima_literature_source(problem_id);
                CREATE INDEX IF NOT EXISTS idx_lima_literature_source_arxiv ON lima_literature_source(arxiv_id);
                CREATE INDEX IF NOT EXISTS idx_lima_literature_source_doi ON lima_literature_source(doi);

                CREATE TABLE IF NOT EXISTS lima_literature_extract (
                    id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    extract_kind TEXT NOT NULL DEFAULT 'method',
                    title TEXT NOT NULL DEFAULT '',
                    body_md TEXT NOT NULL DEFAULT '',
                    formal_hint TEXT NOT NULL DEFAULT '',
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    relevance_score REAL NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_literature_extract_source ON lima_literature_extract(source_id);
                CREATE INDEX IF NOT EXISTS idx_lima_literature_extract_kind ON lima_literature_extract(extract_kind);

                CREATE TABLE IF NOT EXISTS lima_universe_literature_link (
                    id TEXT PRIMARY KEY,
                    universe_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    relation_kind TEXT NOT NULL DEFAULT 'support',
                    note TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_universe_lit_universe ON lima_universe_literature_link(universe_id);
                CREATE INDEX IF NOT EXISTS idx_lima_universe_lit_source ON lima_universe_literature_link(source_id);
                CREATE INDEX IF NOT EXISTS idx_lima_universe_lit_relation ON lima_universe_literature_link(relation_kind);

                CREATE TABLE IF NOT EXISTS lima_meta_run (
                    id TEXT PRIMARY KEY,
                    problem_id TEXT NOT NULL,
                    from_run_id TEXT,
                    analysis_summary_md TEXT NOT NULL DEFAULT '',
                    policy_changes_json TEXT NOT NULL DEFAULT '{}',
                    benchmark_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_meta_run_problem ON lima_meta_run(problem_id);
                CREATE INDEX IF NOT EXISTS idx_lima_meta_run_from ON lima_meta_run(from_run_id);

                CREATE TABLE IF NOT EXISTS lima_policy_revision (
                    id TEXT PRIMARY KEY,
                    problem_id TEXT NOT NULL,
                    revision_no INTEGER NOT NULL,
                    generation_policy_json TEXT NOT NULL DEFAULT '{}',
                    rupture_policy_json TEXT NOT NULL DEFAULT '{}',
                    literature_policy_json TEXT NOT NULL DEFAULT '{}',
                    formal_policy_json TEXT NOT NULL DEFAULT '{}',
                    scoring_weights_json TEXT NOT NULL DEFAULT '{}',
                    change_reason_md TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_policy_problem ON lima_policy_revision(problem_id);
                CREATE INDEX IF NOT EXISTS idx_lima_policy_revision ON lima_policy_revision(problem_id, revision_no);

                CREATE TABLE IF NOT EXISTS lima_handoff_request (
                    id TEXT PRIMARY KEY,
                    problem_id TEXT NOT NULL,
                    universe_id TEXT NOT NULL,
                    destination_kind TEXT NOT NULL DEFAULT 'review_packet',
                    status TEXT NOT NULL DEFAULT 'pending',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    reviewed_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_lima_handoff_problem ON lima_handoff_request(problem_id);
                CREATE INDEX IF NOT EXISTS idx_lima_handoff_universe ON lima_handoff_request(universe_id);
                CREATE INDEX IF NOT EXISTS idx_lima_handoff_status ON lima_handoff_request(status);
                CREATE INDEX IF NOT EXISTS idx_lima_handoff_created ON lima_handoff_request(created_at);

                CREATE TABLE IF NOT EXISTS lima_artifact (
                    id TEXT PRIMARY KEY,
                    problem_id TEXT NOT NULL,
                    universe_id TEXT,
                    artifact_kind TEXT NOT NULL DEFAULT 'benchmark',
                    content_json TEXT NOT NULL DEFAULT '{}',
                    content_hash TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lima_artifact_problem ON lima_artifact(problem_id);
                CREATE INDEX IF NOT EXISTS idx_lima_artifact_universe ON lima_artifact(universe_id);
                CREATE INDEX IF NOT EXISTS idx_lima_artifact_kind ON lima_artifact(artifact_kind);
                CREATE INDEX IF NOT EXISTS idx_lima_artifact_hash ON lima_artifact(content_hash);
                """
            )
            self._migrate_schema(conn)
            conn.commit()
        finally:
            conn.close()
        self.ensure_default_problem()

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        _ensure_column(conn, "lima_problem", "routing_policy_json", "TEXT NOT NULL DEFAULT '{}'")
        _ensure_column(conn, "lima_obligation", "why_exists_md", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "lima_obligation", "prove_or_kill_md", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "lima_obligation", "why_this_exists", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "lima_obligation", "what_success_would_show", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "lima_obligation", "what_failure_would_kill", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "lima_obligation", "source_run_id", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "lima_obligation", "source_universe_id", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "lima_obligation", "source_claim_id", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "lima_obligation", "source_claim_ids_json", "TEXT NOT NULL DEFAULT '[]'")
        _ensure_column(conn, "lima_obligation", "lineage_json", "TEXT NOT NULL DEFAULT '{}'")
        _ensure_column(conn, "lima_obligation", "canonical_hash", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "lima_obligation", "review_status", "TEXT NOT NULL DEFAULT 'not_reviewed'")
        _ensure_column(conn, "lima_obligation", "formal_backend", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "lima_obligation", "formal_payload_json", "TEXT NOT NULL DEFAULT '{}'")
        _ensure_column(conn, "lima_obligation", "formal_submission_ref_json", "TEXT NOT NULL DEFAULT '{}'")
        _ensure_column(conn, "lima_obligation", "review_note", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "lima_obligation", "reviewed_at", "TEXT")
        _ensure_column(conn, "lima_obligation", "estimated_formalization_value", "REAL NOT NULL DEFAULT 0")
        _ensure_column(conn, "lima_obligation", "estimated_execution_cost", "REAL NOT NULL DEFAULT 0")
        _ensure_column(conn, "lima_obligation", "estimated_value", "REAL NOT NULL DEFAULT 0")
        _ensure_column(conn, "lima_obligation", "estimated_cost", "REAL NOT NULL DEFAULT 0")
        _ensure_column(conn, "lima_formal_review_queue", "family_id", "TEXT")
        _ensure_column(conn, "lima_formal_review_queue", "claim_ids_json", "TEXT NOT NULL DEFAULT '[]'")
        _ensure_column(conn, "lima_formal_review_queue", "rupture_summary_md", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "lima_formal_review_queue", "literature_links_json", "TEXT NOT NULL DEFAULT '[]'")
        _ensure_column(conn, "lima_formal_review_queue", "policy_revision_id", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "lima_formal_review_queue", "lineage_json", "TEXT NOT NULL DEFAULT '{}'")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lima_obligation_canonical ON lima_obligation(problem_id, canonical_hash)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lima_obligation_backend ON lima_obligation(formal_backend)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lima_obligation_reviewed ON lima_obligation(reviewed_at)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lima_formal_family ON lima_formal_review_queue(family_id)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lima_formal_backend ON lima_formal_review_queue(backend_kind)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lima_formal_reviewed ON lima_formal_review_queue(reviewed_at)")
        conn.execute("UPDATE lima_obligation SET status = 'queued_local' WHERE status = 'queued'")
        conn.execute("UPDATE lima_obligation SET status = 'verified_local' WHERE status = 'checked'")
        conn.execute("UPDATE lima_obligation SET status = 'refuted_local' WHERE status = 'falsified'")

    def ensure_default_problem(self) -> str:
        slug = slugify(app_config.LIMA_DEFAULT_PROBLEM, fallback="collatz")
        seed_packet = {
            "known_frontier": [
                "Parity-vector and residue-class structure is central.",
                "Odd/even induced dynamics and 2-adic completions are plausible bridge languages.",
                "Naive global height monotonicity is a known trap.",
            ],
            "retrieval_keywords": ["Collatz", "3x+1", "parity vector", "residue class"],
            "campaign_tags": ["collatz", "3x+1"],
            "routing_policy": {
                "retrieval_keywords": ["Collatz", "3x+1", "parity vector", "residue class"],
                "campaign_tags": ["collatz", "3x+1"],
                "shadow_goal_id": "global_collatz",
                "supershadow_goal_id": "global_collatz_supershadow",
                "literature_defaults": ["local", "local_file"],
                "bounded_test_templates": ["residue_scan_mod_16", "counterexample_hunt"],
            },
            "default_modes": ["balanced", "wild", "stress", "forge"],
        }
        return self.ensure_problem(
            slug=slug,
            title="Collatz conjecture" if slug == "collatz" else slug.replace("_", " ").title(),
            statement_md=(
                "For every positive integer n, repeated application of n/2 when n is even "
                "and 3n+1 when n is odd eventually reaches 1."
            )
            if slug == "collatz"
            else "",
            domain="number_theory",
            default_goal_text=(
                "Find falsification-first conceptual universes that make Collatz easier, "
                "then compile only narrow survivors into formalizable obligations."
            ),
            seed_packet_json=seed_packet,
        )

    def ensure_problem(
        self,
        *,
        slug: str,
        title: str,
        statement_md: str = "",
        domain: str = "",
        default_goal_text: str = "",
        seed_packet_json: dict[str, Any] | str | None = None,
    ) -> str:
        now = _now()
        slug = slugify(slug, fallback="problem")
        seed = seed_packet_json
        if isinstance(seed, str):
            seed_raw = seed if seed.strip() else "{}"
        else:
            seed_raw = _json(seed or {})
        routing_policy_raw = _json(_routing_policy_from_seed(seed_raw, slug=slug))
        conn = self._connect()
        try:
            row = conn.execute("SELECT id FROM lima_problem WHERE slug = ?", (slug,)).fetchone()
            if row:
                pid = str(row["id"])
                conn.execute(
                    """
                    UPDATE lima_problem
                    SET title = COALESCE(NULLIF(?, ''), title),
                        statement_md = COALESCE(NULLIF(?, ''), statement_md),
                        domain = COALESCE(NULLIF(?, ''), domain),
                        default_goal_text = COALESCE(NULLIF(?, ''), default_goal_text),
                        routing_policy_json = COALESCE(NULLIF(?, '{}'), routing_policy_json),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (title, statement_md, domain, default_goal_text, routing_policy_raw, now, pid),
                )
            else:
                pid = _new_id()
                conn.execute(
                    """
                    INSERT INTO lima_problem (
                        id, slug, title, statement_md, domain, status, default_goal_text,
                        seed_packet_json, routing_policy_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)
                    """,
                    (
                        pid,
                        slug,
                        title,
                        statement_md,
                        domain,
                        default_goal_text,
                        seed_raw,
                        routing_policy_raw,
                        now,
                        now,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO lima_state (
                        problem_id, revision, frontier_summary_md, frontier_json,
                        pressure_map_json, worldview_json, policy_json,
                        generation_priors_json, rupture_policy_json,
                        literature_policy_json, formal_policy_json, updated_at
                    )
                    VALUES (?, 0, '', ?, '{}', '{}', '{}', '{}', '{}', '{}', '{}', ?)
                    """,
                    (pid, seed_raw, now),
                )
            conn.commit()
            return pid
        finally:
            conn.close()

    def create_problem(
        self,
        *,
        slug: str,
        title: str,
        statement_md: str,
        domain: str,
        default_goal_text: str,
        seed_packet_json: dict[str, Any] | str | None = None,
    ) -> tuple[str, bool]:
        now = _now()
        normalized_slug = slugify(slug or title, fallback="problem")
        seed = seed_packet_json
        if isinstance(seed, str):
            seed_raw = seed if seed.strip() else "{}"
        else:
            seed_raw = _json(seed or {})
        routing_policy_raw = _json(_routing_policy_from_seed(seed_raw, slug=normalized_slug))
        conn = self._connect()
        try:
            existing = conn.execute(
                "SELECT id FROM lima_problem WHERE slug = ?",
                (normalized_slug,),
            ).fetchone()
            if existing:
                problem_id = str(existing["id"])
                conn.execute(
                    """
                    UPDATE lima_problem
                    SET title = ?, statement_md = ?, domain = ?,
                        default_goal_text = ?, seed_packet_json = ?, status = 'active',
                        routing_policy_json = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        title[:500],
                        statement_md,
                        domain[:120],
                        default_goal_text[:4000],
                        seed_raw,
                        routing_policy_raw,
                        now,
                        problem_id,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO lima_state (
                        problem_id, revision, frontier_summary_md, frontier_json,
                        pressure_map_json, worldview_json, policy_json,
                        generation_priors_json, rupture_policy_json,
                        literature_policy_json, formal_policy_json, updated_at
                    )
                    VALUES (?, 0, '', ?, '{}', '{}', '{}', '{}', '{}', '{}', '{}', ?)
                    ON CONFLICT(problem_id) DO UPDATE SET
                        frontier_json = excluded.frontier_json,
                        updated_at = excluded.updated_at
                    """,
                    (problem_id, seed_raw, now),
                )
                conn.commit()
                return problem_id, False
            problem_id = _new_id()
            conn.execute(
                """
                INSERT INTO lima_problem (
                    id, slug, title, statement_md, domain, status, default_goal_text,
                    seed_packet_json, routing_policy_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)
                """,
                (
                    problem_id,
                    normalized_slug,
                    title[:500],
                    statement_md,
                    domain[:120],
                    default_goal_text[:4000],
                    seed_raw,
                    routing_policy_raw,
                    now,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO lima_state (
                    problem_id, revision, frontier_summary_md, frontier_json,
                    pressure_map_json, worldview_json, policy_json,
                    generation_priors_json, rupture_policy_json,
                    literature_policy_json, formal_policy_json, updated_at
                )
                VALUES (?, 0, '', ?, '{}', '{}', '{}', '{}', '{}', '{}', '{}', ?)
                """,
                (problem_id, seed_raw, now),
            )
            conn.commit()
            return problem_id, True
        finally:
            conn.close()

    def list_problems(self) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute("SELECT * FROM lima_problem ORDER BY created_at ASC")
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def get_problem(self, problem_id_or_slug: str | None = None) -> dict[str, Any]:
        key = problem_id_or_slug or app_config.LIMA_DEFAULT_PROBLEM
        self.ensure_default_problem()
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM lima_problem WHERE id = ? OR slug = ?",
                (key, slugify(key, fallback="collatz")),
            ).fetchone()
            if row:
                return dict(row)
            fallback = conn.execute(
                "SELECT * FROM lima_problem ORDER BY created_at ASC LIMIT 1"
            ).fetchone()
            if not fallback:
                raise ValueError("Lima has no problem rows after initialization")
            return dict(fallback)
        finally:
            conn.close()

    def get_state(self, problem_id: str) -> dict[str, Any]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM lima_state WHERE problem_id = ?",
                (problem_id,),
            ).fetchone()
            return dict(row) if row else {}
        finally:
            conn.close()

    def get_latest_run(self, problem_id: str) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT * FROM lima_run
                WHERE problem_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT 1
                """,
                (problem_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_runs(self, problem_id: str, limit: int = 20) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT * FROM lima_run
                WHERE problem_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT ?
                """,
                (problem_id, min(limit, 200)),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def _upsert_family(
        self,
        conn: sqlite3.Connection,
        problem_id: str,
        universe: LimaUniverseSpec,
        *,
        verdict: str | None = None,
    ) -> str:
        now = _now()
        family_key = slugify(universe.family_key or universe.title, fallback="universe")
        row = conn.execute(
            "SELECT * FROM lima_universe_family WHERE problem_id = ? AND family_key = ?",
            (problem_id, family_key),
        ).fetchone()
        status = "active"
        survival_inc = 1 if verdict in {"survived"} else 0
        failure_inc = 1 if verdict in {"collapsed", "weakened"} else 0
        thesis = universe.core_story_md or universe.solved_world or universe.title
        if row:
            family_id = str(row["id"])
            conn.execute(
                """
                UPDATE lima_universe_family
                SET family_kind = ?, thesis_md = COALESCE(NULLIF(?, ''), thesis_md),
                    survival_count = survival_count + ?,
                    failure_count = failure_count + ?,
                    saturation_penalty = saturation_penalty + 0.05,
                    last_seen_at = ?, status = ?
                WHERE id = ?
                """,
                (
                    universe.family_kind,
                    thesis[:4000],
                    survival_inc,
                    failure_inc,
                    now,
                    status,
                    family_id,
                ),
            )
            return family_id
        family_id = _new_id()
        novelty_prior = universe.score("novelty_score", 3)
        conn.execute(
            """
            INSERT INTO lima_universe_family (
                id, problem_id, family_key, family_kind, parent_family_id, thesis_md,
                novelty_prior, saturation_penalty, survival_count, failure_count,
                formal_win_count, last_seen_at, status
            )
            VALUES (?, ?, ?, ?, NULL, ?, ?, 0, ?, ?, 0, ?, ?)
            """,
            (
                family_id,
                problem_id,
                family_key,
                universe.family_kind,
                thesis[:4000],
                novelty_prior,
                survival_inc,
                failure_inc,
                now,
                status,
            ),
        )
        return family_id

    def commit_run(
        self,
        *,
        problem_id: str,
        trigger_kind: str,
        mode: str,
        run_summary_md: str,
        frontier_snapshot: dict[str, Any],
        pressure_snapshot: dict[str, Any],
        policy_snapshot: dict[str, Any],
        response_obj: dict[str, Any],
        universes: list[LimaUniverseSpec],
        rupture_reports: list[dict[str, Any]] | None = None,
        reference_points: list[dict[str, Any]] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
    ) -> str:
        now = _now()
        run_id = _new_id()
        rupture_by_title = {
            str(r.get("universe_title") or ""): r for r in rupture_reports or []
        }
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO lima_run (
                    id, problem_id, trigger_kind, mode, run_summary_md,
                    frontier_snapshot_json, pressure_snapshot_json,
                    policy_snapshot_json, response_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    problem_id,
                    trigger_kind[:64],
                    mode[:32],
                    run_summary_md[:8000],
                    _json(frontier_snapshot),
                    _json(pressure_snapshot),
                    _json(policy_snapshot),
                    _json(response_obj),
                    now,
                ),
            )
            universe_id_by_title: dict[str, str] = {}
            for universe in universes:
                rupture = rupture_by_title.get(universe.title, {})
                verdict = str(rupture.get("verdict") or "inconclusive")
                family_id = self._upsert_family(conn, problem_id, universe, verdict=verdict)
                status = {
                    "collapsed": "dead",
                    "weakened": "weakened",
                    "survived": "promising",
                }.get(verdict, "proposed")
                universe_id = _new_id()
                universe_id_by_title[universe.title] = universe_id
                conn.execute(
                    """
                    INSERT INTO lima_universe (
                        id, run_id, problem_id, family_id, parent_universe_id, title,
                        branch_of_math, solved_world, why_problem_is_easy_here,
                        core_story_md, universe_status, compression_score, fit_score,
                        novelty_score, falsifiability_score, bridgeability_score,
                        formalizability_score, theorem_yield_score,
                        literature_novelty_score, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        universe_id,
                        run_id,
                        problem_id,
                        family_id,
                        universe.title[:500],
                        universe.branch_of_math[:200],
                        universe.solved_world[:4000],
                        universe.why_problem_is_easy_here[:4000],
                        universe.core_story_md[:8000],
                        status,
                        universe.score("compression_score"),
                        universe.score("fit_score"),
                        universe.score("novelty_score"),
                        universe.score("falsifiability_score"),
                        universe.score("bridgeability_score"),
                        universe.score("formalizability_score"),
                        universe.score("theorem_yield_score"),
                        universe.score("literature_novelty_score"),
                        now,
                        now,
                    ),
                )
                for obj in universe.core_objects:
                    conn.execute(
                        """
                        INSERT INTO lima_universe_object (
                            id, universe_id, object_kind, name, description_md,
                            formal_shape, payload_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            _new_id(),
                            universe_id,
                            obj.object_kind[:80],
                            obj.name[:300],
                            obj.description_md[:4000],
                            obj.formal_shape[:2000],
                            _json(obj.payload),
                        ),
                    )
                claim_ids_by_title: dict[str, str] = {}
                for claim in universe.all_claim_specs():
                    cid = _new_id()
                    claim_ids_by_title[claim.title] = cid
                    conn.execute(
                        """
                        INSERT INTO lima_claim (
                            id, universe_id, claim_kind, title, statement_md,
                            formal_statement, status, priority, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            cid,
                            universe_id,
                            claim.claim_kind[:80],
                            claim.title[:500],
                            claim.statement_md[:8000],
                            claim.formal_statement[:4000],
                            claim.status[:32],
                            int(claim.priority),
                            now,
                            now,
                        ),
                    )
                for claim in universe.all_claim_specs():
                    from_id = claim_ids_by_title.get(claim.title)
                    if not from_id:
                        continue
                    for dep in claim.depends_on:
                        to_id = claim_ids_by_title.get(dep)
                        if to_id:
                            conn.execute(
                                "INSERT INTO lima_claim_edge (id, from_claim_id, to_claim_id, edge_kind, note) VALUES (?, ?, ?, 'depends_on', '')",
                                (_new_id(), from_id, to_id),
                            )
                    for conflict in claim.conflicts_with:
                        to_id = claim_ids_by_title.get(conflict)
                        if to_id:
                            conn.execute(
                                "INSERT INTO lima_claim_edge (id, from_claim_id, to_claim_id, edge_kind, note) VALUES (?, ?, ?, 'conflicts_with', '')",
                                (_new_id(), from_id, to_id),
                            )
                inserted_obligation_ids: list[str] = []
                for obligation in universe.formalization_targets[
                    : int(app_config.LIMA_MAX_OBLIGATIONS_PER_RUN)
                ]:
                    claim_id = None
                    if obligation.title in claim_ids_by_title:
                        claim_id = claim_ids_by_title[obligation.title]
                    kind = obligation.obligation_kind[:80]
                    canonical_hash = _canonical_hash(
                        [
                            problem_id,
                            kind,
                            obligation.title,
                            obligation.statement_md,
                            obligation.lean_goal,
                        ]
                    )
                    existing_obligation = conn.execute(
                        """
                        SELECT id FROM lima_obligation
                        WHERE problem_id = ? AND canonical_hash = ?
                        LIMIT 1
                        """,
                        (problem_id, canonical_hash),
                    ).fetchone()
                    if existing_obligation:
                        inserted_obligation_ids.append(str(existing_obligation["id"]))
                        continue
                    local_kinds = {"finite_check", "counterexample_search", "invariant_check", "consistency"}
                    normalized_status = obligation.status[:32]
                    if normalized_status == "queued":
                        normalized_status = "queued_local" if kind in local_kinds else "queued_formal_review"
                    if kind in {"lean_goal", "bridge_lemma", "equivalence"} and normalized_status == "queued_local":
                        normalized_status = "queued_formal_review"
                    why_exists = (
                        obligation.why_exists_md
                        or f"Compiled from Lima universe '{universe.title}' after rupture verdict {verdict}."
                    )
                    prove_or_kill = (
                        obligation.prove_or_kill_md
                        or "Verify this if it preserves the survivor; refute it to fracture the universe early."
                    )
                    success_would_show = (
                        f"Success would support Lima universe '{universe.title}' as a narrow formal-review candidate."
                    )
                    failure_would_kill = prove_or_kill
                    lineage = {
                        "source_problem_id": problem_id,
                        "source_run_id": run_id,
                        "source_universe_id": universe_id,
                        "source_universe_title": universe.title,
                        "source_family_id": family_id,
                        "source_family_key": universe.family_key,
                        "source_claim_id": claim_id,
                        "claim_ids": [claim_id] if claim_id else [],
                        "rupture_summary": str(rupture.get("summary_md") or ""),
                        "policy_snapshot": policy_snapshot,
                        "zero_live_authority": True,
                    }
                    obligation_id = _new_id()
                    conn.execute(
                        """
                        INSERT INTO lima_obligation (
                            id, problem_id, universe_id, claim_id, obligation_kind,
                            title, statement_md, lean_goal, status, priority,
                            why_exists_md, prove_or_kill_md,
                            why_this_exists, what_success_would_show, what_failure_would_kill,
                            source_run_id, source_universe_id, source_claim_id, source_claim_ids_json,
                            lineage_json, canonical_hash,
                            review_status, formal_backend, formal_payload_json,
                            formal_submission_ref_json, review_note, reviewed_at,
                            estimated_formalization_value, estimated_execution_cost,
                            estimated_value, estimated_cost,
                            aristotle_ref_json, result_summary_md, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            obligation_id,
                            problem_id,
                            universe_id,
                            claim_id,
                            kind,
                            obligation.title[:500],
                            obligation.statement_md[:8000],
                            obligation.lean_goal[:4000],
                            normalized_status,
                            int(obligation.priority),
                            why_exists[:4000],
                            prove_or_kill[:4000],
                            why_exists[:4000],
                            success_would_show[:4000],
                            failure_would_kill[:4000],
                            run_id,
                            universe_id,
                            claim_id or "",
                            _json([claim_id] if claim_id else []),
                            _json(lineage),
                            canonical_hash,
                            str(obligation.review_status or "not_reviewed")[:32],
                            str(obligation.formal_backend or "")[:80],
                            _json({}),
                            _json({}),
                            "",
                            None,
                            float(obligation.estimated_formalization_value or 0),
                            float(obligation.estimated_execution_cost or 0),
                            float(obligation.estimated_formalization_value or 0),
                            float(obligation.estimated_execution_cost or 0),
                            _json({}),
                            "",
                            now,
                            now,
                        ),
                    )
                    inserted_obligation_ids.append(obligation_id)
                if rupture:
                    rupture_id = _new_id()
                    conn.execute(
                        """
                        INSERT INTO lima_rupture_run (
                            id, run_id, universe_id, attack_suite_json, summary_md,
                            verdict, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            rupture_id,
                            run_id,
                            universe_id,
                            _json(rupture.get("attacks") or []),
                            str(rupture.get("summary_md") or "")[:8000],
                            verdict[:32],
                            now,
                        ),
                    )
                    for fracture in rupture.get("fractures") or []:
                        if not isinstance(fracture, dict):
                            continue
                        conn.execute(
                            """
                            INSERT INTO lima_fracture (
                                id, problem_id, family_id, universe_id, rupture_run_id,
                                failure_type, breakpoint_md, smallest_counterexample_json,
                                boundary_region_json, reusable_negative_theorem_md,
                                surviving_fragment_md, confidence, created_at
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                _new_id(),
                                problem_id,
                                family_id,
                                universe_id,
                                rupture_id,
                                str(fracture.get("failure_type") or "weak_explanation")[:80],
                                str(fracture.get("breakpoint_md") or "")[:4000],
                                _json(fracture.get("smallest_counterexample") or {}),
                                _json(fracture.get("boundary_region") or {}),
                                str(fracture.get("reusable_negative_theorem_md") or "")[:4000],
                                str(fracture.get("surviving_fragment_md") or "")[:4000],
                                float(fracture.get("confidence") or 0),
                                now,
                            ),
                        )
                if status in {"promising", "weakened"} and universe.formalization_targets:
                    payload = {
                        "source": "lima",
                        "universe_id": universe_id,
                        "family_id": family_id,
                        "claim_ids": list(claim_ids_by_title.values()),
                        "obligation_ids": inserted_obligation_ids,
                        "title": universe.title,
                        "destination_kind": "formal_review_queue",
                        "fracture_summary": str(rupture.get("summary_md") or ""),
                        "top_fractures": rupture.get("fractures") or [],
                        "key_obligations": [
                            target.model_dump(mode="json")
                            for target in universe.formalization_targets[:3]
                        ],
                        "linked_literature": [],
                        "policy_snapshot": policy_snapshot,
                        "zero_live_authority": True,
                    }
                    conn.execute(
                        """
                        INSERT INTO lima_handoff_request (
                            id, problem_id, universe_id, destination_kind, status,
                            payload_json, created_at, reviewed_at
                        )
                        VALUES (?, ?, ?, 'formal_review_queue', 'pending', ?, ?, NULL)
                        """,
                        (_new_id(), problem_id, universe_id, _json(payload), now),
                    )
            for ref in reference_points or []:
                if not isinstance(ref, dict):
                    continue
                conn.execute(
                    """
                    INSERT INTO lima_reference (
                        id, problem_id, universe_id, reference_kind, external_source,
                        external_id, snapshot_json, note, created_at
                    )
                    VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _new_id(),
                        problem_id,
                        str(ref.get("reference_kind") or "manual_note")[:80],
                        str(ref.get("external_source") or "")[:120],
                        str(ref.get("external_id") or "")[:200],
                        _json(ref.get("snapshot") or {}),
                        str(ref.get("note") or "")[:2000],
                        now,
                    ),
                )
            for artifact in artifacts or []:
                if not isinstance(artifact, dict):
                    continue
                content = artifact.get("content") or {}
                content_raw = _json(content)
                conn.execute(
                    """
                    INSERT INTO lima_artifact (
                        id, problem_id, universe_id, artifact_kind, content_json,
                        content_hash, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _new_id(),
                        problem_id,
                        universe_id_by_title.get(str(artifact.get("universe_title") or "")),
                        str(artifact.get("artifact_kind") or "benchmark")[:80],
                        content_raw,
                        hashlib.sha256(content_raw.encode("utf-8")).hexdigest(),
                        now,
                    ),
                )
            old_state = conn.execute(
                "SELECT revision FROM lima_state WHERE problem_id = ?",
                (problem_id,),
            ).fetchone()
            revision = int(old_state["revision"] or 0) + 1 if old_state else 1
            conn.execute(
                """
                INSERT INTO lima_state (
                    problem_id, revision, frontier_summary_md, frontier_json,
                    pressure_map_json, worldview_json, policy_json,
                    generation_priors_json, rupture_policy_json,
                    literature_policy_json, formal_policy_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, '{}', '{}', '{}', '{}', ?)
                ON CONFLICT(problem_id) DO UPDATE SET
                    revision = excluded.revision,
                    frontier_summary_md = excluded.frontier_summary_md,
                    frontier_json = excluded.frontier_json,
                    pressure_map_json = excluded.pressure_map_json,
                    worldview_json = excluded.worldview_json,
                    policy_json = excluded.policy_json,
                    updated_at = excluded.updated_at
                """,
                (
                    problem_id,
                    revision,
                    str(frontier_snapshot.get("summary") or "")[:8000],
                    _json(frontier_snapshot),
                    _json(pressure_snapshot),
                    _json(
                        {
                            "latest_run_id": run_id,
                            "latest_universes": [
                                {"title": u.title, "family_key": u.family_key}
                                for u in universes
                            ],
                        }
                    ),
                    _json(policy_snapshot),
                    now,
                ),
            )
            conn.commit()
            return run_id
        finally:
            conn.close()

    def insert_literature_source(
        self,
        *,
        problem_id: str,
        source_type: str,
        title: str,
        authors: list[str] | None = None,
        year: int | None = None,
        venue: str = "",
        doi: str = "",
        arxiv_id: str = "",
        url: str = "",
        abstract_md: str = "",
        bibtex: dict[str, Any] | None = None,
        extracts: list[dict[str, Any]] | None = None,
    ) -> str:
        now = _now()
        source_id = _new_id()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO lima_literature_source (
                    id, problem_id, source_type, title, authors_json, year, venue,
                    doi, arxiv_id, url, abstract_md, bibtex_json, fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    problem_id,
                    source_type[:80],
                    title[:500],
                    _json(authors or []),
                    year,
                    venue[:300],
                    doi[:200],
                    arxiv_id[:200],
                    url[:1000],
                    abstract_md[:8000],
                    _json(bibtex or {}),
                    now,
                ),
            )
            for extract in extracts or []:
                conn.execute(
                    """
                    INSERT INTO lima_literature_extract (
                        id, source_id, extract_kind, title, body_md, formal_hint,
                        tags_json, relevance_score, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _new_id(),
                        source_id,
                        str(extract.get("extract_kind") or "method")[:80],
                        str(extract.get("title") or title)[:500],
                        str(extract.get("body_md") or "")[:8000],
                        str(extract.get("formal_hint") or "")[:4000],
                        _json(extract.get("tags") or []),
                        float(extract.get("relevance_score") or 0),
                        now,
                    ),
                )
            conn.commit()
            return source_id
        finally:
            conn.close()

    def link_universe_literature(
        self, *, universe_id: str, source_id: str, relation_kind: str, note: str = ""
    ) -> str:
        link_id = _new_id()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO lima_universe_literature_link (
                    id, universe_id, source_id, relation_kind, note, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (link_id, universe_id, source_id, relation_kind[:80], note[:2000], _now()),
            )
            conn.commit()
            return link_id
        finally:
            conn.close()

    def list_literature_sources(self, problem_id: str, limit: int = 20) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT * FROM lima_literature_source
                WHERE problem_id = ?
                ORDER BY fetched_at DESC
                LIMIT ?
                """,
                (problem_id, min(limit, 200)),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def list_literature_extracts(self, source_ids: list[str]) -> list[dict[str, Any]]:
        if not source_ids:
            return []
        placeholders = ",".join("?" * len(source_ids))
        conn = self._connect()
        try:
            cur = conn.execute(
                f"""
                SELECT * FROM lima_literature_extract
                WHERE source_id IN ({placeholders})
                ORDER BY relevance_score DESC, created_at DESC
                """,
                source_ids,
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def list_universe_literature_links(self, problem_id: str, limit: int = 50) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT l.*, u.title AS universe_title, s.title AS source_title, s.source_type
                FROM lima_universe_literature_link l
                JOIN lima_universe u ON u.id = l.universe_id
                JOIN lima_literature_source s ON s.id = l.source_id
                WHERE u.problem_id = ?
                ORDER BY l.created_at DESC
                LIMIT ?
                """,
                (problem_id, min(limit, 200)),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def list_artifacts(self, problem_id: str, limit: int = 50) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT a.*, u.title AS universe_title
                FROM lima_artifact a
                LEFT JOIN lima_universe u ON u.id = a.universe_id
                WHERE a.problem_id = ?
                ORDER BY a.created_at DESC
                LIMIT ?
                """,
                (problem_id, min(limit, 200)),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def list_universes(self, problem_id: str, limit: int = 30) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT u.*, f.family_key, f.family_kind
                FROM lima_universe u
                LEFT JOIN lima_universe_family f ON f.id = u.family_id
                WHERE u.problem_id = ?
                ORDER BY
                    CASE u.universe_status
                      WHEN 'formalized' THEN 7
                      WHEN 'handed_off' THEN 6
                      WHEN 'promising' THEN 5
                      WHEN 'weakened' THEN 3
                      WHEN 'proposed' THEN 2
                      WHEN 'dead' THEN 1
                      ELSE 0
                    END DESC,
                    u.created_at DESC
                LIMIT ?
                """,
                (problem_id, min(limit, 200)),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def list_universes_for_run(self, run_id: str) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT u.*, f.family_key, f.family_kind
                FROM lima_universe u
                LEFT JOIN lima_universe_family f ON f.id = u.family_id
                WHERE u.run_id = ?
                ORDER BY u.created_at ASC
                """,
                (run_id,),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def get_universe(self, universe_id: str) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT u.*, fam.family_key, fam.family_kind
                FROM lima_universe u
                LEFT JOIN lima_universe_family fam ON fam.id = u.family_id
                WHERE u.id = ?
                """,
                (universe_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_family_leaderboard(self, problem_id: str, limit: int = 20) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT *
                FROM lima_universe_family
                WHERE problem_id = ?
                ORDER BY survival_count DESC, formal_win_count DESC, failure_count ASC, last_seen_at DESC
                LIMIT ?
                """,
                (problem_id, min(limit, 100)),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def list_fractures(self, problem_id: str, limit: int = 20) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT f.*, u.title AS universe_title, fam.family_key
                FROM lima_fracture f
                LEFT JOIN lima_universe u ON u.id = f.universe_id
                LEFT JOIN lima_universe_family fam ON fam.id = f.family_id
                WHERE f.problem_id = ?
                ORDER BY f.confidence DESC, f.created_at DESC
                LIMIT ?
                """,
                (problem_id, min(limit, 200)),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def list_fractures_for_universe(
        self, universe_id: str, *, limit: int = 20
    ) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT f.*, u.title AS universe_title, fam.family_key
                FROM lima_fracture f
                LEFT JOIN lima_universe u ON u.id = f.universe_id
                LEFT JOIN lima_universe_family fam ON fam.id = f.family_id
                WHERE f.universe_id = ?
                ORDER BY f.confidence DESC, f.created_at DESC
                LIMIT ?
                """,
                (universe_id, min(limit, 100)),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def list_obligations(
        self, problem_id: str, *, status: str | None = None, limit: int = 30
    ) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            if status:
                cur = conn.execute(
                    """
                    SELECT o.*, u.title AS universe_title
                    FROM lima_obligation o
                    LEFT JOIN lima_universe u ON u.id = o.universe_id
                    WHERE o.problem_id = ? AND o.status = ?
                    ORDER BY o.priority DESC, o.created_at DESC
                    LIMIT ?
                    """,
                    (problem_id, status, min(limit, 200)),
                )
            else:
                cur = conn.execute(
                    """
                    SELECT o.*, u.title AS universe_title
                    FROM lima_obligation o
                    LEFT JOIN lima_universe u ON u.id = o.universe_id
                    WHERE o.problem_id = ?
                    ORDER BY o.priority DESC, o.created_at DESC
                    LIMIT ?
                    """,
                    (problem_id, min(limit, 200)),
                )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def list_obligations_by_statuses(
        self, problem_id: str, statuses: list[str], *, limit: int = 30
    ) -> list[dict[str, Any]]:
        if not statuses:
            return []
        placeholders = ",".join("?" * len(statuses))
        conn = self._connect()
        try:
            cur = conn.execute(
                f"""
                SELECT o.*, u.title AS universe_title
                FROM lima_obligation o
                LEFT JOIN lima_universe u ON u.id = o.universe_id
                WHERE o.problem_id = ? AND o.status IN ({placeholders})
                ORDER BY o.priority DESC, o.created_at DESC
                LIMIT ?
                """,
                [problem_id, *statuses, min(limit, 200)],
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def list_obligations_for_universe(
        self, universe_id: str, *, limit: int = 50
    ) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT o.*, u.title AS universe_title
                FROM lima_obligation o
                LEFT JOIN lima_universe u ON u.id = o.universe_id
                WHERE o.universe_id = ?
                ORDER BY o.priority DESC, o.created_at DESC
                LIMIT ?
                """,
                (universe_id, min(limit, 200)),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def get_obligation(self, obligation_id: str) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT o.*, u.title AS universe_title, u.family_id
                FROM lima_obligation o
                LEFT JOIN lima_universe u ON u.id = o.universe_id
                WHERE o.id = ?
                """,
                (obligation_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def update_obligation_result(
        self,
        obligation_id: str,
        *,
        status: str,
        result_summary_md: str,
        aristotle_ref: dict[str, Any] | None = None,
    ) -> tuple[bool, str]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                UPDATE lima_obligation
                SET status = ?, result_summary_md = ?, aristotle_ref_json = ?,
                    formal_submission_ref_json = CASE
                      WHEN ? IN ('submitted_formal', 'verified_formal', 'refuted_formal', 'inconclusive') THEN ?
                      ELSE formal_submission_ref_json
                    END,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    status[:32],
                    result_summary_md[:8000],
                    _json(aristotle_ref or {}),
                    status[:32],
                    _json(aristotle_ref or {}),
                    _now(),
                    obligation_id,
                ),
            )
            conn.commit()
            if cur.rowcount <= 0:
                return False, "unknown obligation"
            return True, f"obligation {status}"
        finally:
            conn.close()

    def set_obligation_status(
        self,
        obligation_id: str,
        status: str,
        *,
        review_status: str | None = None,
        formal_backend: str | None = None,
        formal_payload: dict[str, Any] | None = None,
        formal_submission_ref: dict[str, Any] | None = None,
        result_summary_md: str | None = None,
        review_note: str | None = None,
    ) -> tuple[bool, str]:
        assignments = ["status = ?", "updated_at = ?"]
        values: list[Any] = [status[:32], _now()]
        if review_status is not None:
            assignments.append("review_status = ?")
            values.append(review_status[:32])
        if formal_backend is not None:
            assignments.append("formal_backend = ?")
            values.append(formal_backend[:80])
        if formal_payload is not None:
            assignments.append("formal_payload_json = ?")
            values.append(_json(formal_payload))
        if formal_submission_ref is not None:
            assignments.append("formal_submission_ref_json = ?")
            values.append(_json(formal_submission_ref))
        if result_summary_md is not None:
            assignments.append("result_summary_md = ?")
            values.append(result_summary_md[:8000])
        if review_note is not None:
            assignments.append("review_note = ?")
            values.append(review_note[:4000])
        if review_status in {"approved", "rejected", "archived"} or status in {
            "approved_for_formal",
            "submitted_formal",
            "verified_formal",
            "refuted_formal",
            "inconclusive",
            "archived",
        }:
            assignments.append("reviewed_at = COALESCE(reviewed_at, ?)")
            values.append(_now())
        values.append(obligation_id)
        conn = self._connect()
        try:
            cur = conn.execute(
                f"UPDATE lima_obligation SET {', '.join(assignments)} WHERE id = ?",
                values,
            )
            conn.commit()
            if cur.rowcount <= 0:
                return False, "unknown obligation"
            return True, f"obligation {status}"
        finally:
            conn.close()

    def create_formal_review_item(
        self,
        *,
        problem_id: str,
        obligation_id: str,
        universe_id: str | None,
        claim_id: str | None,
        backend_kind: str,
        packet: dict[str, Any],
        family_id: str | None = None,
        claim_ids: list[str] | None = None,
        rupture_summary_md: str = "",
        literature_links: list[dict[str, Any]] | None = None,
        policy_revision_id: str = "",
        lineage: dict[str, Any] | None = None,
    ) -> str:
        now = _now()
        existing = None
        conn = self._connect()
        try:
            existing = conn.execute(
                """
                SELECT id FROM lima_formal_review_queue
                WHERE obligation_id = ? AND status IN ('queued_formal_review', 'approved_for_formal', 'submitted_formal')
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (obligation_id,),
            ).fetchone()
            if existing:
                return str(existing["id"])
            item_id = _new_id()
            conn.execute(
                """
                INSERT INTO lima_formal_review_queue (
                    id, problem_id, obligation_id, universe_id, claim_id,
                    family_id, claim_ids_json, rupture_summary_md,
                    literature_links_json, policy_revision_id, lineage_json,
                    backend_kind, status, review_decision, packet_json,
                    backend_result_json, created_at, reviewed_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued_formal_review', 'pending', ?, '{}', ?, NULL, ?)
                """,
                (
                    item_id,
                    problem_id,
                    obligation_id,
                    universe_id or None,
                    claim_id or None,
                    family_id or None,
                    _json(claim_ids or ([claim_id] if claim_id else [])),
                    rupture_summary_md[:8000],
                    _json(literature_links or []),
                    policy_revision_id[:120],
                    _json(lineage or {}),
                    backend_kind[:80],
                    _json(packet),
                    now,
                    now,
                ),
            )
            conn.commit()
            return item_id
        finally:
            conn.close()

    def update_formal_review_item(
        self,
        review_id: str,
        *,
        status: str,
        review_decision: str | None = None,
        backend_result: dict[str, Any] | None = None,
    ) -> tuple[bool, str]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                UPDATE lima_formal_review_queue
                SET status = ?,
                    review_decision = COALESCE(?, review_decision),
                    backend_result_json = COALESCE(?, backend_result_json),
                    reviewed_at = CASE WHEN ? IS NOT NULL THEN ? ELSE reviewed_at END,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    status[:32],
                    review_decision[:32] if review_decision else None,
                    _json(backend_result) if backend_result is not None else None,
                    review_decision,
                    _now(),
                    _now(),
                    review_id,
                ),
            )
            conn.commit()
            if cur.rowcount <= 0:
                return False, "unknown formal review item"
            return True, f"formal review {status}"
        finally:
            conn.close()

    def list_formal_reviews(self, problem_id: str, limit: int = 30) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT r.*, o.title AS obligation_title, o.obligation_kind
                FROM lima_formal_review_queue r
                LEFT JOIN lima_obligation o ON o.id = r.obligation_id
                WHERE r.problem_id = ?
                ORDER BY r.created_at DESC
                LIMIT ?
                """,
                (problem_id, min(limit, 200)),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def create_artifact(
        self,
        *,
        problem_id: str,
        universe_id: str | None,
        artifact_kind: str,
        content: dict[str, Any],
    ) -> str:
        artifact_id = _new_id()
        content_raw = _json(content)
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO lima_artifact (
                    id, problem_id, universe_id, artifact_kind, content_json,
                    content_hash, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_id,
                    problem_id,
                    universe_id or None,
                    artifact_kind[:80],
                    content_raw,
                    hashlib.sha256(content_raw.encode("utf-8")).hexdigest(),
                    _now(),
                ),
            )
            conn.commit()
            return artifact_id
        finally:
            conn.close()

    def list_handoffs(
        self, problem_id: str, *, status: str | None = None, limit: int = 30
    ) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            if status:
                cur = conn.execute(
                    """
                    SELECT h.*, u.title AS universe_title
                    FROM lima_handoff_request h
                    LEFT JOIN lima_universe u ON u.id = h.universe_id
                    WHERE h.problem_id = ? AND h.status = ?
                    ORDER BY h.created_at DESC
                    LIMIT ?
                    """,
                    (problem_id, status, min(limit, 200)),
                )
            else:
                cur = conn.execute(
                    """
                    SELECT h.*, u.title AS universe_title
                    FROM lima_handoff_request h
                    LEFT JOIN lima_universe u ON u.id = h.universe_id
                    WHERE h.problem_id = ?
                    ORDER BY h.created_at DESC
                    LIMIT ?
                    """,
                    (problem_id, min(limit, 200)),
                )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def get_handoff(self, handoff_id: str) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM lima_handoff_request WHERE id = ?",
                (handoff_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def set_handoff_status(self, handoff_id: str, status: str) -> tuple[bool, str]:
        row = self.get_handoff(handoff_id)
        if not row:
            return False, "unknown handoff"
        if str(row.get("status")) not in {"pending", "held"}:
            return False, "not reviewable"
        conn = self._connect()
        try:
            conn.execute(
                """
                UPDATE lima_handoff_request
                SET status = ?, reviewed_at = ?
                WHERE id = ?
                """,
                (status[:32], _now(), handoff_id),
            )
            conn.commit()
            return True, f"handoff {status}"
        finally:
            conn.close()

    def create_policy_revision(
        self,
        *,
        problem_id: str,
        generation_policy: dict[str, Any],
        rupture_policy: dict[str, Any],
        literature_policy: dict[str, Any],
        formal_policy: dict[str, Any],
        scoring_weights: dict[str, Any],
        change_reason_md: str,
    ) -> str:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT MAX(revision_no) FROM lima_policy_revision WHERE problem_id = ?",
                (problem_id,),
            ).fetchone()
            revision_no = int(row[0] or 0) + 1 if row else 1
            revision_id = _new_id()
            conn.execute(
                """
                INSERT INTO lima_policy_revision (
                    id, problem_id, revision_no, generation_policy_json,
                    rupture_policy_json, literature_policy_json, formal_policy_json,
                    scoring_weights_json, change_reason_md, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    revision_id,
                    problem_id,
                    revision_no,
                    _json(generation_policy),
                    _json(rupture_policy),
                    _json(literature_policy),
                    _json(formal_policy),
                    _json(scoring_weights),
                    change_reason_md[:4000],
                    _now(),
                ),
            )
            conn.commit()
            return revision_id
        finally:
            conn.close()

    def list_policy_revisions(self, problem_id: str, limit: int = 10) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT * FROM lima_policy_revision
                WHERE problem_id = ?
                ORDER BY revision_no DESC
                LIMIT ?
                """,
                (problem_id, min(limit, 100)),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def create_meta_run(
        self,
        *,
        problem_id: str,
        from_run_id: str | None,
        analysis_summary_md: str,
        policy_changes: dict[str, Any],
        benchmark: dict[str, Any],
    ) -> str:
        meta_id = _new_id()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO lima_meta_run (
                    id, problem_id, from_run_id, analysis_summary_md,
                    policy_changes_json, benchmark_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    meta_id,
                    problem_id,
                    from_run_id,
                    analysis_summary_md[:8000],
                    _json(policy_changes),
                    _json(benchmark),
                    _now(),
                ),
            )
            conn.commit()
            return meta_id
        finally:
            conn.close()

    def get_dashboard_snapshot(self, problem_id_or_slug: str | None = None) -> dict[str, Any]:
        problem = self.get_problem(problem_id_or_slug)
        problem_id = str(problem["id"])
        sources = self.list_literature_sources(problem_id, limit=10)
        extracts = self.list_literature_extracts([str(s["id"]) for s in sources])
        return {
            "problem": problem,
            "problems": self.list_problems(),
            "state": self.get_state(problem_id),
            "latest_run": self.get_latest_run(problem_id),
            "runs": self.list_runs(problem_id, limit=12),
            "families": self.list_family_leaderboard(problem_id, limit=12),
            "universes": self.list_universes(problem_id, limit=20),
            "fractures": self.list_fractures(problem_id, limit=12),
            "obligations": self.list_obligations(problem_id, limit=20),
            "handoffs": self.list_handoffs(problem_id, limit=20),
            "literature_sources": sources,
            "literature_extracts": extracts[:20],
            "literature_links": self.list_universe_literature_links(problem_id, limit=30),
            "formal_reviews": self.list_formal_reviews(problem_id, limit=20),
            "artifacts": self.list_artifacts(problem_id, limit=30),
            "policy_revisions": self.list_policy_revisions(problem_id, limit=8),
        }


def parse_row_json(row: dict[str, Any], key: str, default: Any) -> Any:
    return safe_json_loads(row.get(key), default)
