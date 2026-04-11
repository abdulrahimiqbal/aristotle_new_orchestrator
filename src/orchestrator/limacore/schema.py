from __future__ import annotations


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS problems (
    id TEXT PRIMARY KEY,
    slug TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    statement_md TEXT NOT NULL,
    domain TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active',
    target_theorem TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    problem_id TEXT NOT NULL,
    parent_event_id TEXT,
    event_type TEXT NOT NULL,
    decision TEXT NOT NULL,
    score_delta_json TEXT NOT NULL DEFAULT '{}',
    artifact_refs_json TEXT NOT NULL DEFAULT '[]',
    summary_md TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_limacore_events_problem_created ON events(problem_id, created_at);

CREATE TABLE IF NOT EXISTS frontier_nodes (
    id TEXT PRIMARY KEY,
    problem_id TEXT NOT NULL,
    node_key TEXT NOT NULL,
    node_kind TEXT NOT NULL,
    title TEXT NOT NULL,
    statement_md TEXT NOT NULL DEFAULT '',
    formal_statement TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'open',
    dependency_keys_json TEXT NOT NULL DEFAULT '[]',
    blocker_kind TEXT NOT NULL DEFAULT '',
    blocker_note_md TEXT NOT NULL DEFAULT '',
    best_world_id TEXT,
    replay_ref_json TEXT NOT NULL DEFAULT '{}',
    priority REAL NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_limacore_frontier_problem_key ON frontier_nodes(problem_id, node_key);

CREATE TABLE IF NOT EXISTS world_heads (
    id TEXT PRIMARY KEY,
    problem_id TEXT NOT NULL,
    family_key TEXT NOT NULL,
    world_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'proposed',
    bridge_status TEXT NOT NULL DEFAULT 'unknown',
    kill_status TEXT NOT NULL DEFAULT 'unknown',
    theorem_status TEXT NOT NULL DEFAULT 'unknown',
    yield_score REAL NOT NULL DEFAULT 0,
    last_event_id TEXT,
    latest_artifact_ref_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_limacore_world_problem_family ON world_heads(problem_id, family_key);

CREATE TABLE IF NOT EXISTS fracture_heads (
    id TEXT PRIMARY KEY,
    problem_id TEXT NOT NULL,
    family_key TEXT NOT NULL,
    failure_type TEXT NOT NULL,
    smallest_counterexample_ref_json TEXT NOT NULL DEFAULT '{}',
    blocker_note_md TEXT NOT NULL DEFAULT '',
    required_delta_md TEXT NOT NULL DEFAULT '',
    ban_level TEXT NOT NULL DEFAULT 'none',
    repeat_count INTEGER NOT NULL DEFAULT 0,
    last_event_id TEXT,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_limacore_fracture_problem_family_type
ON fracture_heads(problem_id, family_key, failure_type);

CREATE TABLE IF NOT EXISTS cohorts (
    id TEXT PRIMARY KEY,
    problem_id TEXT NOT NULL,
    world_id TEXT,
    cohort_kind TEXT NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    total_jobs INTEGER NOT NULL DEFAULT 0,
    queued_jobs INTEGER NOT NULL DEFAULT 0,
    running_jobs INTEGER NOT NULL DEFAULT 0,
    succeeded_jobs INTEGER NOT NULL DEFAULT 0,
    failed_jobs INTEGER NOT NULL DEFAULT 0,
    yielded_lemmas INTEGER NOT NULL DEFAULT 0,
    yielded_counterexamples INTEGER NOT NULL DEFAULT 0,
    yielded_blockers INTEGER NOT NULL DEFAULT 0,
    last_event_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS aristotle_jobs (
    id TEXT PRIMARY KEY,
    problem_id TEXT NOT NULL,
    cohort_id TEXT,
    frontier_node_key TEXT NOT NULL DEFAULT '',
    job_kind TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    input_artifact_ref_json TEXT NOT NULL DEFAULT '{}',
    output_artifact_ref_json TEXT NOT NULL DEFAULT '{}',
    result_summary_md TEXT NOT NULL DEFAULT '',
    replayable INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_limacore_jobs_problem_status ON aristotle_jobs(problem_id, status);
CREATE INDEX IF NOT EXISTS idx_limacore_jobs_cohort ON aristotle_jobs(cohort_id);

CREATE TABLE IF NOT EXISTS artifacts (
    hash TEXT PRIMARY KEY,
    artifact_kind TEXT NOT NULL,
    mime_type TEXT NOT NULL DEFAULT 'application/json',
    content_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS program_state (
    problem_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""
