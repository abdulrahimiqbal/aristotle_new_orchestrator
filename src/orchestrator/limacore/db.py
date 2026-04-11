from __future__ import annotations

import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import uuid4

from .artifacts import artifact_hash, parse_json, stable_json, utc_now
from .models import FrontierNode, ProblemSpec, ProgramState
from .schema import SCHEMA_SQL


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    if column not in _column_names(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _new_id() -> str:
    return uuid4().hex


class LimaCoreDB:
    def __init__(self, path: str) -> None:
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA_SQL)
            self._run_migrations(conn)
            conn.commit()
        from .seed import ensure_seed_data

        ensure_seed_data(self)

    def _run_migrations(self, conn: sqlite3.Connection) -> None:
        problem_columns = [
            ("original_prompt", "TEXT NOT NULL DEFAULT ''"),
            ("normalized_statement_md", "TEXT NOT NULL DEFAULT ''"),
            ("runtime_status", "TEXT NOT NULL DEFAULT 'booting'"),
            ("status_reason_md", "TEXT NOT NULL DEFAULT ''"),
            ("blocked_node_key", "TEXT NOT NULL DEFAULT ''"),
            ("blocker_kind", "TEXT NOT NULL DEFAULT ''"),
            ("exhausted_family_key", "TEXT NOT NULL DEFAULT ''"),
            ("exhausted_family_since", "TEXT NOT NULL DEFAULT ''"),
            ("stalled_since", "TEXT NOT NULL DEFAULT ''"),
            ("last_gain_at", "TEXT NOT NULL DEFAULT ''"),
            ("since_timestamp", "TEXT NOT NULL DEFAULT ''"),
            ("autopilot_enabled", "INTEGER NOT NULL DEFAULT 1"),
        ]
        for column, definition in problem_columns:
            _ensure_column(conn, "problems", column, definition)

    def store_artifact(
        self,
        artifact_kind: str,
        content: Any,
        *,
        mime_type: str = "application/json",
    ) -> dict[str, Any]:
        now = utc_now()
        blob = stable_json(content)
        digest = artifact_hash(artifact_kind, mime_type, content)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO artifacts(hash, artifact_kind, mime_type, content_json, created_at)
                VALUES(?, ?, ?, ?, ?)
                """,
                (digest, artifact_kind, mime_type, blob, now),
            )
            conn.commit()
        return {"hash": digest, "artifact_kind": artifact_kind, "mime_type": mime_type}

    def get_artifact(self, ref: str | dict[str, Any] | None) -> dict[str, Any] | None:
        if not ref:
            return None
        digest = ref if isinstance(ref, str) else str(ref.get("hash") or "")
        if not digest:
            return None
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM artifacts WHERE hash = ?", (digest,)).fetchone()
        if row is None:
            return None
        return {
            "hash": row["hash"],
            "artifact_kind": row["artifact_kind"],
            "mime_type": row["mime_type"],
            "content": parse_json(str(row["content_json"]), default={}),
            "created_at": row["created_at"],
        }

    def append_event(
        self,
        problem_id: str,
        event_type: str,
        decision: str,
        *,
        parent_event_id: str | None = None,
        score_delta: dict[str, Any] | None = None,
        artifact_refs: list[dict[str, Any]] | None = None,
        summary_md: str = "",
    ) -> str:
        event_id = _new_id()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO events(
                    id, problem_id, parent_event_id, event_type, decision, score_delta_json,
                    artifact_refs_json, summary_md, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    problem_id,
                    parent_event_id,
                    event_type,
                    decision,
                    stable_json(score_delta or {}),
                    stable_json(artifact_refs or []),
                    summary_md,
                    utc_now(),
                ),
            )
            conn.commit()
        return event_id

    def create_problem(
        self,
        slug: str,
        title: str,
        statement_md: str,
        *,
        domain: str = "",
        target_theorem: str = "",
        original_prompt: str = "",
        normalized_statement_md: str = "",
        runtime_status: str = "booting",
        status_reason_md: str = "",
        autopilot_enabled: bool = True,
    ) -> tuple[str, bool]:
        now = utc_now()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT id FROM problems WHERE slug = ?",
                (slug,),
            ).fetchone()
            if existing is not None:
                return str(existing["id"]), False
            problem_id = _new_id()
            conn.execute(
                """
                INSERT INTO problems(
                    id, slug, title, statement_md, domain, status, target_theorem,
                    original_prompt, normalized_statement_md, runtime_status, status_reason_md,
                    blocked_node_key, blocker_kind, exhausted_family_key, exhausted_family_since,
                    stalled_since, last_gain_at, since_timestamp,
                    autopilot_enabled, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, '', '', '', '', '', ?, ?, ?, ?, ?)
                """,
                (
                    problem_id,
                    slug,
                    title,
                    statement_md,
                    domain,
                    target_theorem,
                    original_prompt,
                    normalized_statement_md or statement_md,
                    runtime_status,
                    status_reason_md,
                    now,
                    now,
                    1 if autopilot_enabled else 0,
                    now,
                    now,
                ),
            )
            conn.commit()
        self.ensure_program_state(problem_id)
        return problem_id, True

    def list_problems(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM problems ORDER BY updated_at DESC, title ASC").fetchall()
        return [dict(row) for row in rows]

    def get_problem(self, slug_or_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM problems WHERE id = ? OR slug = ?",
                (slug_or_id, slug_or_id),
            ).fetchone()
        return dict(row) if row else None

    def update_problem_status(self, slug_or_id: str, status: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE problems SET status = ?, updated_at = ? WHERE id = ? OR slug = ?",
                (status, utc_now(), slug_or_id, slug_or_id),
            )
            conn.commit()
        return self.get_problem(slug_or_id)

    def update_problem_runtime(
        self,
        slug_or_id: str,
        *,
        runtime_status: str,
        status_reason_md: str = "",
        blocked_node_key: str | None = None,
        blocker_kind: str | None = None,
        exhausted_family_key: str | None = None,
        exhausted_family_since: str | None = None,
        stalled_since: str | None = None,
        last_gain_at: str | None = None,
        autopilot_enabled: bool | None = None,
        since_timestamp: str | None = None,
    ) -> dict[str, Any] | None:
        current = self.get_problem(slug_or_id)
        if current is None:
            return None
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE problems SET
                    runtime_status = ?,
                    status_reason_md = ?,
                    blocked_node_key = ?,
                    blocker_kind = ?,
                    exhausted_family_key = ?,
                    exhausted_family_since = ?,
                    stalled_since = ?,
                    last_gain_at = ?,
                    autopilot_enabled = ?,
                    since_timestamp = ?,
                    updated_at = ?
                WHERE id = ? OR slug = ?
                """,
                (
                    runtime_status,
                    status_reason_md,
                    blocked_node_key if blocked_node_key is not None else str(current.get("blocked_node_key") or ""),
                    blocker_kind if blocker_kind is not None else str(current.get("blocker_kind") or ""),
                    exhausted_family_key if exhausted_family_key is not None else str(current.get("exhausted_family_key") or ""),
                    exhausted_family_since if exhausted_family_since is not None else str(current.get("exhausted_family_since") or ""),
                    stalled_since if stalled_since is not None else str(current.get("stalled_since") or ""),
                    last_gain_at if last_gain_at is not None else str(current.get("last_gain_at") or ""),
                    int(current.get("autopilot_enabled") if autopilot_enabled is None else (1 if autopilot_enabled else 0)),
                    since_timestamp if since_timestamp is not None else str(current.get("since_timestamp") or utc_now()),
                    utc_now(),
                    slug_or_id,
                    slug_or_id,
                ),
            )
            conn.commit()
        return self.get_problem(slug_or_id)

    def set_autopilot_enabled(self, slug_or_id: str, enabled: bool) -> dict[str, Any] | None:
        current = self.get_problem(slug_or_id)
        if current is None:
            return None
        next_status = "paused" if not enabled else ("running" if str(current.get("runtime_status")) != "solved" else "solved")
        return self.update_problem_runtime(
            slug_or_id,
            runtime_status=next_status,
            status_reason_md="Autopilot paused by operator." if not enabled else "Running: autopilot active.",
            autopilot_enabled=enabled,
            since_timestamp=utc_now(),
        )

    def upsert_frontier_node(self, node: FrontierNode) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO frontier_nodes(
                    id, problem_id, node_key, node_kind, title, statement_md, formal_statement, status,
                    dependency_keys_json, blocker_kind, blocker_note_md, best_world_id, replay_ref_json,
                    priority, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(problem_id, node_key) DO UPDATE SET
                    title = excluded.title,
                    statement_md = excluded.statement_md,
                    formal_statement = excluded.formal_statement,
                    status = excluded.status,
                    dependency_keys_json = excluded.dependency_keys_json,
                    blocker_kind = excluded.blocker_kind,
                    blocker_note_md = excluded.blocker_note_md,
                    best_world_id = excluded.best_world_id,
                    replay_ref_json = excluded.replay_ref_json,
                    priority = excluded.priority,
                    updated_at = excluded.updated_at
                """,
                (
                    node.id,
                    node.problem_id,
                    node.node_key,
                    node.node_kind,
                    node.title,
                    node.statement_md,
                    node.formal_statement,
                    node.status,
                    stable_json(node.dependency_keys),
                    node.blocker_kind,
                    node.blocker_note_md,
                    node.best_world_id,
                    stable_json(node.replay_ref),
                    node.priority,
                    node.updated_at or utc_now(),
                ),
            )
            conn.execute("UPDATE problems SET updated_at = ? WHERE id = ?", (utc_now(), node.problem_id))
            conn.commit()

    def get_frontier_nodes(self, problem_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM frontier_nodes WHERE problem_id = ? ORDER BY priority DESC, node_key ASC",
                (problem_id,),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            d["dependency_keys"] = parse_json(str(row["dependency_keys_json"]), default=[])
            d["replay_ref"] = parse_json(str(row["replay_ref_json"]), default={})
            out.append(d)
        return out

    def get_frontier_node(self, problem_id: str, node_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM frontier_nodes WHERE problem_id = ? AND node_key = ?",
                (problem_id, node_key),
            ).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["dependency_keys"] = parse_json(str(row["dependency_keys_json"]), default=[])
        d["replay_ref"] = parse_json(str(row["replay_ref_json"]), default={})
        return d

    def replace_world_head(self, problem_id: str, payload: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO world_heads(
                    id, problem_id, family_key, world_name, status, bridge_status, kill_status,
                    theorem_status, yield_score, last_event_id, latest_artifact_ref_json, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(problem_id, family_key) DO UPDATE SET
                    world_name = excluded.world_name,
                    status = excluded.status,
                    bridge_status = excluded.bridge_status,
                    kill_status = excluded.kill_status,
                    theorem_status = excluded.theorem_status,
                    yield_score = excluded.yield_score,
                    last_event_id = excluded.last_event_id,
                    latest_artifact_ref_json = excluded.latest_artifact_ref_json,
                    updated_at = excluded.updated_at
                """,
                (
                    payload.get("id") or _new_id(),
                    problem_id,
                    payload["family_key"],
                    payload["world_name"],
                    payload.get("status", "proposed"),
                    payload.get("bridge_status", "unknown"),
                    payload.get("kill_status", "unknown"),
                    payload.get("theorem_status", "unknown"),
                    float(payload.get("yield_score", 0.0)),
                    payload.get("last_event_id"),
                    stable_json(payload.get("latest_artifact_ref", {})),
                    payload.get("updated_at") or utc_now(),
                ),
            )
            conn.commit()

    def list_world_heads(self, problem_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM world_heads WHERE problem_id = ? ORDER BY yield_score DESC, updated_at DESC",
                (problem_id,),
            ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            d["latest_artifact_ref"] = parse_json(str(row["latest_artifact_ref_json"]), default={})
            out.append(d)
        return out

    def replace_fracture_head(self, problem_id: str, payload: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO fracture_heads(
                    id, problem_id, family_key, failure_type, smallest_counterexample_ref_json,
                    blocker_note_md, required_delta_md, ban_level, repeat_count, last_event_id, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(problem_id, family_key, failure_type) DO UPDATE SET
                    smallest_counterexample_ref_json = excluded.smallest_counterexample_ref_json,
                    blocker_note_md = excluded.blocker_note_md,
                    required_delta_md = excluded.required_delta_md,
                    ban_level = excluded.ban_level,
                    repeat_count = excluded.repeat_count,
                    last_event_id = excluded.last_event_id,
                    updated_at = excluded.updated_at
                """,
                (
                    payload.get("id") or _new_id(),
                    problem_id,
                    payload["family_key"],
                    payload["failure_type"],
                    stable_json(payload.get("smallest_counterexample_ref", {})),
                    payload.get("blocker_note_md", ""),
                    payload.get("required_delta_md", ""),
                    payload.get("ban_level", "none"),
                    int(payload.get("repeat_count", 0)),
                    payload.get("last_event_id"),
                    payload.get("updated_at") or utc_now(),
                ),
            )
            conn.commit()

    def list_fracture_heads(self, problem_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM fracture_heads
                WHERE problem_id = ?
                ORDER BY repeat_count DESC, updated_at DESC
                """,
                (problem_id,),
            ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            d["smallest_counterexample_ref"] = parse_json(
                str(row["smallest_counterexample_ref_json"]),
                default={},
            )
            out.append(d)
        return out

    def create_cohort(
        self,
        problem_id: str,
        *,
        world_id: str | None,
        cohort_kind: str,
        title: str,
        total_jobs: int,
        last_event_id: str | None = None,
    ) -> str:
        cohort_id = _new_id()
        now = utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO cohorts(
                    id, problem_id, world_id, cohort_kind, title, status, total_jobs,
                    queued_jobs, running_jobs, succeeded_jobs, failed_jobs, yielded_lemmas,
                    yielded_counterexamples, yielded_blockers, last_event_id, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, 'queued', ?, ?, 0, 0, 0, 0, 0, 0, ?, ?, ?)
                """,
                (cohort_id, problem_id, world_id, cohort_kind, title, total_jobs, total_jobs, last_event_id, now, now),
            )
            conn.commit()
        return cohort_id

    def create_job(
        self,
        problem_id: str,
        *,
        cohort_id: str | None,
        frontier_node_key: str,
        job_kind: str,
        input_artifact_ref: dict[str, Any],
    ) -> str:
        job_id = _new_id()
        now = utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO aristotle_jobs(
                    id, problem_id, cohort_id, frontier_node_key, job_kind, status,
                    input_artifact_ref_json, output_artifact_ref_json, result_summary_md, replayable,
                    created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, 'queued', ?, '{}', '', 0, ?, ?)
                """,
                (job_id, problem_id, cohort_id, frontier_node_key, job_kind, stable_json(input_artifact_ref), now, now),
            )
            conn.commit()
        return job_id

    def set_job_status(
        self,
        job_id: str,
        *,
        status: str,
        output_artifact_ref: dict[str, Any] | None = None,
        result_summary_md: str = "",
        replayable: bool = False,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE aristotle_jobs
                SET status = ?, output_artifact_ref_json = ?, result_summary_md = ?, replayable = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, stable_json(output_artifact_ref or {}), result_summary_md, 1 if replayable else 0, utc_now(), job_id),
            )
            conn.commit()

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM aristotle_jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["input_artifact_ref"] = parse_json(str(row["input_artifact_ref_json"]), default={})
        d["output_artifact_ref"] = parse_json(str(row["output_artifact_ref_json"]), default={})
        return d

    def list_jobs(self, problem_id: str, *, cohort_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM aristotle_jobs WHERE problem_id = ?"
        params: list[Any] = [problem_id]
        if cohort_id is not None:
            query += " AND cohort_id = ?"
            params.append(cohort_id)
        query += " ORDER BY created_at DESC"
        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            d["input_artifact_ref"] = parse_json(str(row["input_artifact_ref_json"]), default={})
            d["output_artifact_ref"] = parse_json(str(row["output_artifact_ref_json"]), default={})
            out.append(d)
        return out

    def update_cohort_metrics(self, cohort_id: str) -> None:
        with self._connect() as conn:
            jobs = conn.execute(
                "SELECT status, job_kind, result_summary_md FROM aristotle_jobs WHERE cohort_id = ?",
                (cohort_id,),
            ).fetchall()
            total = len(jobs)
            queued = sum(1 for row in jobs if row["status"] == "queued")
            running = sum(1 for row in jobs if row["status"] == "running")
            succeeded = sum(1 for row in jobs if row["status"] == "succeeded")
            failed = sum(1 for row in jobs if row["status"] == "failed")
            lemmas = sum(
                1
                for row in jobs
                if row["status"] == "succeeded" and str(row["job_kind"]) in {"bridge_lemma", "local_law", "equivalence_probe"}
            )
            counterexamples = sum(
                1 for row in jobs if "counterexample" in str(row["result_summary_md"]).lower()
            )
            blockers = sum(
                1
                for row in jobs
                if row["status"] == "failed" or "blocked" in str(row["result_summary_md"]).lower()
            )
            status = "queued"
            if running:
                status = "running"
            elif queued == 0 and total > 0:
                status = "finished"
            conn.execute(
                """
                UPDATE cohorts
                SET status = ?, total_jobs = ?, queued_jobs = ?, running_jobs = ?, succeeded_jobs = ?,
                    failed_jobs = ?, yielded_lemmas = ?, yielded_counterexamples = ?, yielded_blockers = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, total, queued, running, succeeded, failed, lemmas, counterexamples, blockers, utc_now(), cohort_id),
            )
            conn.commit()

    def get_cohort(self, cohort_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM cohorts WHERE id = ?", (cohort_id,)).fetchone()
        return dict(row) if row else None

    def list_cohorts(self, problem_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM cohorts WHERE problem_id = ? ORDER BY created_at DESC",
                (problem_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def ensure_program_state(self, problem_id: str) -> ProgramState:
        now = utc_now()
        payload = {
            "version": 1,
            "worldsmith_policy_md": "Prefer falsification-first worlds with explicit hidden objects and concise bridges.",
            "retrieval_policy_md": "Use up to 3 formal, 3 literature, and 3 internal analogs. Prefer local corpora and internal frontier evidence.",
            "compiler_policy_md": "Every delta must compile to one bridge, one kill test, one theorem skeleton, and a bounded job fanout.",
            "frontier_policy_md": "Prioritize open target obligations, blocked theorem skeletons, and fresh fractures with actionable deltas.",
            "acceptance_policy_md": "Accept only replayable gain, proof debt reduction, or sharper fractures. Reject narrative-only motion.",
            "updated_at": now,
        }
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM program_state WHERE problem_id = ?",
                (problem_id,),
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO program_state(problem_id, version, payload_json, updated_at) VALUES(?, 1, ?, ?)",
                    (problem_id, stable_json(payload), now),
                )
                conn.commit()
                row = conn.execute(
                    "SELECT payload_json FROM program_state WHERE problem_id = ?",
                    (problem_id,),
                ).fetchone()
        loaded = parse_json(str(row["payload_json"]), default=payload)
        return ProgramState(**loaded)

    def get_program_state(self, problem_id: str) -> ProgramState:
        return self.ensure_program_state(problem_id)

    def set_program_state(self, problem_id: str, state: ProgramState) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO program_state(problem_id, version, payload_json, updated_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(problem_id) DO UPDATE SET
                    version = excluded.version,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (problem_id, state.version, stable_json(asdict(state)), state.updated_at),
            )
            conn.commit()

    def list_events(self, problem_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM events WHERE problem_id = ? ORDER BY created_at ASC"
        params: list[Any] = [problem_id]
        if limit is not None:
            sql = "SELECT * FROM events WHERE problem_id = ? ORDER BY created_at DESC LIMIT ?"
            params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            d["score_delta"] = parse_json(str(row["score_delta_json"]), default={})
            d["artifact_refs"] = parse_json(str(row["artifact_refs_json"]), default=[])
            out.append(d)
        if limit is not None:
            out.reverse()
        return out

    def rebuild_state(self, problem_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM frontier_nodes WHERE problem_id = ?", (problem_id,))
            conn.execute("DELETE FROM world_heads WHERE problem_id = ?", (problem_id,))
            conn.execute("DELETE FROM fracture_heads WHERE problem_id = ?", (problem_id,))
            conn.commit()
        for event in self.list_events(problem_id):
            for ref in event["artifact_refs"]:
                artifact = self.get_artifact(ref)
                if artifact is None:
                    continue
                content = artifact["content"]
                kind = artifact["artifact_kind"]
                if kind == "frontier_node":
                    self.upsert_frontier_node(FrontierNode(**content))
                elif kind == "world_head":
                    payload = dict(content)
                    payload["last_event_id"] = event["id"]
                    self.replace_world_head(problem_id, payload)
                elif kind == "fracture_head":
                    payload = dict(content)
                    payload["last_event_id"] = event["id"]
                    self.replace_fracture_head(problem_id, payload)

    def snapshot(self, problem_slug_or_id: str) -> dict[str, Any]:
        problem = self.get_problem(problem_slug_or_id)
        if problem is None:
            raise KeyError(problem_slug_or_id)
        problem_id = str(problem["id"])
        frontier = self.get_frontier_nodes(problem_id)
        worlds = self.list_world_heads(problem_id)
        fractures = self.list_fracture_heads(problem_id)
        cohorts = self.list_cohorts(problem_id)
        jobs = self.list_jobs(problem_id)
        events = self.list_events(problem_id, limit=20)
        program = self.get_program_state(problem_id)
        return {
            "problem": problem,
            "frontier": frontier,
            "worlds": worlds,
            "fractures": fractures,
            "cohorts": cohorts,
            "jobs": jobs,
            "events": events,
            "program": asdict(program),
        }
