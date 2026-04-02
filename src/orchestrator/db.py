from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from orchestrator.models import (
    Campaign,
    CampaignState,
    CampaignStatus,
    Experiment,
    ExperimentStatus,
    Target,
    TargetStatus,
    Tick,
    Verdict,
)
from orchestrator.problem_map_util import (
    map_needs_init,
    parse_problem_map,
    seed_problem_map_json,
)


def _new_id() -> str:
    return uuid4().hex[:12]


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(str(r[1]) == col for r in cur.fetchall())


def _get_user_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA user_version").fetchone()
    return int(row[0]) if row else 0


def _set_user_version(conn: sqlite3.Connection, v: int) -> None:
    conn.execute(f"PRAGMA user_version = {v}")


class Database:
    def __init__(self, path: str) -> None:
        self.path = path

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
                CREATE TABLE IF NOT EXISTS campaigns (
                    id TEXT PRIMARY KEY,
                    prompt TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    workspace_dir TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS targets (
                    id TEXT PRIMARY KEY,
                    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
                    description TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    evidence_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS experiments (
                    id TEXT PRIMARY KEY,
                    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
                    target_id TEXT NOT NULL REFERENCES targets(id),
                    objective TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    aristotle_job_id TEXT,
                    result_raw TEXT,
                    result_summary TEXT,
                    verdict TEXT,
                    submitted_at TEXT,
                    completed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
                    tick_number INTEGER NOT NULL,
                    reasoning TEXT NOT NULL,
                    actions_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_targets_campaign ON targets(campaign_id);
                CREATE INDEX IF NOT EXISTS idx_experiments_campaign ON experiments(campaign_id);
                CREATE INDEX IF NOT EXISTS idx_ticks_campaign ON ticks(campaign_id);
                """
            )
            self._run_migrations(conn)
            conn.commit()
        finally:
            conn.close()

    def _run_migrations(self, conn: sqlite3.Connection) -> None:
        v = _get_user_version(conn)
        if v < 1:
            if not _column_exists(conn, "campaigns", "workspace_template"):
                conn.execute(
                    "ALTER TABLE campaigns ADD COLUMN workspace_template TEXT NOT NULL DEFAULT 'minimal'"
                )
            extras = [
                ("parsed_proved_lemmas_json", "TEXT NOT NULL DEFAULT '[]'"),
                ("parsed_generated_lemmas_json", "TEXT NOT NULL DEFAULT '[]'"),
                ("parsed_unsolved_goals_json", "TEXT NOT NULL DEFAULT '[]'"),
                ("parsed_blockers_json", "TEXT NOT NULL DEFAULT '[]'"),
                ("parsed_counterexamples_json", "TEXT NOT NULL DEFAULT '[]'"),
                ("parsed_error_message", "TEXT NOT NULL DEFAULT ''"),
            ]
            for col, decl in extras:
                if not _column_exists(conn, "experiments", col):
                    conn.execute(f"ALTER TABLE experiments ADD COLUMN {col} {decl}")
            _set_user_version(conn, 1)
            v = 1
        if v < 2:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS lemma_ledger (
                    id TEXT PRIMARY KEY,
                    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
                    target_id TEXT NOT NULL REFERENCES targets(id),
                    experiment_id TEXT NOT NULL REFERENCES experiments(id),
                    label TEXT NOT NULL,
                    status TEXT NOT NULL,
                    detail TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_ledger_campaign ON lemma_ledger(campaign_id);
                CREATE INDEX IF NOT EXISTS idx_ledger_experiment ON lemma_ledger(experiment_id);
                CREATE TABLE IF NOT EXISTS ops_counters (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS manager_tick_diagnostics (
                    campaign_id TEXT PRIMARY KEY REFERENCES campaigns(id),
                    last_error_class TEXT NOT NULL DEFAULT '',
                    last_error_message TEXT NOT NULL DEFAULT '',
                    last_tick_number INTEGER NOT NULL DEFAULT -1,
                    updated_at TEXT NOT NULL
                );
                """
            )
            _set_user_version(conn, 2)
            v = 2
        if v < 3:
            v3_cols = [
                ("result_structured_json", "TEXT NOT NULL DEFAULT ''"),
                ("parse_schema_version", "INTEGER NOT NULL DEFAULT 0"),
                ("parse_source", "TEXT NOT NULL DEFAULT ''"),
                ("parse_warnings_json", "TEXT NOT NULL DEFAULT '[]'"),
            ]
            for col, decl in v3_cols:
                if not _column_exists(conn, "experiments", col):
                    conn.execute(f"ALTER TABLE experiments ADD COLUMN {col} {decl}")
            _set_user_version(conn, 3)
            v = 3
        if v < 4:
            if not _column_exists(conn, "campaigns", "problem_map_json"):
                conn.execute(
                    "ALTER TABLE campaigns ADD COLUMN problem_map_json TEXT NOT NULL DEFAULT '{}'"
                )
            if not _column_exists(conn, "campaigns", "problem_refs_json"):
                conn.execute(
                    "ALTER TABLE campaigns ADD COLUMN problem_refs_json TEXT NOT NULL DEFAULT '{}'"
                )
            if not _column_exists(conn, "experiments", "move_kind"):
                conn.execute(
                    "ALTER TABLE experiments ADD COLUMN move_kind TEXT NOT NULL DEFAULT 'prove'"
                )
            if not _column_exists(conn, "experiments", "move_note"):
                conn.execute(
                    "ALTER TABLE experiments ADD COLUMN move_note TEXT NOT NULL DEFAULT ''"
                )
            _set_user_version(conn, 4)

    def create_campaign(
        self,
        prompt: str,
        *,
        workspace_root: str,
        workspace_template: str = "minimal",
        problem_refs_json: str = "{}",
        problem_map_json: str | None = None,
    ) -> str:
        cid = _new_id()
        ws_dir = str((Path(workspace_root).resolve() / cid))
        now = datetime.utcnow().isoformat()
        tmpl = (workspace_template or "minimal").strip().lower() or "minimal"
        pmap = (
            problem_map_json
            if problem_map_json is not None
            else seed_problem_map_json(prompt)
        )
        prefs = problem_refs_json if problem_refs_json.strip() else "{}"
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO campaigns (
                  id, prompt, status, workspace_dir, workspace_template,
                  problem_map_json, problem_refs_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cid,
                    prompt,
                    CampaignStatus.ACTIVE.value,
                    ws_dir,
                    tmpl,
                    pmap,
                    prefs,
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return cid

    def update_campaign_workspace_dir(self, campaign_id: str, workspace_dir: str) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE campaigns SET workspace_dir = ? WHERE id = ?",
                (workspace_dir, campaign_id),
            )
            conn.commit()
        finally:
            conn.close()

    def ensure_problem_map_initialized(self, campaign_id: str, prompt: str) -> None:
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT problem_map_json FROM campaigns WHERE id = ?",
                (campaign_id,),
            )
            row = cur.fetchone()
            if not row or "problem_map_json" not in row.keys():
                return
            raw = row["problem_map_json"]
            parsed = parse_problem_map(raw if raw is not None else None)
            if map_needs_init(parsed):
                conn.execute(
                    "UPDATE campaigns SET problem_map_json = ? WHERE id = ?",
                    (seed_problem_map_json(prompt), campaign_id),
                )
                conn.commit()
        finally:
            conn.close()

    def update_campaign_problem_map(self, campaign_id: str, problem_map_json: str) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE campaigns SET problem_map_json = ? WHERE id = ?",
                (problem_map_json, campaign_id),
            )
            conn.commit()
        finally:
            conn.close()

    def add_targets(self, campaign_id: str, descriptions: list[str]) -> list[str]:
        ids: list[str] = []
        now = datetime.utcnow().isoformat()
        conn = self._connect()
        try:
            for desc in descriptions:
                tid = _new_id()
                conn.execute(
                    "INSERT INTO targets (id, campaign_id, description, status, evidence_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (tid, campaign_id, desc, TargetStatus.OPEN.value, "[]", now),
                )
                ids.append(tid)
            conn.commit()
        finally:
            conn.close()
        return ids

    def _row_campaign(self, row: sqlite3.Row) -> Campaign:
        keys = set(row.keys())
        tmpl = row["workspace_template"] if "workspace_template" in keys else "minimal"
        pmap = str(row["problem_map_json"] or "{}") if "problem_map_json" in keys else "{}"
        prefs = str(row["problem_refs_json"] or "{}") if "problem_refs_json" in keys else "{}"
        return Campaign(
            id=row["id"],
            prompt=row["prompt"],
            status=CampaignStatus(row["status"]),
            workspace_dir=row["workspace_dir"] or "",
            workspace_template=str(tmpl or "minimal"),
            created_at=_parse_dt(row["created_at"]) or datetime.utcnow(),
            problem_map_json=pmap,
            problem_refs_json=prefs,
        )

    def _parse_json_list(self, raw: str | None) -> list[str]:
        if not raw:
            return []
        try:
            v = json.loads(raw)
            if isinstance(v, list):
                return [str(x) for x in v if x is not None]
        except json.JSONDecodeError:
            pass
        return []

    def _row_experiment(self, row: sqlite3.Row) -> Experiment:
        verdict_val = row["verdict"]
        verdict = Verdict(verdict_val) if verdict_val else None
        keys = set(row.keys())
        err_raw = ""
        if "parsed_error_message" in keys and row["parsed_error_message"] is not None:
            err_raw = str(row["parsed_error_message"])
        mk = str(row["move_kind"] or "prove") if "move_kind" in keys else "prove"
        mn = str(row["move_note"] or "") if "move_note" in keys else ""
        return Experiment(
            id=row["id"],
            campaign_id=row["campaign_id"],
            target_id=row["target_id"],
            objective=row["objective"],
            move_kind=mk,
            move_note=mn,
            status=ExperimentStatus(row["status"]),
            aristotle_job_id=row["aristotle_job_id"],
            result_raw=row["result_raw"],
            result_summary=row["result_summary"],
            verdict=verdict,
            submitted_at=_parse_dt(row["submitted_at"]),
            completed_at=_parse_dt(row["completed_at"]),
            parsed_proved_lemmas=self._parse_json_list(
                row["parsed_proved_lemmas_json"] if "parsed_proved_lemmas_json" in keys else None
            ),
            parsed_generated_lemmas=self._parse_json_list(
                row["parsed_generated_lemmas_json"]
                if "parsed_generated_lemmas_json" in keys
                else None
            ),
            parsed_unsolved_goals=self._parse_json_list(
                row["parsed_unsolved_goals_json"] if "parsed_unsolved_goals_json" in keys else None
            ),
            parsed_blockers=self._parse_json_list(
                row["parsed_blockers_json"] if "parsed_blockers_json" in keys else None
            ),
            parsed_counterexamples=self._parse_json_list(
                row["parsed_counterexamples_json"]
                if "parsed_counterexamples_json" in keys
                else None
            ),
            parsed_error_message=err_raw,
            result_structured_json=str(row["result_structured_json"])
            if "result_structured_json" in keys and row["result_structured_json"] is not None
            else "",
            parse_schema_version=int(row["parse_schema_version"] or 0)
            if "parse_schema_version" in keys
            else 0,
            parse_source=str(row["parse_source"] or "")
            if "parse_source" in keys
            else "",
            parse_warnings=self._parse_json_list(
                row["parse_warnings_json"] if "parse_warnings_json" in keys else None
            ),
        )

    def _row_tick(self, row: sqlite3.Row) -> Tick:
        actions: dict[str, Any] = {}
        try:
            actions = json.loads(row["actions_json"] or "{}")
            if not isinstance(actions, dict):
                actions = {}
        except json.JSONDecodeError:
            actions = {}
        return Tick(
            id=row["id"],
            campaign_id=row["campaign_id"],
            tick_number=row["tick_number"],
            reasoning=row["reasoning"],
            actions=actions,
            created_at=_parse_dt(row["created_at"]) or datetime.utcnow(),
        )

    def get_recent_structured_experiments(
        self, campaign_id: str, limit: int
    ) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT id, target_id, objective, move_kind, status, verdict, result_summary,
                  parsed_proved_lemmas_json, parsed_generated_lemmas_json,
                  parsed_unsolved_goals_json, parsed_blockers_json, parsed_counterexamples_json,
                  parsed_error_message, completed_at,
                  parse_source, parse_schema_version
                FROM experiments
                WHERE campaign_id = ? AND status = ?
                ORDER BY datetime(completed_at) DESC, id DESC
                LIMIT ?
                """,
                (campaign_id, ExperimentStatus.COMPLETED.value, limit),
            )
            return [self._structured_experiment_row(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def _structured_experiment_row(self, r: sqlite3.Row) -> dict[str, Any]:
        rk = set(r.keys())
        mk = str(r["move_kind"] or "prove") if "move_kind" in rk else "prove"
        return {
            "id": r["id"],
            "target_id": r["target_id"],
            "objective": r["objective"],
            "move_kind": mk,
            "status": r["status"],
            "verdict": r["verdict"],
            "result_summary": r["result_summary"],
            "proved_lemmas": self._parse_json_list(r["parsed_proved_lemmas_json"]),
            "generated_lemmas": self._parse_json_list(r["parsed_generated_lemmas_json"]),
            "unsolved_goals": self._parse_json_list(r["parsed_unsolved_goals_json"]),
            "blockers": self._parse_json_list(r["parsed_blockers_json"]),
            "counterexamples": self._parse_json_list(r["parsed_counterexamples_json"]),
            "error_message": r["parsed_error_message"] or "",
            "completed_at": r["completed_at"],
            "parse_source": r["parse_source"] if "parse_source" in r.keys() else "",
            "parse_schema_version": int(r["parse_schema_version"] or 0)
            if "parse_schema_version" in r.keys()
            else 0,
        }

    def get_structured_experiments_for_targets(
        self,
        campaign_id: str,
        target_ids: list[str],
        limit_per_target: int,
    ) -> dict[str, list[dict[str, Any]]]:
        """Recent completed experiments per target (structured fields) for LLM context."""
        if limit_per_target <= 0 or not target_ids:
            return {}
        out: dict[str, list[dict[str, Any]]] = {tid: [] for tid in target_ids}
        conn = self._connect()
        try:
            for tid in target_ids:
                cur = conn.execute(
                    """
                    SELECT id, target_id, objective, move_kind, status, verdict, result_summary,
                      parsed_proved_lemmas_json, parsed_generated_lemmas_json,
                      parsed_unsolved_goals_json, parsed_blockers_json, parsed_counterexamples_json,
                      parsed_error_message, completed_at,
                      parse_source, parse_schema_version
                    FROM experiments
                    WHERE campaign_id = ? AND target_id = ? AND status = ?
                    ORDER BY datetime(completed_at) DESC, id DESC
                    LIMIT ?
                    """,
                    (
                        campaign_id,
                        tid,
                        ExperimentStatus.COMPLETED.value,
                        limit_per_target,
                    ),
                )
                rows = cur.fetchall()
                out[tid] = [self._structured_experiment_row(r) for r in rows]
            return out
        finally:
            conn.close()

    def get_recent_ledger_entries(self, campaign_id: str, limit: int) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT * FROM lemma_ledger
                WHERE campaign_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT ?
                """,
                (campaign_id, limit),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def get_campaign_state(self, campaign_id: str) -> CampaignState:
        conn = self._connect()
        try:
            cur = conn.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,))
            crow = cur.fetchone()
            if not crow:
                raise ValueError(f"Unknown campaign: {campaign_id}")
            campaign = self._row_campaign(crow)

            tcur = conn.execute(
                "SELECT * FROM targets WHERE campaign_id = ? ORDER BY created_at",
                (campaign_id,),
            )
            targets = [self._row_target(r) for r in tcur.fetchall()]

            ecur = conn.execute(
                """
                SELECT * FROM experiments WHERE campaign_id = ?
                ORDER BY CASE WHEN submitted_at IS NULL THEN 1 ELSE 0 END, submitted_at DESC, id
                """,
                (campaign_id,),
            )
            experiments = [self._row_experiment(r) for r in ecur.fetchall()]

            tick_cur = conn.execute(
                """
                SELECT * FROM ticks WHERE campaign_id = ? ORDER BY tick_number DESC, id DESC LIMIT 5
                """,
                (campaign_id,),
            )
            recent_ticks = list(reversed([self._row_tick(r) for r in tick_cur.fetchall()]))

            from orchestrator import config as app_config

            ctx_exp = self.get_recent_structured_experiments(
                campaign_id, app_config.LLM_RECENT_STRUCTURED_EXPERIMENTS
            )
            ctx_led = self.get_recent_ledger_entries(
                campaign_id, app_config.LLM_LEDGER_ENTRIES_LIMIT
            )
            tid_list = [t.id for t in targets]
            ctx_by_target = self.get_structured_experiments_for_targets(
                campaign_id,
                tid_list,
                app_config.LLM_STRUCTURED_EXPERIMENTS_PER_TARGET,
            )

            return CampaignState(
                campaign=campaign,
                targets=targets,
                experiments=experiments,
                recent_ticks=recent_ticks,
                manager_context_experiments=ctx_exp,
                manager_context_experiments_by_target=ctx_by_target,
                manager_context_ledger=ctx_led,
            )
        finally:
            conn.close()

    def create_experiment(
        self,
        campaign_id: str,
        target_id: str,
        objective: str,
        *,
        move_kind: str = "prove",
        move_note: str = "",
    ) -> str:
        eid = _new_id()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO experiments (
                  id, campaign_id, target_id, objective, move_kind, move_note, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    eid,
                    campaign_id,
                    target_id,
                    objective,
                    move_kind[:64],
                    (move_note or "")[:2000],
                    ExperimentStatus.PENDING.value,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return eid

    def update_experiment_submitted(self, experiment_id: str, aristotle_job_id: str) -> None:
        now = datetime.utcnow().isoformat()
        conn = self._connect()
        try:
            conn.execute(
                """
                UPDATE experiments
                SET status = ?, aristotle_job_id = ?, submitted_at = ?
                WHERE id = ?
                """,
                (ExperimentStatus.SUBMITTED.value, aristotle_job_id, now, experiment_id),
            )
            conn.commit()
        finally:
            conn.close()

    def update_experiment_running(self, experiment_id: str) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE experiments SET status = ? WHERE id = ? AND status = ?",
                (ExperimentStatus.RUNNING.value, experiment_id, ExperimentStatus.SUBMITTED.value),
            )
            conn.commit()
        finally:
            conn.close()

    def update_experiment_completed(
        self,
        experiment_id: str,
        *,
        result_raw: str,
        result_summary: str,
        verdict: str,
        parsed_proved_lemmas: list[str],
        parsed_generated_lemmas: list[str],
        parsed_unsolved_goals: list[str],
        parsed_blockers: list[str],
        parsed_counterexamples: list[str],
        parsed_error_message: str,
        result_structured_json: str = "",
        parse_schema_version: int = 0,
        parse_source: str = "",
        parse_warnings: list[str] | None = None,
    ) -> None:
        now = datetime.utcnow().isoformat()
        pw = parse_warnings if parse_warnings is not None else []
        conn = self._connect()
        try:
            conn.execute(
                """
                UPDATE experiments
                SET status = ?, result_raw = ?, result_summary = ?, verdict = ?, completed_at = ?,
                    parsed_proved_lemmas_json = ?, parsed_generated_lemmas_json = ?,
                    parsed_unsolved_goals_json = ?, parsed_blockers_json = ?,
                    parsed_counterexamples_json = ?, parsed_error_message = ?,
                    result_structured_json = ?, parse_schema_version = ?, parse_source = ?,
                    parse_warnings_json = ?
                WHERE id = ?
                """,
                (
                    ExperimentStatus.COMPLETED.value,
                    result_raw,
                    result_summary,
                    verdict,
                    now,
                    json.dumps(parsed_proved_lemmas),
                    json.dumps(parsed_generated_lemmas),
                    json.dumps(parsed_unsolved_goals),
                    json.dumps(parsed_blockers),
                    json.dumps(parsed_counterexamples),
                    parsed_error_message or "",
                    result_structured_json or "",
                    int(parse_schema_version or 0),
                    parse_source or "",
                    json.dumps(pw),
                    experiment_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def append_ledger_entries(
        self,
        campaign_id: str,
        target_id: str,
        experiment_id: str,
        entries: list[tuple[str, str, str]],
    ) -> None:
        """entries: list of (label, status, detail) status in proved|attempted|blocked"""
        if not entries:
            return
        now = datetime.utcnow().isoformat()
        conn = self._connect()
        try:
            for label, status, detail in entries:
                lid = _new_id()
                conn.execute(
                    """
                    INSERT INTO lemma_ledger (id, campaign_id, target_id, experiment_id, label, status, detail, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        lid,
                        campaign_id,
                        target_id,
                        experiment_id,
                        label[:1024],
                        status,
                        (detail or "")[:2000],
                        now,
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    def update_experiment_failed(
        self,
        experiment_id: str,
        error: str,
        *,
        verdict: str | None = None,
    ) -> None:
        now = datetime.utcnow().isoformat()
        conn = self._connect()
        try:
            if verdict is not None:
                conn.execute(
                    """
                    UPDATE experiments
                    SET status = ?, result_summary = ?, completed_at = ?,
                        parsed_error_message = ?, verdict = ?
                    WHERE id = ?
                    """,
                    (
                        ExperimentStatus.FAILED.value,
                        error,
                        now,
                        error[:8000],
                        verdict,
                        experiment_id,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE experiments
                    SET status = ?, result_summary = ?, completed_at = ?,
                        parsed_error_message = ?
                    WHERE id = ?
                    """,
                    (ExperimentStatus.FAILED.value, error, now, error[:8000], experiment_id),
                )
            conn.commit()
        finally:
            conn.close()

    def update_target(self, target_id: str, status: str, evidence: str) -> None:
        conn = self._connect()
        try:
            cur = conn.execute("SELECT evidence_json FROM targets WHERE id = ?", (target_id,))
            row = cur.fetchone()
            if not row:
                return
            ev: list[str] = []
            try:
                ev = json.loads(row["evidence_json"] or "[]")
                if not isinstance(ev, list):
                    ev = []
            except json.JSONDecodeError:
                ev = []
            if evidence:
                ev.append(evidence)
            conn.execute(
                "UPDATE targets SET status = ?, evidence_json = ? WHERE id = ?",
                (status, json.dumps(ev), target_id),
            )
            conn.commit()
        finally:
            conn.close()

    def append_target_evidence(self, target_id: str, evidence: str) -> None:
        if not evidence:
            return
        conn = self._connect()
        try:
            cur = conn.execute("SELECT evidence_json FROM targets WHERE id = ?", (target_id,))
            row = cur.fetchone()
            if not row:
                return
            ev: list[str] = []
            try:
                ev = json.loads(row["evidence_json"] or "[]")
                if not isinstance(ev, list):
                    ev = []
            except json.JSONDecodeError:
                ev = []
            ev.append(evidence)
            conn.execute(
                "UPDATE targets SET evidence_json = ? WHERE id = ?",
                (json.dumps(ev), target_id),
            )
            conn.commit()
        finally:
            conn.close()

    def _row_target(self, row: sqlite3.Row) -> Target:
        evidence: list[str] = []
        try:
            evidence = json.loads(row["evidence_json"] or "[]")
            if not isinstance(evidence, list):
                evidence = []
        except json.JSONDecodeError:
            evidence = []
        return Target(
            id=row["id"],
            campaign_id=row["campaign_id"],
            description=row["description"],
            status=TargetStatus(row["status"]),
            evidence=evidence,
            created_at=_parse_dt(row["created_at"]) or datetime.utcnow(),
        )

    def abandon_inflight_aristotle_jobs(self, campaign_id: str, reason: str) -> int:
        """Mark submitted/running experiments failed when the campaign ends anyway."""
        n = 0
        for exp in self.get_running_experiments(campaign_id):
            self.update_experiment_failed(
                str(exp["id"]),
                reason,
                verdict=Verdict.INFRA_ERROR.value,
            )
            self.append_target_evidence(
                str(exp["target_id"]),
                f"Experiment {exp['id']}: {reason[:240]}",
            )
            n += 1
        return n

    def complete_campaign(self, campaign_id: str) -> None:
        self.abandon_inflight_aristotle_jobs(
            campaign_id,
            "Campaign closed before Aristotle returned a final result (in-flight job abandoned).",
        )
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE campaigns SET status = ? WHERE id = ?",
                (CampaignStatus.COMPLETED.value, campaign_id),
            )
            conn.commit()
        finally:
            conn.close()

    def update_campaign_status(self, campaign_id: str, status: str) -> None:
        conn = self._connect()
        try:
            conn.execute("UPDATE campaigns SET status = ? WHERE id = ?", (status, campaign_id))
            conn.commit()
        finally:
            conn.close()

    def get_running_experiments(self, campaign_id: str) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT * FROM experiments
                WHERE campaign_id = ?
                  AND aristotle_job_id IS NOT NULL
                  AND status IN (?, ?)
                """,
                (campaign_id, ExperimentStatus.SUBMITTED.value, ExperimentStatus.RUNNING.value),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def all_targets_resolved(self, campaign_id: str) -> bool:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT COUNT(*) AS c FROM targets
                WHERE campaign_id = ? AND status = ?
                """,
                (campaign_id, TargetStatus.OPEN.value),
            )
            row = cur.fetchone()
            return (row["c"] if row else 0) == 0
        finally:
            conn.close()

    def record_tick(
        self,
        campaign_id: str,
        tick_number: int,
        *,
        reasoning: str,
        actions: dict[str, Any],
    ) -> None:
        now = datetime.utcnow().isoformat()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO ticks (campaign_id, tick_number, reasoning, actions_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (campaign_id, tick_number, reasoning, json.dumps(actions), now),
            )
            conn.commit()
        finally:
            conn.close()

    def get_active_campaigns(self) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT * FROM campaigns WHERE status = ? ORDER BY created_at DESC",
                (CampaignStatus.ACTIVE.value,),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def get_all_campaigns(self) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute("SELECT * FROM campaigns ORDER BY created_at DESC")
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def count_campaigns(self) -> int:
        conn = self._connect()
        try:
            cur = conn.execute("SELECT COUNT(*) AS c FROM campaigns")
            row = cur.fetchone()
            return int(row["c"]) if row else 0
        finally:
            conn.close()

    def campaign_exists(self, campaign_id: str) -> bool:
        conn = self._connect()
        try:
            cur = conn.execute("SELECT 1 FROM campaigns WHERE id = ? LIMIT 1", (campaign_id,))
            return cur.fetchone() is not None
        finally:
            conn.close()

    def get_recent_ticks(self, campaign_id: str, limit: int = 20) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT * FROM ticks WHERE campaign_id = ?
                ORDER BY tick_number DESC, id DESC LIMIT ?
                """,
                (campaign_id, limit),
            )
            rows = [dict(r) for r in cur.fetchall()]
            rows.reverse()
            return rows
        finally:
            conn.close()

    def increment_ops_counter(self, key: str, delta: int = 1) -> None:
        now = datetime.utcnow().isoformat()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO ops_counters (key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  value = value + excluded.value,
                  updated_at = excluded.updated_at
                """,
                (key, delta, now),
            )
            conn.commit()
        finally:
            conn.close()

    def get_ops_counters(self) -> dict[str, int]:
        conn = self._connect()
        try:
            cur = conn.execute("SELECT key, value FROM ops_counters")
            return {str(r["key"]): int(r["value"]) for r in cur.fetchall()}
        finally:
            conn.close()

    def set_tick_diagnostic(
        self,
        campaign_id: str,
        *,
        last_error_class: str,
        last_error_message: str,
        last_tick_number: int,
    ) -> None:
        now = datetime.utcnow().isoformat()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO manager_tick_diagnostics (campaign_id, last_error_class, last_error_message, last_tick_number, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(campaign_id) DO UPDATE SET
                  last_error_class = excluded.last_error_class,
                  last_error_message = excluded.last_error_message,
                  last_tick_number = excluded.last_tick_number,
                  updated_at = excluded.updated_at
                """,
                (
                    campaign_id,
                    last_error_class[:200],
                    last_error_message[:4000],
                    last_tick_number,
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def clear_tick_diagnostic(self, campaign_id: str) -> None:
        conn = self._connect()
        try:
            conn.execute("DELETE FROM manager_tick_diagnostics WHERE campaign_id = ?", (campaign_id,))
            conn.commit()
        finally:
            conn.close()

    def get_all_tick_diagnostics(self) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute("SELECT * FROM manager_tick_diagnostics ORDER BY updated_at DESC")
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def export_operator_bundle(
        self,
        *,
        ticks_limit: int = 5000,
        ledger_limit: int = 20000,
        include_result_raw: bool = False,
        result_raw_max_chars: int = 500_000,
    ) -> dict[str, Any]:
        """Full JSON snapshot for operators (e.g. curl from Railway). Keeps one DB connection."""
        generated_at = datetime.utcnow().isoformat()
        conn = self._connect()
        try:
            ops_rows = conn.execute("SELECT key, value FROM ops_counters").fetchall()
            ops_counters = {str(r["key"]): int(r["value"]) for r in ops_rows}
            diag_cur = conn.execute(
                "SELECT * FROM manager_tick_diagnostics ORDER BY updated_at DESC"
            )
            tick_diagnostics = [dict(r) for r in diag_cur.fetchall()]

            campaigns_out: list[dict[str, Any]] = []
            ccur = conn.execute("SELECT * FROM campaigns ORDER BY created_at DESC")
            for crow in ccur.fetchall():
                campaign = self._row_campaign(crow)
                cid = campaign.id

                tcur = conn.execute(
                    "SELECT * FROM targets WHERE campaign_id = ? ORDER BY created_at",
                    (cid,),
                )
                targets = [self._row_target(r).model_dump(mode="json") for r in tcur.fetchall()]

                ecur = conn.execute(
                    """
                    SELECT * FROM experiments WHERE campaign_id = ?
                    ORDER BY CASE WHEN submitted_at IS NULL THEN 1 ELSE 0 END,
                             submitted_at DESC, id
                    """,
                    (cid,),
                )
                experiments: list[dict[str, Any]] = []
                for erow in ecur.fetchall():
                    exp = self._row_experiment(erow)
                    d = exp.model_dump(mode="json")
                    raw = d.get("result_raw")
                    if not include_result_raw:
                        d["result_raw"] = None
                    elif (
                        result_raw_max_chars > 0
                        and isinstance(raw, str)
                        and len(raw) > result_raw_max_chars
                    ):
                        d["result_raw"] = (
                            raw[:result_raw_max_chars]
                            + "\n... [truncated by export_operator_bundle]"
                        )
                    experiments.append(d)

                tick_cur = conn.execute(
                    """
                    SELECT * FROM ticks WHERE campaign_id = ?
                    ORDER BY tick_number ASC, id ASC
                    LIMIT ?
                    """,
                    (cid, ticks_limit),
                )
                ticks = [self._row_tick(r).model_dump(mode="json") for r in tick_cur.fetchall()]

                led_cur = conn.execute(
                    """
                    SELECT * FROM lemma_ledger WHERE campaign_id = ?
                    ORDER BY datetime(created_at) ASC, id ASC
                    LIMIT ?
                    """,
                    (cid, ledger_limit),
                )
                lemma_ledger = [dict(r) for r in led_cur.fetchall()]

                campaigns_out.append(
                    {
                        "campaign": campaign.model_dump(mode="json"),
                        "targets": targets,
                        "experiments": experiments,
                        "ticks": ticks,
                        "lemma_ledger": lemma_ledger,
                    }
                )

            return {
                "generated_at": generated_at,
                "database_path": self.path,
                "ops_counters": ops_counters,
                "tick_diagnostics": tick_diagnostics,
                "campaigns": campaigns_out,
            }
        finally:
            conn.close()

    def check_connection(self) -> tuple[bool, str]:
        try:
            conn = self._connect()
            try:
                conn.execute("SELECT 1").fetchone()
            finally:
                conn.close()
            return True, "ok"
        except Exception as e:
            return False, str(e)

    def get_campaign_dashboard(self, campaign_id: str) -> dict[str, Any]:
        state = self.get_campaign_state(campaign_id)
        total = len(state.targets)
        resolved = sum(
            1
            for t in state.targets
            if t.status
            in (TargetStatus.VERIFIED, TargetStatus.REFUTED, TargetStatus.BLOCKED)
        )
        pct = int(100 * resolved / total) if total else 0
        by_status: dict[str, int] = {}
        for t in state.targets:
            k = t.status.value
            by_status[k] = by_status.get(k, 0) + 1
        exp_by_status: dict[str, int] = {}
        for e in state.experiments:
            k = e.status.value
            exp_by_status[k] = exp_by_status.get(k, 0) + 1
        return {
            "campaign_id": campaign_id,
            "prompt": state.campaign.prompt,
            "campaign_status": state.campaign.status.value,
            "target_total": total,
            "target_resolved": resolved,
            "progress_percent": pct,
            "targets_by_status": by_status,
            "experiments_by_status": exp_by_status,
            "experiment_count": len(state.experiments),
        }
