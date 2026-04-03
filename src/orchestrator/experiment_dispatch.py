"""Submit experiments to Aristotle (shared by manager loop and shadow promotion handlers)."""

from __future__ import annotations

import logging
import re
from typing import Any

from orchestrator import config as app_config
from orchestrator.aristotle import submit
from orchestrator.db import Database
from orchestrator.models import ExperimentStatus

logger = logging.getLogger("orchestrator.experiment_dispatch")

_FAILURE_BRACKET = re.compile(r"^\[([^\]]+)\]")


def failure_class_from_message(msg: str) -> str:
    m = _FAILURE_BRACKET.match((msg or "").strip())
    if m:
        return m.group(1)
    low = (msg or "").lower()
    if "not set" in low:
        return "config_error"
    return "unknown"


async def try_submit_experiment_now(db: Database, experiment_id: str) -> dict[str, Any]:
    """Submit one pending experiment immediately (e.g. after shadow promotion approve)."""
    if not app_config.SHADOW_ARISTOTLE_IMMEDIATE_ON_APPROVE:
        return {
            "ok": True,
            "skipped": True,
            "reason": "SHADOW_ARISTOTLE_IMMEDIATE_ON_APPROVE off",
        }
    row = db.get_experiment_for_submit(experiment_id)
    if not row:
        return {"ok": False, "error": "unknown experiment"}
    st = str(row.get("status") or "")
    if st != ExperimentStatus.PENDING.value:
        return {"ok": True, "skipped": True, "reason": f"status={st}"}
    if row.get("aristotle_job_id"):
        return {"ok": True, "skipped": True, "reason": "already_submitted"}
    objective = str(row.get("objective") or "").strip()
    ws = str(row.get("workspace_dir") or "").strip()
    if not objective or not ws:
        return {"ok": False, "error": "missing objective or workspace_dir"}
    job_id, err = await submit(objective, ws)
    if job_id:
        db.update_experiment_submitted(experiment_id, job_id)
        db.increment_ops_counter("shadow:aristotle_submit_immediate_ok", 1)
        logger.info(
            "shadow_aristotle_submit_immediate experiment_id=%s job_id=%s",
            experiment_id,
            job_id,
        )
        return {"ok": True, "job_id": job_id}
    db.update_experiment_failed(experiment_id, err or "submit failed")
    fc = failure_class_from_message(err or "")
    db.increment_ops_counter(f"shadow:aristotle_submit_immediate:{fc}", 1)
    logger.warning(
        "shadow_aristotle_submit_immediate_failed experiment_id=%s error=%s",
        experiment_id,
        err,
    )
    return {"ok": False, "error": err or "submit failed"}
