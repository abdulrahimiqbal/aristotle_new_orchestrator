from __future__ import annotations

from typing import Any

from .db import LimaCoreDB
from .models import FrontierNode


def apply_event_artifacts(db: LimaCoreDB, problem_id: str, event_id: str, artifacts: list[dict[str, Any]]) -> None:
    for artifact in artifacts:
        kind = str(artifact.get("artifact_kind") or "")
        content = artifact.get("content") or {}
        payload = dict(content)
        if kind == "frontier_node":
            db.upsert_frontier_node(FrontierNode(**payload))
        elif kind == "world_head":
            payload["last_event_id"] = event_id
            db.replace_world_head(problem_id, payload)
        elif kind == "fracture_head":
            payload["last_event_id"] = event_id
            db.replace_fracture_head(problem_id, payload)


def rebuild_materialized_state(db: LimaCoreDB, problem_id: str) -> None:
    db.rebuild_state(problem_id)
