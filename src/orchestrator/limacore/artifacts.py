from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def artifact_hash(artifact_kind: str, mime_type: str, content: Any) -> str:
    payload = stable_json(
        {
            "artifact_kind": artifact_kind,
            "mime_type": mime_type,
            "content": content,
        }
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def parse_json(value: str, *, default: Any) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default
