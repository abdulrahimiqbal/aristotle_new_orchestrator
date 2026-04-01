"""Central environment-driven configuration (caps, paths, LLM context limits)."""

from __future__ import annotations

import os
from pathlib import Path


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)).strip())
    except ValueError:
        return default


def _bool_env(name: str, default: bool = True) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() not in ("0", "false", "no", "off")


DATABASE_PATH = os.environ.get("DATABASE_PATH", "orchestrator.db")

# Per-campaign workspaces live under WORKSPACE_ROOT/<campaign_id>/
# Legacy: WORKSPACE_DIR was a single shared tree; see WORKSPACE_LEGACY_DIR / migration.
_default_local_root = str(Path.cwd() / "workspace_root")
WORKSPACE_ROOT = os.environ.get("WORKSPACE_ROOT", _default_local_root).rstrip("/")

# If set, campaigns whose workspace_dir matches this path are migrated once to WORKSPACE_ROOT/<id>/
WORKSPACE_LEGACY_DIR = os.environ.get(
    "WORKSPACE_LEGACY_DIR",
    os.environ.get("WORKSPACE_DIR", ""),
).strip()

DEFAULT_WORKSPACE_TEMPLATE = os.environ.get(
    "DEFAULT_WORKSPACE_TEMPLATE", "minimal"
).strip().lower()

MAX_ACTIVE_EXPERIMENTS = _int_env("MAX_ACTIVE_EXPERIMENTS", 5)
TICK_INTERVAL = _int_env("TICK_INTERVAL", 30)
MAX_EXPERIMENTS = _int_env("MAX_EXPERIMENTS", 100)

# LLM context (reasoning tick)
LLM_EVIDENCE_TARGET_TAIL = _int_env("LLM_EVIDENCE_TARGET_TAIL", 24)
LLM_EXPERIMENT_SUMMARY_CHARS = _int_env("LLM_EXPERIMENT_SUMMARY_CHARS", 4000)
LLM_TICK_REASONING_CHARS = _int_env("LLM_TICK_REASONING_CHARS", 4000)
LLM_RECENT_STRUCTURED_EXPERIMENTS = _int_env("LLM_RECENT_STRUCTURED_EXPERIMENTS", 12)
LLM_LEDGER_ENTRIES_LIMIT = _int_env("LLM_LEDGER_ENTRIES_LIMIT", 40)

# Summarization of raw Aristotle output
LLM_SUMMARIZE_INPUT_CHARS = _int_env("LLM_SUMMARIZE_INPUT_CHARS", 50000)

# JSON mode for chat completions
LLM_JSON_MODE = _bool_env("LLM_JSON_MODE", True)

# Admin HTTP API (Bearer ADMIN_TOKEN, or X-Admin-Token, or ?admin_token=)
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "").strip()

# LLM (also read in llm.py via this module for caps / JSON mode)
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o")
