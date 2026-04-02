"""Central environment-driven configuration (caps, paths, LLM context limits)."""

from __future__ import annotations

import os
from pathlib import Path


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)).strip())
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)).strip())
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

# Tier-0-friendly defaults: fewer parallel completions + slightly longer ticks reduce LLM bursts.
MAX_ACTIVE_EXPERIMENTS = _int_env("MAX_ACTIVE_EXPERIMENTS", 3)
TICK_INTERVAL = _int_env("TICK_INTERVAL", 60)
MAX_EXPERIMENTS = _int_env("MAX_EXPERIMENTS", 100)

# Problem map (cartographer): refresh when experiments finish, or at least every N global ticks
MAP_REFRESH_MAX_INTERVAL_TICKS = _int_env("MAP_REFRESH_MAX_INTERVAL_TICKS", 12)

# LLM context (reasoning tick)
LLM_EVIDENCE_TARGET_TAIL = _int_env("LLM_EVIDENCE_TARGET_TAIL", 24)
LLM_EXPERIMENT_SUMMARY_CHARS = _int_env("LLM_EXPERIMENT_SUMMARY_CHARS", 4000)
LLM_TICK_REASONING_CHARS = _int_env("LLM_TICK_REASONING_CHARS", 4000)
LLM_RECENT_STRUCTURED_EXPERIMENTS = _int_env("LLM_RECENT_STRUCTURED_EXPERIMENTS", 12)
LLM_STRUCTURED_EXPERIMENTS_PER_TARGET = _int_env(
    "LLM_STRUCTURED_EXPERIMENTS_PER_TARGET", 3
)
LLM_LEDGER_ENTRIES_LIMIT = _int_env("LLM_LEDGER_ENTRIES_LIMIT", 40)

# Summarization of raw Aristotle output
LLM_SUMMARIZE_INPUT_CHARS = _int_env("LLM_SUMMARIZE_INPUT_CHARS", 50000)
# Per manager tick: only this many completions use LLM summarize; rest use truncation (saves RPM).
LLM_SUMMARIZE_MAX_LLM_CALLS_PER_TICK = _int_env("LLM_SUMMARIZE_MAX_LLM_CALLS_PER_TICK", 2)

# Global spacing between LLM HTTP calls (same process). 3.5s ≈ ≤17 req/min; raise for stricter APIs.
LLM_MIN_SECONDS_BETWEEN_REQUESTS = _float_env("LLM_MIN_SECONDS_BETWEEN_REQUESTS", 3.5)
LLM_MAX_RETRIES_429 = _int_env("LLM_MAX_RETRIES_429", 12)

# JSON mode for chat completions
LLM_JSON_MODE = _bool_env("LLM_JSON_MODE", True)

# When true (default), missing aristotle_result.json is filled by synthesizing schema v1
# JSON from ARISTOTLE_SUMMARY.md so storage + parsing use one structured path.
SYNTHESIZE_STRUCTURED_JSON = _bool_env("SYNTHESIZE_STRUCTURED_JSON", True)

# Admin HTTP API (Bearer ADMIN_TOKEN, or X-Admin-Token, or ?admin_token=)
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "").strip()

# LLM (also read in llm.py via this module for caps / JSON mode)
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o")
