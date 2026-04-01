"""Seed per-campaign Lean/Lake workspaces from bundled templates (minimal or Mathlib)."""

from __future__ import annotations

import shutil
from pathlib import Path

VALID_TEMPLATES = frozenset({"minimal", "mathlib"})


def template_dir(name: str) -> Path:
    n = (name or "minimal").strip().lower()
    if n not in VALID_TEMPLATES:
        n = "minimal"
    return Path(__file__).resolve().parent / "lean_templates" / n


def ensure_workspace(workspace_dir: str, template: str = "minimal") -> None:
    """If workspace has no lean-toolchain, copy the bundled template (idempotent)."""
    ws = Path(workspace_dir).resolve()
    ws.mkdir(parents=True, exist_ok=True)
    if (ws / "lean-toolchain").is_file():
        return
    src = template_dir(template)
    if not src.is_dir():
        return
    for item in src.iterdir():
        dest = ws / item.name
        if item.is_file():
            shutil.copy2(item, dest)
        elif item.is_dir():
            shutil.copytree(item, dest, dirs_exist_ok=True)
