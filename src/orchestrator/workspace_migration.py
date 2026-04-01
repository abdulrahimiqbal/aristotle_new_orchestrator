"""One-time migration from a single shared WORKSPACE_DIR to per-campaign directories."""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

from orchestrator.db import Database

logger = logging.getLogger("orchestrator.workspace_migration")


def _same_path(a: Path, b: Path) -> bool:
    try:
        return a.resolve() == b.resolve()
    except OSError:
        return str(a) == str(b)


def migrate_legacy_shared_workspaces(
    db: Database,
    *,
    workspace_root: str,
    legacy_dir: str | None,
) -> None:
    """Copy or symlink each campaign from a shared legacy workspace into WORKSPACE_ROOT/<id>.

    Runs only for rows whose workspace_dir matches legacy_dir (resolved). Safe to call repeatedly.
    """
    if not legacy_dir or not legacy_dir.strip():
        return
    legacy = Path(legacy_dir).resolve()
    if not legacy.is_dir():
        logger.info(
            "Workspace migration skipped: legacy dir does not exist: %s", legacy
        )
        return
    root = Path(workspace_root).resolve()
    root.mkdir(parents=True, exist_ok=True)

    campaigns = db.get_all_campaigns()
    migrated = 0
    for c in campaigns:
        cur = Path(str(c.get("workspace_dir") or "")).resolve()
        if not _same_path(cur, legacy):
            continue
        cid = str(c["id"])
        dest = root / cid
        marker = dest / ".orchestrator_migrated_from_shared"
        if not dest.exists():
            logger.info(
                "Migrating campaign %s workspace: %s -> %s", cid, legacy, dest
            )
            if os.environ.get("WORKSPACE_MIGRATION_SYMLINK", "").strip().lower() in (
                "1",
                "true",
                "yes",
            ):
                try:
                    dest.symlink_to(legacy, target_is_directory=True)
                except OSError:
                    shutil.copytree(legacy, dest, dirs_exist_ok=True)
            else:
                shutil.copytree(legacy, dest, dirs_exist_ok=True)
            try:
                marker.write_text(str(legacy), encoding="utf-8")
            except OSError:
                pass
        elif not marker.is_file() and (dest / "lean-toolchain").is_file():
            # Already a per-campaign folder from a prior run; just relink DB path
            pass
        db.update_campaign_workspace_dir(cid, str(dest))
        migrated += 1
    if migrated:
        logger.info("Workspace migration updated %s campaign(s).", migrated)
