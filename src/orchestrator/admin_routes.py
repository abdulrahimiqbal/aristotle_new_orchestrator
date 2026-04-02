"""Authenticated admin / operator observability routes."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import HTMLResponse

from orchestrator import config as app_config
from orchestrator.db import Database


def _require_admin(
    authorization: Annotated[str | None, Header()] = None,
    x_admin_token: Annotated[str | None, Header(alias="X-Admin-Token")] = None,
    admin_token: Annotated[
        str | None, Query(description="Optional; prefer Authorization or X-Admin-Token in production")
    ] = None,
) -> bool:
    if not app_config.ADMIN_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="Admin routes disabled. Set ADMIN_TOKEN to enable.",
        )
    bearer: str | None = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer = authorization[7:].strip()
    got = (bearer or (x_admin_token or "").strip() or (admin_token or "").strip())
    if got != app_config.ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True


AdminAuth = Annotated[bool, Depends(_require_admin)]


def build_admin_router(db: Database) -> APIRouter:
    router = APIRouter(prefix="/admin", tags=["admin"])

    @router.get("/status")
    async def admin_status(_auth: AdminAuth) -> dict:
        db_ok, db_msg = db.check_connection()
        root = Path(app_config.WORKSPACE_ROOT).resolve()
        disk_total = disk_free = disk_used = None
        try:
            usage = shutil.disk_usage(str(root))
            disk_total = usage.total
            disk_free = usage.free
            disk_used = usage.used
        except OSError:
            pass
        return {
            "database_ok": db_ok,
            "database_message": db_msg,
            "workspace_root": str(root),
            "workspace_root_exists": root.is_dir(),
            "disk_bytes": {
                "total": disk_total,
                "free": disk_free,
                "used": disk_used,
            },
            "tick_diagnostics": db.get_all_tick_diagnostics(),
            "ops_counters": db.get_ops_counters(),
        }

    @router.get("/config")
    async def admin_config(_auth: AdminAuth) -> dict:
        return {
            "MAX_EXPERIMENTS": app_config.MAX_EXPERIMENTS,
            "MAX_ACTIVE_EXPERIMENTS": app_config.MAX_ACTIVE_EXPERIMENTS,
            "TICK_INTERVAL": app_config.TICK_INTERVAL,
            "WORKSPACE_ROOT": str(Path(app_config.WORKSPACE_ROOT).resolve()),
            "WORKSPACE_LEGACY_DIR": app_config.WORKSPACE_LEGACY_DIR or None,
            "DEFAULT_WORKSPACE_TEMPLATE": app_config.DEFAULT_WORKSPACE_TEMPLATE,
            "LLM_RECENT_STRUCTURED_EXPERIMENTS": app_config.LLM_RECENT_STRUCTURED_EXPERIMENTS,
            "LLM_LEDGER_ENTRIES_LIMIT": app_config.LLM_LEDGER_ENTRIES_LIMIT,
            "LLM_JSON_MODE": app_config.LLM_JSON_MODE,
            "LLM_SUMMARIZE_MAX_LLM_CALLS_PER_TICK": app_config.LLM_SUMMARIZE_MAX_LLM_CALLS_PER_TICK,
            "LLM_MIN_SECONDS_BETWEEN_REQUESTS": app_config.LLM_MIN_SECONDS_BETWEEN_REQUESTS,
            "LLM_MAX_RETRIES_429": app_config.LLM_MAX_RETRIES_429,
            "MAP_REFRESH_MAX_INTERVAL_TICKS": app_config.MAP_REFRESH_MAX_INTERVAL_TICKS,
        }

    @router.get("/export")
    async def admin_export(
        _auth: AdminAuth,
        ticks_limit: Annotated[int, Query(ge=1, le=50_000)] = 5000,
        ledger_limit: Annotated[int, Query(ge=1, le=200_000)] = 20000,
        include_result_raw: bool = False,
        result_raw_max_chars: Annotated[int, Query(ge=0, le=20_000_000)] = 500_000,
    ) -> dict:
        """Download all campaigns, targets, experiments, ticks, and ledger as JSON (no Railway SSH)."""
        return db.export_operator_bundle(
            ticks_limit=ticks_limit,
            ledger_limit=ledger_limit,
            include_result_raw=include_result_raw,
            result_raw_max_chars=result_raw_max_chars,
        )

    @router.get("/ui", response_class=HTMLResponse)
    async def admin_ui(_auth: AdminAuth) -> HTMLResponse:
        html = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><title>Admin · Orchestrator</title>
<style>
body{font-family:system-ui,sans-serif;background:#0c0c0f;color:#e4e4e7;max-width:56rem;margin:2rem auto;padding:0 1rem}
a{color:#a78bfa} pre{background:#18181b;padding:1rem;overflow:auto;border-radius:8px;font-size:12px}
h1{font-size:1.25rem} .muted{color:#71717a;font-size:12px}
</style></head><body>
<h1>Operator panel</h1>
<p class="muted">Prefer <code>Authorization: Bearer …</code> or <code>X-Admin-Token</code> over query strings (logs).</p>
<p>Use JSON endpoints: <a href="/admin/status"><code>/admin/status</code></a>, <a href="/admin/config"><code>/admin/config</code></a>, <a href="/admin/export"><code>/admin/export</code></a> (full DB snapshot; auth required).</p>
<p class="muted">curl example:<br/>
<code>curl -s -H "Authorization: Bearer $ADMIN_TOKEN" http://127.0.0.1:8000/admin/status | jq</code></p>
</body></html>"""
        return HTMLResponse(html)

    return router
