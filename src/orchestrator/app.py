from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from fastapi.templating import Jinja2Templates

from orchestrator import config as app_config
from orchestrator.admin_routes import build_admin_router
from orchestrator.db import Database
from orchestrator.llm import decompose_prompt
from orchestrator.manager import manager_loop
from orchestrator.models import CampaignStatus, TargetStatus
from orchestrator.workspace_migration import migrate_legacy_shared_workspaces
from orchestrator.workspace_seed import VALID_TEMPLATES, ensure_workspace

logging.basicConfig(level=logging.INFO)

DATABASE_PATH = app_config.DATABASE_PATH

db = Database(DATABASE_PATH)

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _operator_runtime_context() -> dict:
    return {
        "max_experiments": app_config.MAX_EXPERIMENTS,
        "max_active_experiments": app_config.MAX_ACTIVE_EXPERIMENTS,
        "tick_interval_sec": app_config.TICK_INTERVAL,
        "workspace_root": str(Path(app_config.WORKSPACE_ROOT).resolve()),
    }


def _progress_stats(state) -> dict:
    total = len(state.targets)
    resolved = sum(
        1
        for t in state.targets
        if t.status
        in (TargetStatus.VERIFIED, TargetStatus.REFUTED, TargetStatus.BLOCKED)
    )
    pct = (100 * resolved // total) if total else 0
    return {"target_total": total, "target_resolved": resolved, "progress_percent": pct}


def _ticks_view(rows: list[dict]) -> list[dict]:
    out = []
    for row in rows:
        d = dict(row)
        try:
            d["actions_parsed"] = json.loads(d.get("actions_json") or "{}")
        except json.JSONDecodeError:
            d["actions_parsed"] = {}
        try:
            d["actions_json_pretty"] = json.dumps(
                d["actions_parsed"], indent=2, default=str
            )
        except (TypeError, ValueError):
            d["actions_json_pretty"] = "{}"
        out.append(d)
    return out


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.initialize()
    Path(app_config.WORKSPACE_ROOT).mkdir(parents=True, exist_ok=True)
    migrate_legacy_shared_workspaces(
        db,
        workspace_root=app_config.WORKSPACE_ROOT,
        legacy_dir=app_config.WORKSPACE_LEGACY_DIR or None,
    )
    task = asyncio.create_task(manager_loop(db))
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Aristotle Orchestrator", lifespan=lifespan)
app.include_router(build_admin_router(db))


@app.get("/health")
async def health():
    db_ok, db_msg = db.check_connection()
    campaigns = db.get_active_campaigns()
    status = "healthy" if db_ok else "degraded"
    return {
        "status": status,
        "database_ok": db_ok,
        "database_message": db_msg,
        "active_campaigns": len(campaigns),
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    campaigns = db.get_all_campaigns()
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "campaigns": campaigns,
            "selected": None,
            "state": None,
            "ticks": [],
            "progress": None,
            "operator": _operator_runtime_context(),
        },
    )


@app.get("/campaign/{campaign_id}", response_class=HTMLResponse)
async def campaign_detail(request: Request, campaign_id: str):
    campaigns = db.get_all_campaigns()
    try:
        state = db.get_campaign_state(campaign_id)
    except ValueError:
        return RedirectResponse("/", status_code=303)
    ticks = _ticks_view(db.get_recent_ticks(campaign_id, limit=20))
    progress = _progress_stats(state)
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "campaigns": campaigns,
            "selected": campaign_id,
            "state": state,
            "ticks": ticks,
            "progress": progress,
            "operator": _operator_runtime_context(),
        },
    )


@app.post("/api/campaign")
async def start_campaign(
    prompt: str = Form(...),
    workspace_template: str = Form(""),
):
    tmpl = (workspace_template or "").strip().lower()
    if tmpl not in VALID_TEMPLATES:
        tmpl = (
            app_config.DEFAULT_WORKSPACE_TEMPLATE
            if app_config.DEFAULT_WORKSPACE_TEMPLATE in VALID_TEMPLATES
            else "minimal"
        )
    campaign_id = db.create_campaign(
        prompt,
        workspace_root=app_config.WORKSPACE_ROOT,
        workspace_template=tmpl,
    )
    ws_dir = str((Path(app_config.WORKSPACE_ROOT).resolve() / campaign_id))
    ensure_workspace(ws_dir, tmpl)
    targets = await decompose_prompt(prompt)
    db.add_targets(campaign_id, [t.description for t in targets])
    return RedirectResponse(f"/campaign/{campaign_id}", status_code=303)


class NewCampaignJSON(BaseModel):
    prompt: str = Field(min_length=1)
    workspace_template: str = Field(default="minimal")


@app.post("/api/campaign/start")
async def start_campaign_json(body: NewCampaignJSON):
    tmpl = body.workspace_template.strip().lower()
    if tmpl not in VALID_TEMPLATES:
        tmpl = (
            app_config.DEFAULT_WORKSPACE_TEMPLATE
            if app_config.DEFAULT_WORKSPACE_TEMPLATE in VALID_TEMPLATES
            else "minimal"
        )
    campaign_id = db.create_campaign(
        body.prompt.strip(),
        workspace_root=app_config.WORKSPACE_ROOT,
        workspace_template=tmpl,
    )
    ws_dir = str((Path(app_config.WORKSPACE_ROOT).resolve() / campaign_id))
    ensure_workspace(ws_dir, tmpl)
    targets = await decompose_prompt(body.prompt.strip())
    db.add_targets(campaign_id, [t.description for t in targets])
    return JSONResponse(
        {
            "campaign_id": campaign_id,
            "workspace_dir": ws_dir,
            "workspace_template": tmpl,
            "targets_created": len(targets),
        },
        status_code=201,
    )


@app.get("/api/campaign/{campaign_id}/ledger")
async def campaign_ledger_json(campaign_id: str, limit: int = 200):
    """Read-only lemma / obligation ledger rows for dashboards or tooling."""
    if not db.campaign_exists(campaign_id):
        return JSONResponse({"error": "unknown campaign"}, status_code=404)
    rows = db.get_recent_ledger_entries(campaign_id, min(limit, 2000))
    return {"campaign_id": campaign_id, "entries": rows}


@app.get("/api/campaign/{campaign_id}/state", response_class=HTMLResponse)
async def campaign_state_fragment(request: Request, campaign_id: str):
    try:
        state = db.get_campaign_state(campaign_id)
    except ValueError:
        return HTMLResponse("", status_code=404)
    ticks = _ticks_view(db.get_recent_ticks(campaign_id, limit=20))
    progress = _progress_stats(state)
    return templates.TemplateResponse(
        request,
        "campaign_panel.html",
        {
            "campaigns": db.get_all_campaigns(),
            "selected": campaign_id,
            "state": state,
            "ticks": ticks,
            "progress": progress,
            "operator": _operator_runtime_context(),
        },
    )


@app.post("/api/campaign/{campaign_id}/pause")
async def pause_campaign(campaign_id: str):
    db.update_campaign_status(campaign_id, CampaignStatus.PAUSED.value)
    return RedirectResponse(f"/campaign/{campaign_id}", status_code=303)


@app.post("/api/campaign/{campaign_id}/resume")
async def resume_campaign(campaign_id: str):
    db.update_campaign_status(campaign_id, CampaignStatus.ACTIVE.value)
    return RedirectResponse(f"/campaign/{campaign_id}", status_code=303)
