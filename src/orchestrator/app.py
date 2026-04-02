from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
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
from orchestrator.problem_map_util import (
    map_progress_stats,
    parse_problem_map,
    parse_problem_refs,
    problem_refs_to_json,
)
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
        "map_refresh_max_interval_ticks": app_config.MAP_REFRESH_MAX_INTERVAL_TICKS,
    }


def _cartography_context(state) -> dict:
    pm = parse_problem_map(state.campaign.problem_map_json)
    refs = parse_problem_refs(state.campaign.problem_refs_json)
    pretty = json.dumps(pm, indent=2, ensure_ascii=False) if pm else "{}"
    return {
        "problem_map": pm,
        "problem_refs": refs,
        "map_progress": map_progress_stats(pm),
        "problem_map_json_pretty": pretty,
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
    db_path = Path(app_config.DATABASE_PATH)
    db_size: int | None = None
    try:
        if db_path.is_file():
            db_size = db_path.stat().st_size
    except OSError:
        pass
    campaign_total = db.count_campaigns() if db_ok else 0
    return {
        "status": status,
        "database_ok": db_ok,
        "database_message": db_msg,
        "database_path": str(db_path),
        "database_size_bytes": db_size,
        "campaign_total": campaign_total,
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
            **_cartography_context(state),
        },
    )


@app.post("/api/campaign")
async def start_campaign(
    prompt: str = Form(...),
    use_mathlib: str | None = Form(None),
    workspace_template: str = Form(""),
    erdos_id: str = Form(""),
    source_url: str = Form(""),
    formal_lean_path: str = Form(""),
    external_notes: str = Form(""),
):
    # Checkbox "use Mathlib" takes precedence over optional legacy workspace_template field.
    if use_mathlib is not None and str(use_mathlib).strip().lower() in (
        "1",
        "on",
        "true",
        "yes",
    ):
        tmpl = "mathlib"
    else:
        tmpl = (workspace_template or "").strip().lower()
    if tmpl not in VALID_TEMPLATES:
        tmpl = (
            app_config.DEFAULT_WORKSPACE_TEMPLATE
            if app_config.DEFAULT_WORKSPACE_TEMPLATE in VALID_TEMPLATES
            else "minimal"
        )
    refs_json = problem_refs_to_json(
        erdos_id=erdos_id,
        source_url=source_url,
        formal_lean_path=formal_lean_path,
        notes=external_notes,
    )
    campaign_id = db.create_campaign(
        prompt,
        workspace_root=app_config.WORKSPACE_ROOT,
        workspace_template=tmpl,
        problem_refs_json=refs_json,
    )
    ws_dir = str((Path(app_config.WORKSPACE_ROOT).resolve() / campaign_id))
    ensure_workspace(ws_dir, tmpl)
    targets = await decompose_prompt(prompt)
    db.add_targets(campaign_id, [t.description for t in targets])
    return RedirectResponse(f"/campaign/{campaign_id}", status_code=303)


class NewCampaignJSON(BaseModel):
    prompt: str = Field(min_length=1)
    workspace_template: str = Field(default="minimal")
    use_mathlib: bool = Field(
        default=False,
        description="If true, use the mathlib4 Lake template (same as the dashboard checkbox).",
    )
    erdos_id: str = Field(default="")
    source_url: str = Field(default="")
    formal_lean_path: str = Field(default="")
    notes: str = Field(default="")


@app.post("/api/campaign/start")
async def start_campaign_json(body: NewCampaignJSON):
    if body.use_mathlib:
        tmpl = "mathlib"
    else:
        tmpl = body.workspace_template.strip().lower()
    if tmpl not in VALID_TEMPLATES:
        tmpl = (
            app_config.DEFAULT_WORKSPACE_TEMPLATE
            if app_config.DEFAULT_WORKSPACE_TEMPLATE in VALID_TEMPLATES
            else "minimal"
        )
    refs_json = problem_refs_to_json(
        erdos_id=body.erdos_id,
        source_url=body.source_url,
        formal_lean_path=body.formal_lean_path,
        notes=body.notes,
    )
    campaign_id = db.create_campaign(
        body.prompt.strip(),
        workspace_root=app_config.WORKSPACE_ROOT,
        workspace_template=tmpl,
        problem_refs_json=refs_json,
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
            **_cartography_context(state),
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
