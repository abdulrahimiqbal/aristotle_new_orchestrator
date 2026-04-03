from __future__ import annotations

import asyncio
import json
import logging
from typing import Any
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
from orchestrator.experiment_dispatch import try_submit_experiment_now
from orchestrator.shadow_agent import (
    SHADOW_GLOBAL_GOAL_ID,
    run_shadow_global_lab,
    run_shadow_lab,
    shadow_global_loop,
)
from orchestrator.shadow_presenter import build_shadow_ui_context

logging.basicConfig(level=logging.INFO)

DATABASE_PATH = app_config.DATABASE_PATH

db = Database(DATABASE_PATH)

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _operator_runtime_context() -> dict:
    gate_kinds = sorted(app_config.MAP_PROVED_GATE_KINDS)
    return {
        "max_experiments": app_config.MAX_EXPERIMENTS,
        "max_active_experiments": app_config.MAX_ACTIVE_EXPERIMENTS,
        "tick_interval_sec": app_config.TICK_INTERVAL,
        "workspace_root": str(Path(app_config.WORKSPACE_ROOT).resolve()),
        "map_refresh_max_interval_ticks": app_config.MAP_REFRESH_MAX_INTERVAL_TICKS,
        "map_proved_gate_kinds": gate_kinds,
        "skeptic_pass_enabled": bool(app_config.SKEPTIC_PASS_ENABLED),
        "min_non_prove_moves": int(app_config.MANAGER_MIN_NON_PROVE_MOVES_PER_BATCH or 0),
        "verdict_reconcile_enabled": bool(app_config.VERDICT_RECONCILE_FROM_SUMMARY),
        "shadow_llm_model": app_config.SHADOW_LLM_MODEL or app_config.LLM_MODEL,
        "shadow_llm_temperature": app_config.SHADOW_LLM_TEMPERATURE,
        "shadow_global_goal": app_config.SHADOW_GLOBAL_GOAL,
        "manager_submit_pending": bool(app_config.MANAGER_SUBMIT_PENDING_EXPERIMENTS),
        "shadow_aristotle_immediate": bool(app_config.SHADOW_ARISTOTLE_IMMEDIATE_ON_APPROVE),
        "shadow_global_auto_enabled": bool(app_config.SHADOW_GLOBAL_AUTO_ENABLED),
        "shadow_global_tick_interval_sec": int(app_config.SHADOW_GLOBAL_TICK_INTERVAL_SEC),
        "shadow_global_pending_cap": int(app_config.SHADOW_GLOBAL_MAX_PENDING_PROMOTIONS),
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


def _shadow_panel_context(campaign_id: str, *, shadow_flash: dict | None = None) -> dict:
    db.ensure_shadow_state_row(campaign_id)
    campaign = db.get_campaign_state(campaign_id).campaign
    ep = db.get_shadow_epistemic_state(campaign_id)
    stance_raw = ep.get("stance_json") or "{}"
    try:
        stance_pretty = json.dumps(json.loads(stance_raw), indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        stance_pretty = stance_raw
    policy_raw = ep.get("policy_json") or "{}"
    try:
        policy_pretty = json.dumps(json.loads(policy_raw), indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        policy_pretty = policy_raw
    hyps = db.list_shadow_hypotheses(campaign_id, limit=60)
    hids = [str(h["id"]) for h in hyps]
    ev_rows = db.list_shadow_hypothesis_evidence(hids)
    ev_by_h: dict[str, list[dict]] = {}
    for r in ev_rows:
        hid = str(r["hypothesis_id"])
        ev_by_h.setdefault(hid, []).append(dict(r))
    for h in hyps:
        h["evidence_rows"] = ev_by_h.get(str(h["id"]), [])
    promos = db.list_shadow_promotion_requests(campaign_id, limit=40)
    runs = db.list_shadow_runs(campaign_id, limit=12)
    ui_ctx = build_shadow_ui_context(
        hypotheses=hyps,
        promotions=promos,
        runs=runs,
    )
    return {
        "selected": campaign_id,
        "shadow_campaign_prompt": campaign.prompt,
        "shadow_epistemic": ep,
        "shadow_stance_pretty": stance_pretty,
        "shadow_policy_pretty": policy_pretty,
        "shadow_hypotheses": hyps,
        "shadow_promotions": promos,
        "shadow_runs": runs,
        "shadow_flash": shadow_flash,
        "operator": _operator_runtime_context(),
        "public_view": False,
        "shadow_view": False,
        **ui_ctx,
    }


def _shadow_global_panel_context(*, shadow_flash: dict | None = None) -> dict:
    goal_id = SHADOW_GLOBAL_GOAL_ID
    goal_text = app_config.SHADOW_GLOBAL_GOAL
    db.ensure_shadow_global_state_row(goal_id, goal_text=goal_text)
    ep = db.get_shadow_global_state(goal_id)
    stance_raw = ep.get("stance_json") or "{}"
    try:
        stance_pretty = json.dumps(json.loads(stance_raw), indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        stance_pretty = stance_raw
    policy_raw = ep.get("policy_json") or "{}"
    try:
        policy_pretty = json.dumps(json.loads(policy_raw), indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        policy_pretty = policy_raw
    hyps = db.list_shadow_global_hypotheses(goal_id, limit=80)
    hids = [str(h["id"]) for h in hyps]
    ev_rows = db.list_shadow_global_hypothesis_evidence(hids)
    ev_by_h: dict[str, list[dict]] = {}
    for r in ev_rows:
        hid = str(r["hypothesis_id"])
        ev_by_h.setdefault(hid, []).append(dict(r))
    for h in hyps:
        h["evidence_rows"] = ev_by_h.get(str(h["id"]), [])
    promos = db.list_shadow_global_promotion_requests(goal_id, limit=80)
    runs = db.list_shadow_global_runs(goal_id, limit=20)
    ui_ctx = build_shadow_ui_context(
        hypotheses=hyps,
        promotions=promos,
        runs=runs,
    )
    return {
        "selected": None,
        "shadow_goal_id": goal_id,
        "shadow_goal_text": ep.get("goal_text") or goal_text,
        "shadow_epistemic": ep,
        "shadow_stance_pretty": stance_pretty,
        "shadow_policy_pretty": policy_pretty,
        "shadow_hypotheses": hyps,
        "shadow_promotions": promos,
        "shadow_runs": runs,
        "shadow_flash": shadow_flash,
        "operator": _operator_runtime_context(),
        "public_view": False,
        "shadow_view": True,
        "campaigns": db.get_all_campaigns(),
        **ui_ctx,
    }


async def _maybe_submit_shadow_promoted_experiment(
    payload_json: str | None, extra: dict[str, Any]
) -> dict[str, Any] | None:
    if not extra.get("experiment_id"):
        return None
    try:
        payload = json.loads(payload_json or "{}")
    except json.JSONDecodeError:
        payload = {}
    if payload.get("defer_aristotle_submit") is True:
        return {"ok": True, "skipped": True, "reason": "defer_aristotle_submit"}
    return await try_submit_experiment_now(db, str(extra["experiment_id"]))


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
    shadow_task = asyncio.create_task(shadow_global_loop(db))
    try:
        yield
    finally:
        shadow_task.cancel()
        task.cancel()
        try:
            await shadow_task
        except asyncio.CancelledError:
            pass
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
            "public_view": False,
            "shadow_view": False,
            "campaign_shadow_view": False,
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
            "public_view": False,
            "shadow_view": False,
            "campaign_shadow_view": False,
            **_cartography_context(state),
        },
    )


@app.get("/campaign/{campaign_id}/shadow", response_class=HTMLResponse)
async def campaign_shadow_detail(request: Request, campaign_id: str):
    campaigns = db.get_all_campaigns()
    if not db.campaign_exists(campaign_id):
        return RedirectResponse("/", status_code=303)
    ctx = _shadow_panel_context(campaign_id)
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "campaigns": campaigns,
            "selected": campaign_id,
            "state": None,
            "ticks": [],
            "progress": None,
            "operator": _operator_runtime_context(),
            "public_view": False,
            "shadow_view": False,
            "campaign_shadow_view": True,
            **ctx,
        },
    )


@app.get("/p/campaign/{campaign_id}", response_class=HTMLResponse)
async def public_campaign_detail(request: Request, campaign_id: str):
    """Read-only campaign view for sharing: no other campaigns listed, no new campaign, no pause/resume."""
    if not db.campaign_exists(campaign_id):
        return HTMLResponse("Unknown campaign", status_code=404)
    try:
        state = db.get_campaign_state(campaign_id)
    except ValueError:
        return HTMLResponse("Unknown campaign", status_code=404)
    campaigns = [c for c in db.get_all_campaigns() if c.id == campaign_id]
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
            "public_view": True,
            "shadow_view": False,
            "campaign_shadow_view": False,
            **_cartography_context(state),
        },
    )


@app.get("/shadow", response_class=HTMLResponse)
async def shadow_dashboard(request: Request):
    ctx = _shadow_global_panel_context()
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "campaigns": ctx["campaigns"],
            "selected": None,
            "state": None,
            "ticks": [],
            "progress": None,
            "operator": _operator_runtime_context(),
            "public_view": False,
            "shadow_view": True,
            "campaign_shadow_view": False,
            **ctx,
        },
    )


@app.post("/api/campaign")
async def start_campaign(
    prompt: str = Form(...),
    use_mathlib: str | None = Form(None),
    use_mathlib_knowledge: str | None = Form(None),
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
    mk = use_mathlib_knowledge is not None and str(use_mathlib_knowledge).strip().lower() in (
        "1",
        "on",
        "true",
        "yes",
    )
    campaign_id = db.create_campaign(
        prompt,
        workspace_root=app_config.WORKSPACE_ROOT,
        workspace_template=tmpl,
        problem_refs_json=refs_json,
        mathlib_knowledge=mk,
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
    use_mathlib_knowledge: bool = Field(
        default=False,
        description="If true, enable LeanSearch Mathlib hints for this campaign (requires MATHLIB_KNOWLEDGE_MODE=leansearch).",
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
        mathlib_knowledge=body.use_mathlib_knowledge,
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
            "mathlib_knowledge": body.use_mathlib_knowledge,
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
            "public_view": False,
            "shadow_view": False,
            **_cartography_context(state),
        },
    )


@app.get("/api/public/campaign/{campaign_id}/state", response_class=HTMLResponse)
async def public_campaign_state_fragment(request: Request, campaign_id: str):
    """HTMX fragment for /p/campaign/{id}: same panel as operator view minus workspace path and controls."""
    if not db.campaign_exists(campaign_id):
        return HTMLResponse("", status_code=404)
    try:
        state = db.get_campaign_state(campaign_id)
    except ValueError:
        return HTMLResponse("", status_code=404)
    ticks = _ticks_view(db.get_recent_ticks(campaign_id, limit=20))
    progress = _progress_stats(state)
    campaigns = [c for c in db.get_all_campaigns() if c.id == campaign_id]
    return templates.TemplateResponse(
        request,
        "campaign_panel.html",
        {
            "campaigns": campaigns,
            "selected": campaign_id,
            "state": state,
            "ticks": ticks,
            "progress": progress,
            "operator": _operator_runtime_context(),
            "public_view": True,
            "shadow_view": False,
            **_cartography_context(state),
        },
    )


@app.get("/api/shadow/panel", response_class=HTMLResponse)
async def shadow_global_panel_fragment(request: Request):
    return templates.TemplateResponse(
        request,
        "shadow_panel.html",
        _shadow_global_panel_context(),
    )


@app.get("/api/shadow/ops")
async def shadow_global_ops():
    snap = db.get_shadow_global_ops_snapshot(SHADOW_GLOBAL_GOAL_ID)
    return {
        "goal_id": SHADOW_GLOBAL_GOAL_ID,
        "goal_text": app_config.SHADOW_GLOBAL_GOAL,
        "auto_enabled": bool(app_config.SHADOW_GLOBAL_AUTO_ENABLED),
        "auto_interval_sec": int(app_config.SHADOW_GLOBAL_TICK_INTERVAL_SEC),
        "pending_cap": int(app_config.SHADOW_GLOBAL_MAX_PENDING_PROMOTIONS),
        "manager_submit_pending": bool(app_config.MANAGER_SUBMIT_PENDING_EXPERIMENTS),
        "shadow_aristotle_immediate": bool(app_config.SHADOW_ARISTOTLE_IMMEDIATE_ON_APPROVE),
        **snap,
    }


@app.post("/api/shadow/run", response_class=HTMLResponse)
async def shadow_global_run_fragment(request: Request):
    flash = await run_shadow_global_lab(
        db,
        goal_text=app_config.SHADOW_GLOBAL_GOAL,
        trigger_kind="manual",
    )
    return templates.TemplateResponse(
        request,
        "shadow_panel.html",
        _shadow_global_panel_context(shadow_flash=flash),
    )


@app.post("/api/shadow/promote/{promotion_id}/approve")
async def shadow_global_promote_approve(request: Request, promotion_id: str):
    row = db.get_shadow_global_promotion_request(promotion_id)
    if not row:
        return HTMLResponse("Unknown promotion", status_code=404)
    payload_json = row.get("payload_json")
    ok, msg, extra = db.apply_shadow_global_promotion(promotion_id)
    flash: dict[str, Any] = {"ok": ok, "error": None if ok else msg, "promotion": "approved"}
    if ok:
        ar = await _maybe_submit_shadow_promoted_experiment(
            str(payload_json) if payload_json is not None else None, extra
        )
        if ar is not None:
            flash["aristotle_submit"] = ar
    return templates.TemplateResponse(
        request,
        "shadow_panel.html",
        _shadow_global_panel_context(shadow_flash=flash),
    )


@app.post("/api/shadow/promote/{promotion_id}/reject")
async def shadow_global_promote_reject(request: Request, promotion_id: str):
    row = db.get_shadow_global_promotion_request(promotion_id)
    if not row:
        return HTMLResponse("Unknown promotion", status_code=404)
    db.reject_shadow_global_promotion(promotion_id)
    flash = {"ok": True, "promotion": "rejected"}
    return templates.TemplateResponse(
        request,
        "shadow_panel.html",
        _shadow_global_panel_context(shadow_flash=flash),
    )


@app.get("/api/campaign/{campaign_id}/shadow/panel", response_class=HTMLResponse)
async def shadow_panel_fragment(request: Request, campaign_id: str):
    if not db.campaign_exists(campaign_id):
        return HTMLResponse("", status_code=404)
    return templates.TemplateResponse(
        request,
        "shadow_panel.html",
        _shadow_panel_context(campaign_id),
    )


@app.post("/api/campaign/{campaign_id}/shadow/run", response_class=HTMLResponse)
async def shadow_run_fragment(request: Request, campaign_id: str):
    if not db.campaign_exists(campaign_id):
        return HTMLResponse("", status_code=404)
    flash = await run_shadow_lab(db, campaign_id, trigger_kind="manual")
    return templates.TemplateResponse(
        request,
        "shadow_panel.html",
        _shadow_panel_context(campaign_id, shadow_flash=flash),
    )


@app.post("/api/campaign/{campaign_id}/shadow/promote/{promotion_id}/approve")
async def shadow_promote_approve(
    request: Request, campaign_id: str, promotion_id: str
):
    if not db.campaign_exists(campaign_id):
        return HTMLResponse("Unknown campaign", status_code=404)
    row = db.get_shadow_promotion_request(promotion_id)
    if not row or row["campaign_id"] != campaign_id:
        return HTMLResponse("Unknown promotion", status_code=404)
    payload_json = row.get("payload_json")
    ok, msg, extra = db.apply_shadow_promotion(promotion_id)
    flash: dict[str, Any] = {"ok": ok, "error": None if ok else msg, "promotion": "approved"}
    if ok:
        ar = await _maybe_submit_shadow_promoted_experiment(
            str(payload_json) if payload_json is not None else None, extra
        )
        if ar is not None:
            flash["aristotle_submit"] = ar
    return templates.TemplateResponse(
        request,
        "shadow_panel.html",
        _shadow_panel_context(campaign_id, shadow_flash=flash),
    )


@app.post("/api/campaign/{campaign_id}/shadow/promote/{promotion_id}/reject")
async def shadow_promote_reject(
    request: Request, campaign_id: str, promotion_id: str
):
    if not db.campaign_exists(campaign_id):
        return HTMLResponse("Unknown campaign", status_code=404)
    row = db.get_shadow_promotion_request(promotion_id)
    if not row or row["campaign_id"] != campaign_id:
        return HTMLResponse("Unknown promotion", status_code=404)
    db.reject_shadow_promotion(promotion_id)
    flash = {"ok": True, "promotion": "rejected"}
    return templates.TemplateResponse(
        request,
        "shadow_panel.html",
        _shadow_panel_context(campaign_id, shadow_flash=flash),
    )


@app.post("/api/campaign/{campaign_id}/pause")
async def pause_campaign(campaign_id: str):
    db.update_campaign_status(campaign_id, CampaignStatus.PAUSED.value)
    return RedirectResponse(f"/campaign/{campaign_id}", status_code=303)


@app.post("/api/campaign/{campaign_id}/resume")
async def resume_campaign(campaign_id: str):
    db.update_campaign_status(campaign_id, CampaignStatus.ACTIVE.value)
    return RedirectResponse(f"/campaign/{campaign_id}", status_code=303)
