from __future__ import annotations

import asyncio
import json
import logging
from typing import Annotated, Any
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, Form, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from fastapi.templating import Jinja2Templates

from orchestrator import config as app_config
from orchestrator.admin_routes import build_admin_router
from orchestrator.db import Database
from orchestrator.lima_agent import lima_loop, run_lima
from orchestrator.lima_db import LimaDatabase
from orchestrator.lima_literature import refresh_literature
from orchestrator.lima_models import slugify
from orchestrator.lima_obligations import (
    AristotleFormalBackend,
    approve_formal_review_async,
    archive_obligation,
    lima_aristotle_budget,
    queue_formal_review,
    reject_formal_review,
    rerun_local_obligation,
    run_queued_obligation_checks,
)
from orchestrator.lima_presenter import build_lima_ui_context
from orchestrator.llm import decompose_prompt
from orchestrator.manager import manager_loop
from orchestrator.models import CampaignStatus, TargetStatus
from orchestrator.problem_map_util import (
    map_progress_stats,
    parse_problem_map,
    parse_problem_refs,
    problem_refs_to_json,
)
from orchestrator.research_packets import (
    parse_research_packet,
    select_attack_families,
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
from orchestrator.supershadow_agent import (
    SUPERSHADOW_GLOBAL_GOAL_ID,
    run_supershadow_global_lab,
    supershadow_global_loop,
)
from orchestrator.supershadow_presenter import build_supershadow_ui_context

logging.basicConfig(level=logging.INFO)

DATABASE_PATH = app_config.DATABASE_PATH

db = Database(DATABASE_PATH)
lima_db = LimaDatabase(app_config.LIMA_DATABASE_PATH, reference_database_path=DATABASE_PATH)

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _require_operator_write(
    authorization: Annotated[str | None, Header()] = None,
    x_admin_token: Annotated[str | None, Header(alias="X-Admin-Token")] = None,
    admin_token: Annotated[str | None, Query()] = None,
) -> bool:
    """Protect write routes when ADMIN_TOKEN is configured.

    The existing dashboard supports local/dev deployments without ADMIN_TOKEN.
    In protected deployments, Lima writes use the same bearer/X-Admin-Token
    pattern as admin routes.
    """

    if not app_config.ADMIN_TOKEN:
        return True
    bearer: str | None = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer = authorization[7:].strip()
    got = (bearer or (x_admin_token or "").strip() or (admin_token or "").strip())
    if got != app_config.ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True


OperatorWriteAuth = Annotated[bool, Depends(_require_operator_write)]


def _is_htmx_request(request: Request) -> bool:
    return request.headers.get("HX-Request", "").strip().lower() == "true"


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
        "supershadow_llm_model": (
            app_config.SUPERSHADOW_LLM_MODEL
            or app_config.SHADOW_LLM_MODEL
            or app_config.LLM_MODEL
        ),
        "supershadow_llm_temperature": app_config.SUPERSHADOW_LLM_TEMPERATURE,
        "supershadow_global_goal": app_config.SUPERSHADOW_GLOBAL_GOAL,
        "supershadow_global_auto_enabled": bool(app_config.SUPERSHADOW_GLOBAL_AUTO_ENABLED),
        "supershadow_global_tick_interval_sec": int(
            app_config.SUPERSHADOW_GLOBAL_TICK_INTERVAL_SEC
        ),
        "supershadow_max_handoffs_per_run": int(
            app_config.SUPERSHADOW_MAX_HANDOFFS_PER_RUN
        ),
        "supershadow_max_pending_handoffs": int(
            app_config.SUPERSHADOW_MAX_PENDING_HANDOFFS
        ),
        "lima_enabled": True,
        "lima_database_path": app_config.LIMA_DATABASE_PATH,
        "lima_loop_interval_sec": int(app_config.LIMA_LOOP_INTERVAL_SEC),
        "lima_default_problem": app_config.LIMA_DEFAULT_PROBLEM,
        "lima_default_mode": app_config.LIMA_DEFAULT_MODE,
        "lima_max_universes_per_run": int(app_config.LIMA_MAX_UNIVERSES_PER_RUN),
        "lima_max_obligations_per_run": int(app_config.LIMA_MAX_OBLIGATIONS_PER_RUN),
        "lima_max_literature_results": int(app_config.LIMA_MAX_LITERATURE_RESULTS),
        "lima_auto_policy_updates": bool(app_config.LIMA_ENABLE_AUTO_POLICY_UPDATES),
        "lima_literature_backends": app_config.LIMA_LITERATURE_BACKENDS,
        "lima_literature_local_dir": app_config.LIMA_LITERATURE_LOCALFILE_DIR,
        "lima_formal_backend": app_config.LIMA_FORMAL_BACKEND,
        "lima_formal_auto_submit": bool(app_config.LIMA_FORMAL_AUTO_SUBMIT),
        "lima_aristotle_auto_submit": bool(app_config.LIMA_ARISTOTLE_AUTO_SUBMIT),
        "lima_aristotle_max_active": int(app_config.LIMA_ARISTOTLE_MAX_ACTIVE),
        "lima_aristotle_max_daily_submissions": int(
            app_config.LIMA_ARISTOTLE_MAX_DAILY_SUBMISSIONS
        ),
        "lima_aristotle_campaign_slug": app_config.LIMA_ARISTOTLE_CAMPAIGN_SLUG,
        "lima_aristotle_threshold": app_config.LIMA_ARISTOTLE_THRESHOLD,
        "lima_aristotle_budget": lima_aristotle_budget(db),
        "lima_auto_local_obligation_checks": bool(app_config.LIMA_AUTO_LOCAL_OBLIGATION_CHECKS),
    }


def _cartography_context(state) -> dict:
    pm = parse_problem_map(state.campaign.problem_map_json)
    refs = parse_problem_refs(state.campaign.problem_refs_json)
    packet = parse_research_packet(state.campaign.research_packet_json)
    pretty = json.dumps(pm, indent=2, ensure_ascii=False) if pm else "{}"
    packet_pretty = json.dumps(packet, indent=2, ensure_ascii=False) if packet else "{}"
    fronts = pm.get("active_fronts") if isinstance(pm, dict) else []
    if not isinstance(fronts, list):
        fronts = []
    return {
        "problem_map": pm,
        "problem_refs": refs,
        "research_packet": packet,
        "research_packet_attack_families": select_attack_families(packet, fronts, limit=4),
        "research_packet_json_pretty": packet_pretty,
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


def _supershadow_global_panel_context(
    *, supershadow_flash: dict | None = None
) -> dict:
    goal_id = SUPERSHADOW_GLOBAL_GOAL_ID
    goal_text = app_config.SUPERSHADOW_GLOBAL_GOAL
    db.ensure_supershadow_state_row(goal_id, goal_text=goal_text)
    ep = db.get_supershadow_state(goal_id)
    worldview_raw = ep.get("worldview_json") or "{}"
    try:
        worldview_pretty = json.dumps(
            json.loads(worldview_raw), indent=2, ensure_ascii=False
        )
    except json.JSONDecodeError:
        worldview_pretty = worldview_raw
    policy_raw = ep.get("policy_json") or "{}"
    try:
        policy_pretty = json.dumps(json.loads(policy_raw), indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        policy_pretty = policy_raw
    concepts = db.list_supershadow_concepts(goal_id, limit=80)
    concept_ids = [str(concept["id"]) for concept in concepts]
    fact_rows = db.list_supershadow_fact_links(concept_ids)
    tension_rows = db.list_supershadow_tensions(concept_ids)
    kill_rows = db.list_supershadow_kill_tests(concept_ids)
    facts_by_concept: dict[str, list[dict[str, Any]]] = {}
    tensions_by_concept: dict[str, list[dict[str, Any]]] = {}
    kills_by_concept: dict[str, list[dict[str, Any]]] = {}
    for row in fact_rows:
        facts_by_concept.setdefault(str(row["concept_id"]), []).append(dict(row))
    for row in tension_rows:
        tensions_by_concept.setdefault(str(row["concept_id"]), []).append(dict(row))
    for row in kill_rows:
        kills_by_concept.setdefault(str(row["concept_id"]), []).append(dict(row))
    for concept in concepts:
        cid = str(concept["id"])
        concept["fact_links"] = facts_by_concept.get(cid, [])
        concept["tensions"] = tensions_by_concept.get(cid, [])
        concept["kill_tests"] = kills_by_concept.get(cid, [])
    handoffs = db.list_supershadow_handoff_requests(goal_id, limit=80)
    incubations = db.list_supershadow_incubations(goal_id, limit=80)
    incubation_ids = [str(row["id"]) for row in incubations]
    incubation_events = db.list_supershadow_incubation_events(incubation_ids)
    events_by_incubation: dict[str, list[dict[str, Any]]] = {}
    for row in incubation_events:
        events_by_incubation.setdefault(str(row["incubation_id"]), []).append(dict(row))
    for incubation in incubations:
        incubation["events"] = events_by_incubation.get(str(incubation["id"]), [])
    runs = db.list_supershadow_runs(goal_id, limit=20)
    ui_ctx = build_supershadow_ui_context(
        concepts=concepts,
        handoffs=handoffs,
        incubations=incubations,
        runs=runs,
    )
    return {
        "selected": None,
        "supershadow_goal_id": goal_id,
        "supershadow_goal_text": ep.get("goal_text") or goal_text,
        "supershadow_epistemic": ep,
        "supershadow_worldview_pretty": worldview_pretty,
        "supershadow_policy_pretty": policy_pretty,
        "supershadow_concepts": concepts,
        "supershadow_handoffs": handoffs,
        "supershadow_incubations": incubations,
        "supershadow_runs": runs,
        "supershadow_flash": supershadow_flash,
        "operator": _operator_runtime_context(),
        "public_view": False,
        "shadow_view": False,
        "supershadow_view": True,
        "campaigns": db.get_all_campaigns(),
        **ui_ctx,
    }


def _lima_panel_context(
    *,
    problem: str | None = None,
    lima_flash: dict | None = None,
) -> dict:
    lima_db.initialize()
    snapshot = lima_db.get_dashboard_snapshot(problem or app_config.LIMA_DEFAULT_PROBLEM)
    ui_ctx = build_lima_ui_context(snapshot, lima_flash=lima_flash)
    return {
        "selected": None,
        "operator": _operator_runtime_context(),
        "public_view": False,
        "shadow_view": False,
        "supershadow_view": False,
        "lima_view": True,
        "campaigns": db.get_all_campaigns(),
        **ui_ctx,
    }


def _lima_index_context(*, lima_flash: dict | None = None) -> dict:
    lima_db.initialize()
    problems = lima_db.list_problems()
    cards: list[dict[str, Any]] = []
    totals = {
        "problem_count": len(problems),
        "active_count": 0,
        "pending_handoffs": 0,
        "queued_obligations": 0,
        "escalated_reviews": 0,
    }
    lima_modes: list[dict[str, str]] = []

    for problem in problems:
        snapshot = lima_db.get_dashboard_snapshot(str(problem["id"]))
        ui_ctx = build_lima_ui_context(snapshot)
        metrics = dict(ui_ctx.get("lima_metrics") or {})
        top_candidate = dict(ui_ctx.get("lima_top_candidate") or {})
        top_blocker = dict(ui_ctx.get("lima_top_blocker") or {})
        decision_state = dict(ui_ctx.get("lima_decision_state") or {})
        latest_run = dict(ui_ctx.get("lima_latest_run") or {}) if ui_ctx.get("lima_latest_run") else None
        if not lima_modes:
            lima_modes = list(ui_ctx.get("lima_modes") or [])
        if str(problem.get("status") or "active") == "active":
            totals["active_count"] += 1
        totals["pending_handoffs"] += int(metrics.get("pending_handoffs") or 0)
        totals["queued_obligations"] += int(metrics.get("queued_obligations") or 0)
        totals["escalated_reviews"] += int(metrics.get("steward_escalated") or 0)
        cards.append(
            {
                "problem": problem,
                "href": f"/lima/{problem.get('slug')}",
                "latest_run": latest_run,
                "latest_summary": str(ui_ctx.get("lima_latest_summary") or ""),
                "now_summary": str(ui_ctx.get("lima_now_summary") or ""),
                "metrics": metrics,
                "decision_state": decision_state,
                "top_candidate": top_candidate,
                "top_blocker": top_blocker,
            }
        )

    cards.sort(
        key=lambda card: (
            0 if str(card["problem"].get("status") or "") == "active" else 1,
            -int(card["metrics"].get("steward_escalated") or 0),
            -int(card["metrics"].get("pending_handoffs") or 0),
            str((card.get("latest_run") or {}).get("created_at") or card["problem"].get("updated_at") or ""),
        ),
        reverse=False,
    )

    return {
        "selected": None,
        "operator": _operator_runtime_context(),
        "public_view": False,
        "shadow_view": False,
        "supershadow_view": False,
        "lima_view": False,
        "lima_index_view": True,
        "campaigns": db.get_all_campaigns(),
        "lima_flash": lima_flash,
        "lima_index_cards": cards,
        "lima_index_totals": totals,
        "lima_modes": lima_modes
        or [
            {"value": "balanced", "label": "Balanced", "hint": "general search pass"},
            {"value": "wild", "label": "Wild", "hint": "broader invention"},
            {"value": "stress", "label": "Stress", "hint": "break candidates harder"},
            {"value": "forge", "label": "Forge", "hint": "push toward formal obligations"},
        ],
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
    lima_db.initialize()
    Path(app_config.WORKSPACE_ROOT).mkdir(parents=True, exist_ok=True)
    migrate_legacy_shared_workspaces(
        db,
        workspace_root=app_config.WORKSPACE_ROOT,
        legacy_dir=app_config.WORKSPACE_LEGACY_DIR or None,
    )
    task = asyncio.create_task(manager_loop(db))
    shadow_task = asyncio.create_task(shadow_global_loop(db))
    supershadow_task = asyncio.create_task(supershadow_global_loop(db))
    lima_task = asyncio.create_task(lima_loop(lima_db, db))
    try:
        yield
    finally:
        lima_task.cancel()
        supershadow_task.cancel()
        shadow_task.cancel()
        task.cancel()
        try:
            await lima_task
        except asyncio.CancelledError:
            pass
        try:
            await supershadow_task
        except asyncio.CancelledError:
            pass
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
            "supershadow_view": False,
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
            "supershadow_view": False,
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
            "supershadow_view": False,
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
            "supershadow_view": False,
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
            "supershadow_view": False,
            "campaign_shadow_view": False,
            **ctx,
        },
    )


@app.get("/supershadow", response_class=HTMLResponse)
async def supershadow_dashboard(request: Request):
    ctx = _supershadow_global_panel_context()
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
            "shadow_view": False,
            "supershadow_view": True,
            "campaign_shadow_view": False,
            **ctx,
        },
    )


@app.get("/lima", response_class=HTMLResponse)
async def lima_dashboard(request: Request):
    ctx = _lima_index_context()
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
            "shadow_view": False,
            "supershadow_view": False,
            "lima_view": False,
            "lima_index_view": True,
            "campaign_shadow_view": False,
            **ctx,
        },
    )


@app.get("/lima/{problem_slug}", response_class=HTMLResponse)
async def lima_problem_dashboard(request: Request, problem_slug: str):
    ctx = _lima_panel_context(problem=problem_slug)
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
            "shadow_view": False,
            "supershadow_view": False,
            "lima_view": True,
            "lima_index_view": False,
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
    research_packet_json: str = Form(""),
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
        research_packet_json=research_packet_json,
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
    research_packet_json: str = Field(default="")


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
        research_packet_json=body.research_packet_json,
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


class ResearchPacketUpdateJSON(BaseModel):
    research_packet_json: str = Field(default="")
    research_packet: dict[str, Any] | None = None


@app.get("/api/campaign/{campaign_id}/ledger")
async def campaign_ledger_json(campaign_id: str, limit: int = 200):
    """Read-only lemma / obligation ledger rows for dashboards or tooling."""
    if not db.campaign_exists(campaign_id):
        return JSONResponse({"error": "unknown campaign"}, status_code=404)
    rows = db.get_recent_ledger_entries(campaign_id, min(limit, 2000))
    return {"campaign_id": campaign_id, "entries": rows}


@app.post("/api/campaign/{campaign_id}/research-packet")
async def update_campaign_research_packet(request: Request, campaign_id: str):
    if not db.campaign_exists(campaign_id):
        return JSONResponse({"error": "unknown campaign"}, status_code=404)

    content_type = request.headers.get("content-type", "").lower()
    if "application/json" in content_type:
        try:
            raw_body = await request.json()
        except json.JSONDecodeError:
            return JSONResponse({"error": "invalid json"}, status_code=400)
        if not isinstance(raw_body, dict):
            return JSONResponse({"error": "expected object body"}, status_code=400)
        body = ResearchPacketUpdateJSON.model_validate(raw_body)
        raw = body.research_packet_json
        if body.research_packet is not None:
            raw = json.dumps(body.research_packet, ensure_ascii=False)
        db.update_campaign_research_packet(campaign_id, raw)
        packet = parse_research_packet(db.get_campaign_state(campaign_id).campaign.research_packet_json)
        return {"campaign_id": campaign_id, "research_packet": packet}

    form = await request.form()
    raw = str(form.get("research_packet_json") or "")
    db.update_campaign_research_packet(campaign_id, raw)
    return RedirectResponse(f"/campaign/{campaign_id}", status_code=303)


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
            "supershadow_view": False,
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
            "supershadow_view": False,
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


@app.get("/api/supershadow/panel", response_class=HTMLResponse)
async def supershadow_global_panel_fragment(request: Request):
    return templates.TemplateResponse(
        request,
        "supershadow_panel.html",
        _supershadow_global_panel_context(),
    )


@app.get("/api/lima/panel", response_class=HTMLResponse)
async def lima_panel_fragment(request: Request, problem: str | None = None):
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=problem),
    )


@app.get("/api/lima/ops")
async def lima_ops(problem: str | None = None):
    lima_db.initialize()
    snapshot = lima_db.get_dashboard_snapshot(problem or app_config.LIMA_DEFAULT_PROBLEM)
    metrics = build_lima_ui_context(snapshot).get("lima_metrics", {})
    return {
        "problem": snapshot.get("problem"),
        "enabled": True,
        "auto_interval_sec": int(app_config.LIMA_LOOP_INTERVAL_SEC),
        "database_path": app_config.LIMA_DATABASE_PATH,
        "default_mode": app_config.LIMA_DEFAULT_MODE,
        "benchmark_locked": bool(app_config.LIMA_BENCHMARK_LOCKED),
        "family_governance_frozen": bool(app_config.LIMA_FREEZE_FAMILY_GOVERNANCE),
        "zero_live_authority": not bool(app_config.LIMA_ARISTOTLE_AUTO_SUBMIT),
        "zero_live_authority_default": True,
        "live_aristotle_submission_enabled": bool(app_config.LIMA_ARISTOTLE_AUTO_SUBMIT),
        "aristotle_auto_submit": bool(app_config.LIMA_ARISTOTLE_AUTO_SUBMIT),
        "aristotle_budget": lima_aristotle_budget(db),
        "aristotle_threshold": app_config.LIMA_ARISTOTLE_THRESHOLD,
        "aristotle_campaign_slug": app_config.LIMA_ARISTOTLE_CAMPAIGN_SLUG,
        **metrics,
    }


@app.get("/api/supershadow/ops")
async def supershadow_global_ops():
    snap = db.get_supershadow_ops_snapshot(SUPERSHADOW_GLOBAL_GOAL_ID)
    return {
        "goal_id": SUPERSHADOW_GLOBAL_GOAL_ID,
        "goal_text": app_config.SUPERSHADOW_GLOBAL_GOAL,
        "auto_enabled": bool(app_config.SUPERSHADOW_GLOBAL_AUTO_ENABLED),
        "auto_interval_sec": int(app_config.SUPERSHADOW_GLOBAL_TICK_INTERVAL_SEC),
        "handoff_cap": int(app_config.SUPERSHADOW_MAX_HANDOFFS_PER_RUN),
        "pending_cap": int(app_config.SUPERSHADOW_MAX_PENDING_HANDOFFS),
        **snap,
    }


@app.post("/api/supershadow/run", response_class=HTMLResponse)
async def supershadow_global_run_fragment(request: Request):
    flash = await run_supershadow_global_lab(
        db,
        goal_text=app_config.SUPERSHADOW_GLOBAL_GOAL,
        trigger_kind="manual",
    )
    return templates.TemplateResponse(
        request,
        "supershadow_panel.html",
        _supershadow_global_panel_context(supershadow_flash=flash),
    )


@app.post("/api/lima/run", response_class=HTMLResponse)
async def lima_run_fragment(
    request: Request,
    _auth: OperatorWriteAuth,
    problem_slug: str = Form(""),
    mode: str = Form(""),
):
    selected_problem = (problem_slug or app_config.LIMA_DEFAULT_PROBLEM).strip()
    flash = await run_lima(
        lima_db,
        db,
        problem_slug=selected_problem,
        trigger_kind="manual",
        mode=mode or app_config.LIMA_DEFAULT_MODE,
    )
    if not _is_htmx_request(request):
        return RedirectResponse(f"/lima/{selected_problem}", status_code=303)
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=selected_problem, lima_flash=flash),
    )


@app.post("/api/lima/problem", response_class=HTMLResponse)
async def lima_problem_create_fragment(
    request: Request,
    _auth: OperatorWriteAuth,
    slug: str = Form(""),
    title: str = Form(...),
    statement_md: str = Form(...),
    domain: str = Form("number_theory"),
    default_goal_text: str = Form(""),
    seed_packet_json: str = Form(""),
):
    raw_seed = (seed_packet_json or "").strip()
    seed_payload: dict[str, Any] = {}
    if raw_seed:
        try:
            parsed = json.loads(raw_seed)
            seed_payload = parsed if isinstance(parsed, dict) else {"notes": raw_seed}
        except json.JSONDecodeError:
            seed_payload = {"notes": raw_seed}
    if not default_goal_text.strip():
        default_goal_text = (
            "Find falsification-first conceptual universes for this problem, "
            "compile claims and obligations, and preserve fracture memory."
        )
    problem_id, created = lima_db.create_problem(
        slug=slug or title,
        title=title.strip(),
        statement_md=statement_md.strip(),
        domain=domain.strip() or "unspecified",
        default_goal_text=default_goal_text.strip(),
        seed_packet_json=seed_payload,
    )
    problem = lima_db.get_problem(problem_id)
    flash = {
        "ok": True,
        "problem": "created" if created else "updated",
        "problem_title": problem.get("title"),
    }
    if not _is_htmx_request(request):
        return RedirectResponse(f"/lima/{problem.get('slug') or problem_id}", status_code=303)
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=str(problem.get("slug") or problem_id), lima_flash=flash),
    )


@app.post("/api/lima/problem/{problem_slug}/pause", response_class=HTMLResponse)
async def lima_problem_pause_fragment(
    request: Request,
    problem_slug: str,
    _auth: OperatorWriteAuth,
):
    problem = lima_db.update_problem_status(problem_slug, status="paused")
    flash = {
        "ok": True,
        "problem": "paused",
        "problem_title": problem.get("title"),
    }
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=str(problem.get("slug") or problem_slug), lima_flash=flash),
    )


@app.post("/api/lima/problem/{problem_slug}/resume", response_class=HTMLResponse)
async def lima_problem_resume_fragment(
    request: Request,
    problem_slug: str,
    _auth: OperatorWriteAuth,
):
    problem = lima_db.update_problem_status(problem_slug, status="active")
    flash = {
        "ok": True,
        "problem": "resumed",
        "problem_title": problem.get("title"),
    }
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=str(problem.get("slug") or problem_slug), lima_flash=flash),
    )


@app.post("/api/lima/start", response_class=HTMLResponse)
async def lima_start_from_prompt_fragment(
    request: Request,
    _auth: OperatorWriteAuth,
    prompt: str = Form(...),
    title: str = Form(""),
    slug: str = Form(""),
    domain: str = Form("unspecified"),
    mode: str = Form(""),
    run_now: str = Form(""),
):
    prompt_text = prompt.strip()
    if not prompt_text:
        flash = {"ok": False, "error": "prompt_required"}
        return templates.TemplateResponse(
            request,
            "lima_panel.html",
            _lima_panel_context(lima_flash=flash),
        )
    first_line = next((line.strip() for line in prompt_text.splitlines() if line.strip()), "New Lima problem")
    problem_title = (title.strip() or first_line)[:120]
    problem_slug = slugify(slug or problem_title, fallback="lima_problem")
    seed_payload = {
        "operator_prompt": prompt_text,
        "known_frontier": [],
        "routing_policy": {
            "retrieval_keywords": [problem_title],
            "campaign_tags": [problem_slug],
            "literature_defaults": ["local", "local_file"],
        },
    }
    default_goal_text = (
        "Treat the operator prompt as the visible problem packet. Invent candidate ontologies, "
        "attack repeated bad families, compile proof-oriented obligations, and preserve scoped fracture memory."
    )
    problem_id, created = lima_db.create_problem(
        slug=problem_slug,
        title=problem_title,
        statement_md=prompt_text,
        domain=domain.strip() or "unspecified",
        default_goal_text=default_goal_text,
        seed_packet_json=seed_payload,
    )
    problem_row = lima_db.get_problem(problem_id)
    selected_problem = str(problem_row.get("slug") or problem_id)
    flash: dict[str, Any] = {
        "ok": True,
        "problem": "created" if created else "updated",
        "problem_title": problem_row.get("title"),
    }
    if run_now:
        run_flash = await run_lima(
            lima_db,
            db,
            problem_slug=selected_problem,
            trigger_kind="manual",
            mode=mode or app_config.LIMA_DEFAULT_MODE,
        )
        flash = {**run_flash, "problem": flash["problem"], "problem_title": flash["problem_title"]}
    if not _is_htmx_request(request):
        return RedirectResponse(f"/lima/{selected_problem}", status_code=303)
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=selected_problem, lima_flash=flash),
    )


@app.post("/api/lima/literature/refresh", response_class=HTMLResponse)
async def lima_literature_refresh_fragment(
    request: Request,
    _auth: OperatorWriteAuth,
    problem_slug: str = Form(""),
    backend_selection: str = Form("configured"),
):
    selected_problem = (problem_slug or app_config.LIMA_DEFAULT_PROBLEM).strip()
    lima_db.initialize()
    problem = lima_db.get_problem(selected_problem)
    state = lima_db.get_state(str(problem["id"]))
    pressure = json.loads(state.get("pressure_map_json") or "{}")
    res = refresh_literature(
        lima_db,
        problem=problem,
        pressure_map=pressure,
        backend_selection=backend_selection or "configured",
    )
    flash = {"ok": True, "literature_refresh": res}
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=selected_problem, lima_flash=flash),
    )


@app.post("/api/lima/obligations/run", response_class=HTMLResponse)
async def lima_obligations_run_fragment(
    request: Request,
    _auth: OperatorWriteAuth,
    problem_slug: str = Form(""),
):
    selected_problem = (problem_slug or app_config.LIMA_DEFAULT_PROBLEM).strip()
    lima_db.initialize()
    problem = lima_db.get_problem(selected_problem)
    result = run_queued_obligation_checks(lima_db, problem_id=str(problem["id"]))
    flash = {"ok": True, "obligation_checks": result}
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=selected_problem, lima_flash=flash),
    )


@app.post("/api/lima/obligation/{obligation_id}/formal-review", response_class=HTMLResponse)
@app.post("/api/lima/obligation/{obligation_id}/queue-formal-review", response_class=HTMLResponse)
async def lima_obligation_formal_review_fragment(
    request: Request, obligation_id: str, _auth: OperatorWriteAuth
):
    result = queue_formal_review(lima_db, obligation_id=obligation_id, main_db=db)
    row = lima_db.get_obligation(obligation_id)
    problem_id = str((row or {}).get("problem_id") or app_config.LIMA_DEFAULT_PROBLEM)
    flash = {"ok": bool(result.get("ok")), "formal_review": result, "error": result.get("error")}
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=problem_id, lima_flash=flash),
    )


@app.post("/api/lima/obligation/{obligation_id}/approve-formal", response_class=HTMLResponse)
async def lima_obligation_approve_formal_fragment(
    request: Request, obligation_id: str, _auth: OperatorWriteAuth
):
    result = await approve_formal_review_async(lima_db, obligation_id=obligation_id, main_db=db)
    row = lima_db.get_obligation(obligation_id)
    problem_id = str((row or {}).get("problem_id") or app_config.LIMA_DEFAULT_PROBLEM)
    flash = {"ok": bool(result.get("ok")), "formal_review": result, "error": result.get("error")}
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=problem_id, lima_flash=flash),
    )


@app.post("/api/lima/obligation/{obligation_id}/submit-aristotle", response_class=HTMLResponse)
async def lima_obligation_submit_aristotle_fragment(
    request: Request, obligation_id: str, _auth: OperatorWriteAuth
):
    backend = AristotleFormalBackend(lima_db=lima_db, main_db=db, force=True)
    result = await approve_formal_review_async(
        lima_db,
        obligation_id=obligation_id,
        backend=backend,
        main_db=db,
        force_aristotle=True,
    )
    row = lima_db.get_obligation(obligation_id)
    problem_id = str((row or {}).get("problem_id") or app_config.LIMA_DEFAULT_PROBLEM)
    flash = {
        "ok": bool(result.get("ok")),
        "formal_review": result,
        "error": result.get("error"),
    }
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=problem_id, lima_flash=flash),
    )


@app.post("/api/lima/obligation/{obligation_id}/reject-formal", response_class=HTMLResponse)
async def lima_obligation_reject_formal_fragment(
    request: Request, obligation_id: str, _auth: OperatorWriteAuth
):
    result = reject_formal_review(lima_db, obligation_id=obligation_id)
    row = lima_db.get_obligation(obligation_id)
    problem_id = str((row or {}).get("problem_id") or app_config.LIMA_DEFAULT_PROBLEM)
    flash = {"ok": bool(result.get("ok")), "formal_review": result, "error": result.get("error")}
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=problem_id, lima_flash=flash),
    )


@app.post("/api/lima/obligation/{obligation_id}/rerun-local", response_class=HTMLResponse)
async def lima_obligation_rerun_local_fragment(
    request: Request, obligation_id: str, _auth: OperatorWriteAuth
):
    result = rerun_local_obligation(lima_db, obligation_id=obligation_id)
    row = lima_db.get_obligation(obligation_id)
    problem_id = str((row or {}).get("problem_id") or app_config.LIMA_DEFAULT_PROBLEM)
    flash = {"ok": bool(result.get("ok")), "obligation_checks": result, "error": result.get("error")}
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=problem_id, lima_flash=flash),
    )


@app.post("/api/lima/obligation/{obligation_id}/archive", response_class=HTMLResponse)
async def lima_obligation_archive_fragment(
    request: Request, obligation_id: str, _auth: OperatorWriteAuth
):
    result = archive_obligation(lima_db, obligation_id=obligation_id)
    row = lima_db.get_obligation(obligation_id)
    problem_id = str((row or {}).get("problem_id") or app_config.LIMA_DEFAULT_PROBLEM)
    flash = {"ok": bool(result.get("ok")), "formal_review": result, "error": result.get("error")}
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=problem_id, lima_flash=flash),
    )


@app.post("/api/lima/handoff/{handoff_id}/approve", response_class=HTMLResponse)
async def lima_handoff_approve(
    request: Request, handoff_id: str, _auth: OperatorWriteAuth
):
    row = lima_db.get_handoff(handoff_id)
    if not row:
        return HTMLResponse("Unknown Lima handoff", status_code=404)
    problem_id = str(row.get("problem_id") or app_config.LIMA_DEFAULT_PROBLEM)
    ok, msg = lima_db.set_handoff_status(handoff_id, "approved")
    obligation_result = None
    if ok:
        obligation_result = run_queued_obligation_checks(lima_db, problem_id=problem_id)
    flash = {
        "ok": ok,
        "handoff": "approved",
        "error": None if ok else msg,
        "obligation_checks": obligation_result,
    }
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=problem_id, lima_flash=flash),
    )


@app.post("/api/lima/handoff/{handoff_id}/approve-formal", response_class=HTMLResponse)
async def lima_handoff_approve_formal(
    request: Request, handoff_id: str, _auth: OperatorWriteAuth
):
    row = lima_db.get_handoff(handoff_id)
    if not row:
        return HTMLResponse("Unknown Lima handoff", status_code=404)
    ok, msg = lima_db.set_handoff_status(handoff_id, "approved_formal_review")
    flash = {"ok": ok, "handoff": "approved_formal_review", "error": None if ok else msg}
    problem_id = str(row.get("problem_id") or app_config.LIMA_DEFAULT_PROBLEM)
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=problem_id, lima_flash=flash),
    )


@app.post("/api/lima/handoff/{handoff_id}/approve-shadow", response_class=HTMLResponse)
async def lima_handoff_approve_shadow(
    request: Request, handoff_id: str, _auth: OperatorWriteAuth
):
    row = lima_db.get_handoff(handoff_id)
    if not row:
        return HTMLResponse("Unknown Lima handoff", status_code=404)
    ok, msg = lima_db.set_handoff_status(handoff_id, "approved_shadow_incubation")
    flash = {"ok": ok, "handoff": "approved_shadow_incubation", "error": None if ok else msg}
    problem_id = str(row.get("problem_id") or app_config.LIMA_DEFAULT_PROBLEM)
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=problem_id, lima_flash=flash),
    )


@app.post("/api/lima/handoff/{handoff_id}/hold", response_class=HTMLResponse)
async def lima_handoff_hold(
    request: Request, handoff_id: str, _auth: OperatorWriteAuth
):
    row = lima_db.get_handoff(handoff_id)
    if not row:
        return HTMLResponse("Unknown Lima handoff", status_code=404)
    ok, msg = lima_db.set_handoff_status(handoff_id, "held")
    flash = {"ok": ok, "handoff": "held", "error": None if ok else msg}
    problem_id = str(row.get("problem_id") or app_config.LIMA_DEFAULT_PROBLEM)
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=problem_id, lima_flash=flash),
    )


@app.post("/api/lima/handoff/{handoff_id}/reject", response_class=HTMLResponse)
async def lima_handoff_reject(
    request: Request, handoff_id: str, _auth: OperatorWriteAuth
):
    row = lima_db.get_handoff(handoff_id)
    if not row:
        return HTMLResponse("Unknown Lima handoff", status_code=404)
    ok, msg = lima_db.set_handoff_status(handoff_id, "rejected")
    flash = {"ok": ok, "handoff": "rejected", "error": None if ok else msg}
    problem_id = str(row.get("problem_id") or app_config.LIMA_DEFAULT_PROBLEM)
    return templates.TemplateResponse(
        request,
        "lima_panel.html",
        _lima_panel_context(problem=problem_id, lima_flash=flash),
    )


@app.post("/api/supershadow/handoff/{handoff_id}/approve")
async def supershadow_handoff_approve(request: Request, handoff_id: str):
    row = db.get_supershadow_handoff_request(handoff_id)
    if not row:
        return HTMLResponse("Unknown handoff", status_code=404)
    ok, msg, extra = db.approve_supershadow_handoff(handoff_id)
    flash: dict[str, Any] = {
        "ok": ok,
        "error": None if ok else msg,
        "handoff": "approved",
        "incubation_id": extra.get("incubation_id") if ok else None,
    }
    return templates.TemplateResponse(
        request,
        "supershadow_panel.html",
        _supershadow_global_panel_context(supershadow_flash=flash),
    )


@app.post("/api/supershadow/handoff/{handoff_id}/reject")
async def supershadow_handoff_reject(request: Request, handoff_id: str):
    row = db.get_supershadow_handoff_request(handoff_id)
    if not row:
        return HTMLResponse("Unknown handoff", status_code=404)
    db.reject_supershadow_handoff(handoff_id)
    flash = {"ok": True, "handoff": "rejected"}
    return templates.TemplateResponse(
        request,
        "supershadow_panel.html",
        _supershadow_global_panel_context(supershadow_flash=flash),
    )


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
