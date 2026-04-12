from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from .db import LimaCoreDB
from .loop import LimaCoreLoop
from .presenter import build_index_context, build_scheduler_ui_view, build_workspace_context


def _is_htmx(request: Request) -> bool:
    return request.headers.get("HX-Request", "").strip().lower() == "true"


async def _extract_prompt(request: Request) -> str:
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        payload = await request.json()
        return str(payload.get("prompt") or "").strip()
    form = await request.form()
    return str(form.get("prompt") or "").strip()


def build_router(db_or_getter: LimaCoreDB | Callable[[], LimaCoreDB], templates: Jinja2Templates) -> APIRouter:
    router = APIRouter()

    def current_db() -> LimaCoreDB:
        return db_or_getter() if callable(db_or_getter) else db_or_getter

    def render_workspace(request: Request, problem_slug: str, *, flash: dict | None = None) -> HTMLResponse:
        db = current_db()
        ctx = build_workspace_context(db, problem_slug, flash=flash)
        template = "limacore/partials/workspace_body.html" if _is_htmx(request) else "limacore/workspace.html"
        return templates.TemplateResponse(request, template, ctx)

    @router.get("/limacore", response_class=HTMLResponse)
    async def limacore_index(request: Request) -> HTMLResponse:
        db = current_db()
        ctx = build_index_context(db)
        return templates.TemplateResponse(request, "limacore/index.html", ctx)

    @router.get("/limacore/{problem_slug}", response_class=HTMLResponse)
    async def limacore_workspace(request: Request, problem_slug: str) -> HTMLResponse:
        db = current_db()
        try:
            ctx = build_workspace_context(db, problem_slug)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Problem not found") from exc
        return templates.TemplateResponse(request, "limacore/workspace.html", ctx)

    @router.post("/api/limacore/problem_from_prompt")
    async def problem_from_prompt(request: Request) -> JSONResponse:
        prompt = await _extract_prompt(request)
        if not prompt:
            raise HTTPException(status_code=400, detail="prompt is required")
        db = current_db()
        loop = LimaCoreLoop(db)
        result = loop.create_problem_from_prompt(prompt)
        return JSONResponse(result)

    @router.post("/api/limacore/problem")
    async def create_problem_legacy(request: Request) -> Response:
        form = await request.form()
        prompt_parts = [
            str(form.get("title") or "").strip(),
            str(form.get("statement_md") or "").strip(),
            str(form.get("domain") or "").strip(),
        ]
        prompt = ". ".join(part for part in prompt_parts if part)
        if not prompt:
            prompt = str(form.get("slug") or "").strip()
        if not prompt:
            raise HTTPException(status_code=400, detail="problem prompt is required")
        db = current_db()
        loop = LimaCoreLoop(db)
        result = loop.create_problem_from_prompt(prompt)
        if _is_htmx(request):
            return render_workspace(request, result["problem_slug"], flash={"created": True, "run": result["first_result"]})
        return RedirectResponse(result["workspace_url"], status_code=303)

    @router.get("/api/limacore/workspace", response_class=HTMLResponse)
    async def workspace_fragment(request: Request, problem: str) -> HTMLResponse:
        return render_workspace(request, problem)

    @router.get("/api/limacore/workspace_header/{problem_slug}", response_class=HTMLResponse)
    async def workspace_header_fragment(request: Request, problem_slug: str) -> HTMLResponse:
        db = current_db()
        ctx = build_workspace_context(db, problem_slug)
        return templates.TemplateResponse(request, "limacore/partials/workspace_header.html", ctx)

    @router.get("/api/limacore/alerts/{problem_slug}", response_class=HTMLResponse)
    async def workspace_alerts_fragment(request: Request, problem_slug: str) -> HTMLResponse:
        db = current_db()
        ctx = build_workspace_context(db, problem_slug)
        return templates.TemplateResponse(request, "limacore/partials/alert_banner.html", ctx)

    @router.get("/api/limacore/problem_card/{problem_slug}", response_class=HTMLResponse)
    async def problem_card_fragment(request: Request, problem_slug: str) -> HTMLResponse:
        db = current_db()
        ctx = build_index_context(db)
        card = next((card for card in ctx["cards"] if str(card["problem"]["slug"]) == problem_slug), None)
        if card is None:
            raise HTTPException(status_code=404, detail="problem not found")
        return templates.TemplateResponse(request, "limacore/partials/problem_card.html", {"card": card})

    @router.post("/api/limacore/run", response_class=HTMLResponse)
    async def run_iteration_legacy(request: Request) -> Response:
        form = await request.form()
        problem = str(form.get("problem") or "").strip()
        if not problem:
            raise HTTPException(status_code=400, detail="problem is required")
        return await run_once(request, problem)

    @router.post("/api/limacore/run/{problem_slug}/once", response_class=HTMLResponse)
    async def run_once(request: Request, problem_slug: str) -> Response:
        db = current_db()
        loop = LimaCoreLoop(db)
        result = loop.run_iteration(problem_slug)
        if not _is_htmx(request):
            return RedirectResponse(f"/limacore/{problem_slug}", status_code=303)
        return render_workspace(request, problem_slug, flash={"run": result})

    @router.post("/api/limacore/run/{problem_slug}/batch", response_class=HTMLResponse)
    async def run_batch(request: Request, problem_slug: str) -> Response:
        db = current_db()
        loop = LimaCoreLoop(db)
        form = await request.form() if "application/x-www-form-urlencoded" in request.headers.get("content-type", "") or "multipart/form-data" in request.headers.get("content-type", "") else {}
        iterations = int((form.get("iterations") if form else None) or 10)
        result = loop.run_batch(problem_slug, iterations=iterations)
        if not _is_htmx(request):
            return RedirectResponse(f"/limacore/{problem_slug}", status_code=303)
        return render_workspace(request, problem_slug, flash={"batch_run": result})

    @router.post("/api/limacore/autopilot/{problem_slug}/start", response_class=HTMLResponse)
    async def autopilot_start(request: Request, problem_slug: str) -> Response:
        db = current_db()
        problem = db.set_autopilot_enabled(problem_slug, True)
        if problem is None:
            raise HTTPException(status_code=404, detail="problem not found")
        db.append_event(str(problem["id"]), "autopilot_started", "accepted", summary_md="Autopilot started by operator.")
        loop = LimaCoreLoop(db)
        loop.run_iteration(problem_slug)
        if not _is_htmx(request):
            return RedirectResponse(f"/limacore/{problem_slug}", status_code=303)
        return render_workspace(request, problem_slug, flash={"autopilot": "started"})

    @router.post("/api/limacore/autopilot/{problem_slug}/pause", response_class=HTMLResponse)
    async def autopilot_pause(request: Request, problem_slug: str) -> Response:
        db = current_db()
        problem = db.set_autopilot_enabled(problem_slug, False)
        if problem is None:
            raise HTTPException(status_code=404, detail="problem not found")
        db.append_event(str(problem["id"]), "autopilot_paused", "accepted", summary_md="Autopilot paused by operator.")
        if not _is_htmx(request):
            return RedirectResponse(f"/limacore/{problem_slug}", status_code=303)
        return render_workspace(request, problem_slug, flash={"autopilot": "paused"})

    @router.post("/api/limacore/cohort/{problem_slug}/spawn", response_class=HTMLResponse)
    async def spawn_cohort(request: Request, problem_slug: str) -> Response:
        db = current_db()
        loop = LimaCoreLoop(db)
        result = loop.run_iteration(problem_slug)
        if not _is_htmx(request):
            return RedirectResponse(f"/limacore/{problem_slug}", status_code=303)
        return render_workspace(request, problem_slug, flash={"spawned_cohort": result})

    @router.get("/api/limacore/cohort/{cohort_id}")
    async def cohort_detail(cohort_id: str) -> JSONResponse:
        db = current_db()
        row = db.get_cohort(cohort_id)
        if row is None:
            raise HTTPException(status_code=404, detail="cohort not found")
        problem_id = str(row["problem_id"])
        jobs = db.list_jobs(problem_id, cohort_id=cohort_id)
        return JSONResponse({"cohort": row, "jobs": jobs})

    @router.get("/api/limacore/job/{job_id}")
    async def job_detail(job_id: str) -> JSONResponse:
        db = current_db()
        row = db.get_job(job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="job not found")
        return JSONResponse({"job": row})

    @router.get("/api/limacore/frontier/{problem_slug}", response_class=HTMLResponse)
    async def frontier_fragment(request: Request, problem_slug: str) -> HTMLResponse:
        db = current_db()
        ctx = build_workspace_context(db, problem_slug)
        return templates.TemplateResponse(request, "limacore/partials/frontier_panel.html", ctx)

    @router.get("/api/limacore/program/{problem_slug}")
    async def program_fragment(problem_slug: str) -> JSONResponse:
        db = current_db()
        ctx = build_workspace_context(db, problem_slug)
        return JSONResponse({"program": ctx["program"], "throughput": ctx["stats"], "status": ctx["status_view"]})

    @router.get("/api/limacore/ops")
    async def ops() -> JSONResponse:
        db = current_db()
        scheduler = build_scheduler_ui_view(db)
        scheduler_state = db.get_scheduler_state()
        problems = db.list_problems()
        manager_latest: list[dict[str, str]] = []
        for problem in problems[:10]:
            pid = str(problem["id"])
            events = db.list_events(pid, limit=40)
            tick = next(
                (
                    event
                    for event in reversed(events)
                    if str(event.get("event_type") or "") in {"manager_tick", "manager_tick_failed", "manager_plan_selected"}
                ),
                None,
            )
            if tick is None:
                continue
            manager_latest.append(
                {
                    "problem_slug": str(problem.get("slug") or ""),
                    "event_type": str(tick.get("event_type") or ""),
                    "summary_md": str(tick.get("summary_md") or ""),
                    "created_at": str(tick.get("created_at") or ""),
                }
            )
        return JSONResponse(
            {
                "scheduler_state": scheduler_state,
                "scheduler_health": scheduler["scheduler"],
                "healthy": bool(scheduler["scheduler"]["scheduler_healthy"]),
                "stale": bool(scheduler["scheduler"]["scheduler_stale"]),
                "scheduler_headline": scheduler["scheduler_headline"],
                "scheduler_last_pass_started_at": scheduler["scheduler_last_pass_started_at"],
                "scheduler_last_pass_completed_at": scheduler["scheduler_last_pass_completed_at"],
                "scheduler_last_error_md": scheduler["scheduler_last_error_md"],
                "scheduler_pass_count": scheduler["scheduler_pass_count"],
                "scheduler_failure_count": scheduler["scheduler_failure_count"],
                "manager_latest_events": manager_latest,
            }
        )

    @router.get("/api/limacore/problem/{problem_slug}/ops")
    async def problem_ops(problem_slug: str) -> JSONResponse:
        db = current_db()
        try:
            ctx = build_workspace_context(db, problem_slug)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Problem not found") from exc
        return JSONResponse(
            {
                "problem": ctx["problem"],
                "status_view": ctx["status_view"],
                "manager": ctx.get("manager", {}),
                "autopilot_state": ctx["autopilot_state"],
                "scheduler_state": db.get_scheduler_state(),
                "scheduler_health": ctx["scheduler"],
                "healthy": bool(ctx["scheduler"]["scheduler_healthy"]),
                "stale": bool(ctx["scheduler"]["scheduler_stale"]),
            }
        )

    @router.post("/api/limacore/problem/{problem_slug}/cleanup-legacy", response_class=HTMLResponse)
    async def cleanup_legacy(request: Request, problem_slug: str) -> Response:
        """Clean up legacy frontier nodes for a problem (Collatz-specific)."""
        db = current_db()
        from .cleanup import cleanup_legacy_collatz_frontier, has_legacy_frontier_cleanup_available

        problem = db.get_problem(problem_slug)
        if problem is None:
            raise HTTPException(status_code=404, detail="problem not found")

        problem_id = str(problem["id"])

        # Check if cleanup is available
        if not has_legacy_frontier_cleanup_available(db, problem_id):
            if not _is_htmx(request):
                return RedirectResponse(f"/limacore/{problem_slug}", status_code=303)
            return render_workspace(request, problem_slug, flash={"cleanup": "no_legacy_data"})

        # Perform cleanup
        try:
            result = cleanup_legacy_collatz_frontier(db, problem_id)
            flash = {
                "cleanup": "success",
                "removed_nodes": result.removed_node_keys,
                "archived_nodes": result.archived_node_keys,
            }
        except KeyError as e:
            raise HTTPException(status_code=404, detail=str(e))

        if not _is_htmx(request):
            return RedirectResponse(f"/limacore/{problem_slug}", status_code=303)
        return render_workspace(request, problem_slug, flash=flash)

    @router.post("/api/limacore/problem/{problem_slug}/restart-clean", response_class=HTMLResponse)
    async def restart_clean(request: Request, problem_slug: str) -> Response:
        """Clean up legacy frontier and restart problem with clean state."""
        db = current_db()
        from .cleanup import restart_problem_clean, has_legacy_frontier_cleanup_available
        from .loop import LimaCoreLoop

        problem = db.get_problem(problem_slug)
        if problem is None:
            raise HTTPException(status_code=404, detail="problem not found")

        problem_id = str(problem["id"])
        loop = LimaCoreLoop(db)

        # Perform cleanup and restart
        try:
            result = restart_problem_clean(db, loop, problem_id)
            flash = {
                "restart_clean": "success",
                "removed_nodes": result.removed_node_keys,
                "rerun_triggered": result.rerun_triggered,
            }
        except KeyError as e:
            raise HTTPException(status_code=404, detail=str(e))

        if not _is_htmx(request):
            return RedirectResponse(f"/limacore/{problem_slug}", status_code=303)
        return render_workspace(request, problem_slug, flash=flash)

    return router
