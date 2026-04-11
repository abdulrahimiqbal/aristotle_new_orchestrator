from __future__ import annotations

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from collections.abc import Callable

from .db import LimaCoreDB
from .loop import LimaCoreLoop
from .presenter import build_index_context, build_workspace_context


def _is_htmx(request: Request) -> bool:
    return request.headers.get("HX-Request", "").strip().lower() == "true"


def build_router(db_or_getter: LimaCoreDB | Callable[[], LimaCoreDB], templates: Jinja2Templates) -> APIRouter:
    router = APIRouter()

    def current_db() -> LimaCoreDB:
        return db_or_getter() if callable(db_or_getter) else db_or_getter

    @router.get("/limacore", response_class=HTMLResponse)
    async def limacore_index(request: Request) -> HTMLResponse:
        db = current_db()
        ctx = build_index_context(db)
        return templates.TemplateResponse(
            request,
            "limacore/index.html",
            ctx,
        )

    @router.get("/limacore/{problem_slug}", response_class=HTMLResponse)
    async def limacore_workspace(request: Request, problem_slug: str) -> HTMLResponse:
        db = current_db()
        try:
            ctx = build_workspace_context(db, problem_slug)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Problem not found") from exc
        return templates.TemplateResponse(
            request,
            "limacore/workspace.html",
            ctx,
        )

    @router.get("/api/limacore/workspace", response_class=HTMLResponse)
    async def workspace_fragment(request: Request, problem: str) -> HTMLResponse:
        db = current_db()
        ctx = build_workspace_context(db, problem)
        return templates.TemplateResponse(request, "limacore/partials/workspace_body.html", ctx)

    @router.post("/api/limacore/run", response_class=HTMLResponse)
    async def run_iteration(request: Request, problem: str = Form(...)) -> Response:
        db = current_db()
        loop = LimaCoreLoop(db)
        result = loop.run_iteration(problem)
        if not _is_htmx(request):
            return RedirectResponse(f"/limacore/{problem}", status_code=303)
        ctx = build_workspace_context(db, problem, flash={"run": result})
        return templates.TemplateResponse(request, "limacore/partials/workspace_body.html", ctx)

    @router.post("/api/limacore/problem", response_class=HTMLResponse)
    async def create_problem(
        request: Request,
        slug: str = Form(...),
        title: str = Form(...),
        statement_md: str = Form(...),
        domain: str = Form(default=""),
        target_theorem: str = Form(default=""),
    ) -> Response:
        db = current_db()
        problem_id, _created = db.create_problem(
            slug=slug.strip(),
            title=title.strip(),
            statement_md=statement_md.strip(),
            domain=domain.strip(),
            target_theorem=target_theorem.strip() or statement_md.strip(),
        )
        row = db.get_problem(problem_id)
        assert row is not None
        if not _is_htmx(request):
            return RedirectResponse(f"/limacore/{row['slug']}", status_code=303)
        ctx = build_workspace_context(db, str(row["slug"]), flash={"created": True})
        return templates.TemplateResponse(request, "limacore/partials/workspace_body.html", ctx)

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
        return JSONResponse({"program": ctx["program"], "throughput": ctx["stats"]})

    return router
