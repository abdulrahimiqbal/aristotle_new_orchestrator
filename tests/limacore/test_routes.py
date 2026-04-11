from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from orchestrator import app as app_mod
from orchestrator.limacore.db import LimaCoreDB


async def _noop_loop(*_args, **_kwargs) -> None:
    return None


def test_routes_render_and_return_expected_payloads(tmp_path: Path, monkeypatch) -> None:
    limacore_db = LimaCoreDB(str(tmp_path / "limacore.db"))
    limacore_db.initialize()
    monkeypatch.setattr(app_mod, "limacore_db", limacore_db)
    monkeypatch.setattr(app_mod, "manager_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "shadow_global_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "supershadow_global_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "lima_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "limacore_loop", _noop_loop)

    with TestClient(app_mod.app) as client:
        resp = client.get("/limacore")
        assert resp.status_code == 200
        assert "Lima-core" in resp.text

        ws = client.get("/limacore/inward-compression-conjecture")
        assert ws.status_code == 200
        assert "Inward Compression Conjecture" in ws.text

        frag = client.get("/api/limacore/workspace", params={"problem": "inward-compression-conjecture"})
        assert frag.status_code == 200
        assert "Aristotle Farm" in frag.text

        run = client.post("/api/limacore/run", data={"problem": "inward-compression-conjecture"}, headers={"HX-Request": "true"})
        assert run.status_code == 200
        assert "Balanced compression coordinates" in run.text or "Last run" in run.text

        cohort = limacore_db.list_cohorts(str(limacore_db.get_problem("inward-compression-conjecture")["id"]))[0]
        cohort_resp = client.get(f"/api/limacore/cohort/{cohort['id']}")
        assert cohort_resp.status_code == 200
        assert "cohort" in cohort_resp.json()

        job = limacore_db.list_jobs(str(limacore_db.get_problem("inward-compression-conjecture")["id"]))[0]
        job_resp = client.get(f"/api/limacore/job/{job['id']}")
        assert job_resp.status_code == 200
        assert "job" in job_resp.json()

        frontier = client.get("/api/limacore/frontier/inward-compression-conjecture")
        assert frontier.status_code == 200
        assert "Frontier" in frontier.text

        program = client.get("/api/limacore/program/inward-compression-conjecture")
        assert program.status_code == 200
        assert "program" in program.json()
