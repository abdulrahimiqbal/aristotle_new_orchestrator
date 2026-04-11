from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from orchestrator import app as app_mod
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import limacore_loop


async def _noop_loop(*_args, **_kwargs) -> None:
    return None


def test_problem_from_prompt_creation_and_autopilot_start(tmp_path: Path, monkeypatch) -> None:
    limacore_db = LimaCoreDB(str(tmp_path / "limacore.db"))
    limacore_db.initialize()
    monkeypatch.setattr(app_mod, "limacore_db", limacore_db)
    monkeypatch.setattr(app_mod, "manager_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "shadow_global_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "supershadow_global_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "lima_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "limacore_loop", _noop_loop)

    with TestClient(app_mod.app) as client:
        resp = client.post(
            "/api/limacore/problem_from_prompt",
            json={
                "prompt": "Collatz conjecture. Define T(n)=n/2 for even n and 3n+1 for odd n. Prove every positive integer reaches 1. Prefer odd-step, quotient, or hidden-state worlds."
            },
        )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["problem_slug"] == "collatz-conjecture"
        assert payload["title"] == "Collatz conjecture."
        assert payload["status"] in {"running", "blocked", "stalled"}
        assert payload["workspace_url"] == "/limacore/collatz-conjecture"

    created = limacore_db.get_problem("collatz-conjecture")
    assert created is not None
    assert created["original_prompt"].startswith("Collatz conjecture")
    assert created["normalized_statement_md"]
    assert created["domain"] == "number theory"
    assert int(created["autopilot_enabled"]) == 1


def test_routes_render_and_controls_work(tmp_path: Path, monkeypatch) -> None:
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
        assert "What problem should Lima-core attack?" in resp.text
        assert "Start autopilot" in resp.text
        assert "problem slug" not in resp.text

        ws = client.get("/limacore/inward-compression-conjecture")
        assert ws.status_code == 200
        assert "Autopilot" in ws.text
        assert "Frontier debt" in ws.text
        assert "Run 10 bounded iterations" in ws.text

        pause = client.post("/api/limacore/autopilot/inward-compression-conjecture/pause", headers={"HX-Request": "true"})
        assert pause.status_code == 200
        assert "paused" in pause.text.lower()

        start = client.post("/api/limacore/autopilot/inward-compression-conjecture/start", headers={"HX-Request": "true"})
        assert start.status_code == 200
        assert "Autopilot on" in start.text or "running" in start.text.lower()

        once = client.post("/api/limacore/run/inward-compression-conjecture/once", headers={"HX-Request": "true"})
        assert once.status_code == 200
        assert "Last run" in once.text

        batch = client.post("/api/limacore/run/inward-compression-conjecture/batch", data={"iterations": "10"}, headers={"HX-Request": "true"})
        assert batch.status_code == 200
        assert "Batch run completed" in batch.text

        spawn = client.post("/api/limacore/cohort/inward-compression-conjecture/spawn", headers={"HX-Request": "true"})
        assert spawn.status_code == 200
        assert "Aristotle Farm" in spawn.text

        frontier = client.get("/api/limacore/frontier/inward-compression-conjecture")
        assert frontier.status_code == 200
        assert "Frontier" in frontier.text

        header = client.get("/api/limacore/workspace_header/inward-compression-conjecture")
        assert header.status_code == 200
        assert "Program" in header.text

        alerts = client.get("/api/limacore/alerts/inward-compression-conjecture")
        assert alerts.status_code == 200

        program = client.get("/api/limacore/program/inward-compression-conjecture")
        assert program.status_code == 200
        assert "program" in program.json()
