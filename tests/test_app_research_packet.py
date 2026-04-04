from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from orchestrator import app as app_mod
from orchestrator import config as app_config
from orchestrator.db import Database


async def _noop_loop(_db) -> None:
    return None


def test_update_research_packet_json_endpoint(
    tmp_path: Path, monkeypatch
) -> None:
    tmp_db = Database(str(tmp_path / "app.db"))
    tmp_db.initialize()
    monkeypatch.setattr(app_mod, "db", tmp_db)
    monkeypatch.setattr(app_config, "WORKSPACE_ROOT", str(tmp_path / "ws"))
    monkeypatch.setattr(app_mod, "manager_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "shadow_global_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "supershadow_global_loop", _noop_loop)

    cid = tmp_db.create_campaign(
        "hello",
        workspace_root=str(tmp_path / "ws"),
        workspace_template="minimal",
    )

    with TestClient(app_mod.app) as client:
        resp = client.post(
            f"/api/campaign/{cid}/research-packet",
            json={"research_packet": {"summary": "focus the extension bottleneck"}},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["research_packet"]["summary"] == "focus the extension bottleneck"
    state = tmp_db.get_campaign_state(cid)
    assert "focus the extension bottleneck" in state.campaign.research_packet_json
