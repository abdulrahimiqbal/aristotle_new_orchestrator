from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from orchestrator import app as app_mod
from orchestrator import config as app_config
from orchestrator.db import Database


async def _noop_loop(_db) -> None:
    return None


def _seed_supershadow_run(db: Database) -> None:
    db.supershadow_commit_run(
        "global_collatz_supershadow",
        trigger_kind="manual",
        worldview_summary="Search for an odd-state quotient language.",
        run_summary="One concept is ready for Shadow review.",
        fact_basis_json='[{"fact_key":"builtin:modular_descent_mod_8","label":"Mod 8 grounded"}]',
        pressure_map_json='[{"cluster":"modular","fact_keys":["builtin:modular_descent_mod_8"]}]',
        response_obj={"ok": True},
        new_worldview_json='{"summary":"odd-state quotient"}',
        new_policy_json='{"prefer_compression": true}',
        concepts=[
            {
                "title": "Odd-state quotient",
                "worldview_summary": "Collapse even transport into a derived odd-state operator.",
                "concepts": ["Odd-only dynamics may be the right ambient state space."],
                "ontological_moves": ["Odd-state quotient", "Derived odd-state transfer"],
                "explains_facts": [
                    {
                        "fact_key": "builtin:modular_descent_mod_8",
                        "fact_label": "Mod 8 grounded",
                        "role": "explains",
                        "note": "Explains the modular summary structurally.",
                    }
                ],
                "tensions": [{"text": "Still must explain the global height failure."}],
                "kill_tests": [
                    {
                        "description": "Check whether the odd-state quotient is stable under the derived map.",
                        "expected_failure_signal": "A small residue obstruction breaks the quotient.",
                        "suggested_grounding_path": "Shadow should formalize the odd-state operator first.",
                    }
                ],
                "bridge_lemmas": [
                    "Define the odd-state quotient and prove compatibility with one Collatz step."
                ],
                "reduce_frontier_or_rename": "This reduces the frontier only if the quotient predicts the odd-only survival window.",
                "scores": {
                    "compression_power": 5,
                    "fit_to_known_facts": 5,
                    "ontological_delta": 4,
                    "falsifiability": 4,
                    "bridgeability": 4,
                    "grounding_cost": 2,
                    "speculative_risk": 2,
                },
                "shadow_handoffs": [
                    {
                        "title": "Build the odd-state proof program",
                        "summary": "Shadow should operationalize the quotient language.",
                        "why_compressive": "It explains modular behavior and odd-input structure together.",
                        "bridge_lemmas": [
                            "Define the odd-state quotient and prove compatibility with one Collatz step."
                        ],
                        "shadow_task": "Turn the quotient language into a disciplined proof program.",
                        "recommended_next_step": "Formalize the odd-state operator interface.",
                        "grounding_notes": "Start with a small Lean bridge.",
                    }
                ],
            }
        ],
        goal_text="goal",
    )


def test_supershadow_dashboard_run_and_handoff_routes(
    tmp_path: Path, monkeypatch
) -> None:
    tmp_db = Database(str(tmp_path / "app.db"))
    tmp_db.initialize()
    monkeypatch.setattr(app_mod, "db", tmp_db)
    monkeypatch.setattr(app_config, "WORKSPACE_ROOT", str(tmp_path / "ws"))
    monkeypatch.setattr(app_mod, "manager_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "shadow_global_loop", _noop_loop)
    monkeypatch.setattr(app_mod, "supershadow_global_loop", _noop_loop)

    async def fake_run_supershadow_global_lab(*args, **kwargs):
        _seed_supershadow_run(tmp_db)
        return {
            "ok": True,
            "run_id": "run1",
            "concept_count": 1,
            "handoff_count": 1,
            "validation_warnings": [],
        }

    monkeypatch.setattr(app_mod, "run_supershadow_global_lab", fake_run_supershadow_global_lab)

    with TestClient(app_mod.app) as client:
        resp = client.get("/supershadow")
        assert resp.status_code == 200
        assert "Supershadow Lab" in resp.text
        assert "zero live authority" in resp.text

        run_resp = client.post("/api/supershadow/run")
        assert run_resp.status_code == 200
        assert "Odd-state quotient" in run_resp.text
        assert "Build the odd-state proof program" in run_resp.text

        handoff = tmp_db.list_supershadow_handoff_requests(
            "global_collatz_supershadow", limit=10
        )[0]
        approve_resp = client.post(f"/api/supershadow/handoff/{handoff['id']}/approve")
        assert approve_resp.status_code == 200
        assert "preserved incubation packet" in approve_resp.text

    approved = tmp_db.get_supershadow_handoff_request(handoff["id"])
    assert approved is not None
    assert approved["status"] == "approved"
    incubations = tmp_db.list_supershadow_incubations("global_collatz_supershadow", limit=10)
    assert len(incubations) == 1
    assert incubations[0]["status"] == "incubating"

    conn = sqlite3.connect(str(tmp_path / "app.db"))
    try:
        target_count = conn.execute("SELECT COUNT(*) FROM targets").fetchone()[0]
        experiment_count = conn.execute("SELECT COUNT(*) FROM experiments").fetchone()[0]
        assert int(target_count) == 0
        assert int(experiment_count) == 0
    finally:
        conn.close()
