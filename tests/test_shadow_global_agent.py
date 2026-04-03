from __future__ import annotations

import asyncio
import json
from pathlib import Path

from orchestrator import config as app_config
from orchestrator.db import Database
from orchestrator.shadow_agent import (
    SHADOW_GLOBAL_GOAL_ID,
    _normalize_global_response,
    _safe_json_loads,
    run_shadow_global_lab,
)


def test_normalize_global_response_filters_invalid_promotions(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "g.db"))
    db.initialize()
    cid = db.create_campaign("c", workspace_root=str(tmp_path / "ws"))
    tid = db.add_targets(cid, ["t"])[0]
    raw = {
        "run_summary": "s",
        "solved_world": {"claim": "world"},
        "hypotheses": [
            {
                "kind": "new_axiom",
                "title": "A1",
                "body_md": "SPECULATIVE requires new axioms",
                "lean_snippet": "",
                "evidence": [{"campaign_id": cid, "target_id": tid, "note": "x"}],
            }
        ],
        "promotion_requests": [
            {"kind": "new_target", "campaign_id": cid, "description": "ok"},
            {"kind": "new_experiment", "campaign_id": cid, "target_id": tid, "objective": "o"},
            {"kind": "new_experiment", "campaign_id": "bad", "target_id": tid, "objective": "x"},
        ],
    }
    out, warnings = _normalize_global_response(raw, db)
    assert out["solved_world"]["claim"] == "world"
    assert len(out["promotion_requests"]) == 2
    assert out["hypotheses"][0]["score_0_100"] >= 0
    assert out["hypotheses"][0]["groundability_tier"] in ("A", "B", "C")
    assert isinstance(warnings, list)


def test_safe_json_loads_accepts_wrapped_object() -> None:
    raw = """
    Here is the JSON payload you asked for:

    ```json
    {"run_summary":"wrapped ok","solved_world":{"claim":"c"},"hypotheses":[],"promotion_requests":[]}
    ```

    Hope that helps.
    """
    data = _safe_json_loads(raw)
    assert data["run_summary"] == "wrapped ok"


def test_run_shadow_global_lab_accepts_wrapped_json_response(
    tmp_path: Path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "g.db"))
    db.initialize()
    cid = db.create_campaign("c", workspace_root=str(tmp_path / "ws"))
    tid = db.add_targets(cid, ["t"])[0]

    async def fake_invoke_llm(*args, **kwargs) -> str:
        return f"""
        Here is your JSON response:
        ```json
        {{
          "run_summary": "wrapped ok",
          "solved_world": {{"claim": "world"}},
          "hypotheses": [
            {{
              "kind": "exploration",
              "title": "H1",
              "body_md": "Use the verified target.",
              "lean_snippet": "",
              "evidence": [{{"campaign_id": "{cid}", "target_id": "{tid}", "note": "ok"}}]
            }}
          ],
          "promotion_requests": [
            {{"kind": "new_experiment", "campaign_id": "{cid}", "target_id": "{tid}", "objective": "o"}}
          ]
        }}
        ```
        """

    monkeypatch.setattr("orchestrator.shadow_agent.invoke_llm", fake_invoke_llm)
    monkeypatch.setattr(app_config, "LLM_API_KEY", "test-key")

    res = asyncio.run(
        run_shadow_global_lab(
            db,
            goal_text="goal",
            trigger_kind="auto",
        )
    )

    assert res["ok"] is True
    assert res["promotion_count"] == 1
    assert db.get_shadow_global_state(SHADOW_GLOBAL_GOAL_ID)["revision"] == 1


def test_run_shadow_global_lab_retries_invalid_json_once(
    tmp_path: Path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "g.db"))
    db.initialize()
    cid = db.create_campaign("c", workspace_root=str(tmp_path / "ws"))
    tid = db.add_targets(cid, ["t"])[0]
    calls = {"n": 0}

    async def fake_invoke_llm(*args, **kwargs) -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            return '{"stance":{"summary":"broken"}'
        return json.dumps(
            {
                "run_summary": "retry ok",
                "solved_world": {"claim": "world"},
                "hypotheses": [
                    {
                        "kind": "exploration",
                        "title": "H1",
                        "body_md": "Use retry.",
                        "lean_snippet": "",
                        "evidence": [{"campaign_id": cid, "target_id": tid, "note": "ok"}],
                    }
                ],
                "promotion_requests": [
                    {
                        "kind": "new_experiment",
                        "campaign_id": cid,
                        "target_id": tid,
                        "objective": "o",
                    }
                ],
            }
        )

    monkeypatch.setattr("orchestrator.shadow_agent.invoke_llm", fake_invoke_llm)
    monkeypatch.setattr(app_config, "LLM_API_KEY", "test-key")

    res = asyncio.run(
        run_shadow_global_lab(
            db,
            goal_text="goal",
            trigger_kind="auto",
        )
    )

    assert res["ok"] is True
    assert res["json_retry_count"] == 1
    assert calls["n"] == 2
