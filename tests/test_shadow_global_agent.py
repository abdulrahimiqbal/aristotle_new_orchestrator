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
            {
                "kind": "new_target",
                "campaign_id": cid,
                "description": "ok",
                "grounding_reason": "Formalize a new object the proof program now depends on.",
                "expected_signal": "We either get the reusable object or discover the exact formalization blocker.",
                "novelty_reason": "Adds a new reusable formal object rather than repeating a queued experiment.",
                "rubric_scores": {
                    "novel_math": 3,
                    "proof_program_leverage": 3,
                    "grounding_need": 3,
                    "expected_signal": 3,
                    "queue_fitness": 3,
                },
            },
            {
                "kind": "new_experiment",
                "campaign_id": cid,
                "target_id": tid,
                "objective": "o",
                "grounding_reason": "Ground the first bridge lemma before expanding the program.",
                "expected_signal": "A success gives the bridge lemma; a failure exposes the obstruction.",
                "novelty_reason": "Targets the first live bridge instead of duplicating queued work.",
                "rubric_scores": {
                    "novel_math": 2,
                    "proof_program_leverage": 3,
                    "grounding_need": 3,
                    "expected_signal": 3,
                    "queue_fitness": 2,
                },
            },
            {"kind": "new_experiment", "campaign_id": "bad", "target_id": tid, "objective": "x"},
        ],
    }
    out, warnings = _normalize_global_response(raw, db)
    assert out["solved_world"]["claim"] == "world"
    assert len(out["promotion_requests"]) == 2
    assert out["hypotheses"][0]["score_0_100"] >= 0
    assert out["hypotheses"][0]["groundability_tier"] in ("A", "B", "C")
    assert isinstance(warnings, list)


def test_normalize_global_response_filters_duplicate_and_unjustified_promotions(
    tmp_path: Path,
) -> None:
    db = Database(str(tmp_path / "g.db"))
    db.initialize()
    cid = db.create_campaign("c", workspace_root=str(tmp_path / "ws"))
    tid = db.add_targets(cid, ["parity target"])[0]
    db.shadow_global_commit_run(
        SHADOW_GLOBAL_GOAL_ID,
        trigger_kind="manual",
        summary="existing queue",
        response_obj={},
        new_stance_json="{}",
        new_policy_json="{}",
        hypotheses=[],
        promotions=[
            {
                "kind": "new_experiment",
                "campaign_id": cid,
                "target_id": tid,
                "objective": "Verify descent for n <= 10^6 with k <= 500.",
                "grounding_reason": "Existing queue item.",
                "expected_signal": "Existing queue item.",
                "novelty_reason": "Existing queue item.",
            }
        ],
        goal_text="goal",
    )

    raw = {
        "run_summary": "s",
        "solved_world": {"claim": "world"},
        "hypotheses": [{"kind": "exploration", "title": "H1", "body_md": "SPECULATIVE"}],
        "promotion_requests": [
            {
                "kind": "new_experiment",
                "campaign_id": cid,
                "target_id": tid,
                "objective": "Verify descent for n <= 10^6 with k <= 500 using native_decide.",
                "grounding_reason": "Ground the same bounded descent claim.",
                "expected_signal": "Confirms the same finite check.",
                "novelty_reason": "Almost the same request already exists in queue.",
            },
            {
                "kind": "new_experiment",
                "campaign_id": cid,
                "target_id": tid,
                "objective": "Try another experiment",
                "expected_signal": "Something happens.",
                "novelty_reason": "Unclear.",
            },
            {
                "kind": "new_target",
                "campaign_id": cid,
                "description": "Define a new parity summary object for residue classes modulo 8.",
                "grounding_reason": "The proof program now needs a reusable object for residue-class summaries.",
                "expected_signal": "Either we get a reusable interface or discover the exact formalization gap.",
                "novelty_reason": "This adds a reusable object not currently in the queue.",
                "rubric_scores": {
                    "novel_math": 3,
                    "proof_program_leverage": 3,
                    "grounding_need": 3,
                    "expected_signal": 3,
                    "queue_fitness": 3,
                },
            },
            {
                "kind": "new_target",
                "campaign_id": cid,
                "description": "Vague target that should not survive.",
                "grounding_reason": "Maybe useful.",
                "expected_signal": "Might help.",
                "novelty_reason": "New wording only.",
                "rubric_scores": {
                    "novel_math": 1,
                    "proof_program_leverage": 1,
                    "grounding_need": 1,
                    "expected_signal": 1,
                    "queue_fitness": 1,
                },
            },
        ],
    }

    out, warnings = _normalize_global_response(raw, db)

    assert len(out["promotion_requests"]) == 1
    assert out["promotion_requests"][0]["kind"] == "new_target"
    assert "promotion_duplicate_filtered" in warnings
    assert "promotion_missing_grounding_reason" in warnings
    assert "promotion_below_rubric" in warnings


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
            {{
              "kind": "new_experiment",
              "campaign_id": "{cid}",
              "target_id": "{tid}",
              "objective": "o",
              "grounding_reason": "Ground the first bridge lemma.",
              "expected_signal": "We learn whether the bridge lemma really lands.",
              "novelty_reason": "This is the first live grounding request for that bridge.",
              "rubric_scores": {{"novel_math": 2, "proof_program_leverage": 3, "grounding_need": 3, "expected_signal": 3, "queue_fitness": 2}}
            }}
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
                        "grounding_reason": "Ground the first bridge lemma.",
                        "expected_signal": "We learn whether the bridge lemma really lands.",
                        "novelty_reason": "This is the first live grounding request for that bridge.",
                        "rubric_scores": {
                            "novel_math": 2,
                            "proof_program_leverage": 3,
                            "grounding_need": 3,
                            "expected_signal": 3,
                            "queue_fitness": 2,
                        },
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


def test_run_shadow_global_lab_can_think_without_emitting_promotions(
    tmp_path: Path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "g.db"))
    db.initialize()
    cid = db.create_campaign("c", workspace_root=str(tmp_path / "ws"))
    tid = db.add_targets(cid, ["t"])[0]

    async def fake_invoke_llm(*args, **kwargs) -> str:
        return json.dumps(
            {
                "run_summary": "keep thinking",
                "solved_world": {"claim": "world"},
                "hypotheses": [
                    {
                        "kind": "proof_program",
                        "title": "H1",
                        "body_md": "Invent the new structure first.",
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
                        "grounding_reason": "Ground the first bridge lemma.",
                        "expected_signal": "We learn whether the bridge lemma really lands.",
                        "novelty_reason": "This is the first live grounding request for that bridge.",
                        "rubric_scores": {
                            "novel_math": 2,
                            "proof_program_leverage": 3,
                            "grounding_need": 3,
                            "expected_signal": 3,
                            "queue_fitness": 2,
                        },
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
            promotion_budget=0,
            experiment_promotion_budget=0,
            suppress_promotions_reason="Queue already full.",
        )
    )

    assert res["ok"] is True
    assert res["promotion_count"] == 0
    assert "promotion_budget_zero" in res["validation_warnings"]
    assert db.list_shadow_global_promotion_requests(SHADOW_GLOBAL_GOAL_ID, limit=10) == []
