from __future__ import annotations

import asyncio
import json
from pathlib import Path

from orchestrator import config as app_config
from orchestrator.db import Database
from orchestrator.supershadow_agent import (
    SUPERSHADOW_GLOBAL_GOAL_ID,
    _build_grounded_fact_basis,
    _normalize_supershadow_response,
    run_supershadow_global_lab,
)


def test_build_grounded_fact_basis_includes_builtin_and_live_facts(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "facts.db"))
    db.initialize()
    cid = db.create_campaign(
        "collatz campaign",
        workspace_root=str(tmp_path / "ws"),
        workspace_template="minimal",
        research_packet_json=json.dumps(
            {
                "known_true": ["modular scaffold survives on this campaign"],
                "known_false": ["naive monotonicity route failed here"],
                "finite_examples": ["checked descent to 10^5"],
                "formal_anchors": ["2-adic operator interface"],
            }
        ),
    )
    tid = db.add_targets(cid, ["prove a verified modular lemma"])[0]
    db.update_target(tid, "verified", "proved in a prior run")
    eid = db.create_experiment(cid, tid, "bounded descent check")
    db.update_experiment_completed(
        eid,
        result_raw="raw",
        result_summary="Completed bounded descent check with strong support.",
        verdict="proved",
        parsed_proved_lemmas=["lemma"],
        parsed_generated_lemmas=[],
        parsed_unsolved_goals=[],
        parsed_blockers=[],
        parsed_counterexamples=[],
        parsed_error_message="",
    )

    facts = _build_grounded_fact_basis(db)
    fact_keys = {fact["fact_key"] for fact in facts}
    assert "builtin:modular_descent_mod_8" in fact_keys
    assert f"live:{cid}:packet:known_true:0" in fact_keys
    assert f"live:{cid}:target:{tid}" in fact_keys
    assert f"live:{cid}:experiment:{eid}" in fact_keys


def test_normalize_supershadow_response_filters_invalid_concepts_and_caps_handoffs() -> None:
    fact_basis = [
        {
            "fact_key": "builtin:modular_descent_mod_8",
            "label": "Mod 8 descent is grounded.",
            "detail": "detail",
            "kind": "modular",
            "provenance": "builtin_seed",
        },
        {
            "fact_key": "builtin:naive_height_survives_odd_inputs",
            "label": "Naive height survives on odd inputs.",
            "detail": "detail",
            "kind": "odd_subdynamics",
            "provenance": "builtin_seed",
        },
    ]
    raw = {
        "worldview_summary": "Try a new odd-state language.",
        "run_summary": "One concept survives.",
        "concepts": [
            {
                "title": "Missing facts",
                "worldview_summary": "bad",
                "concepts": ["bad"],
                "ontological_moves": ["bad"],
                "kill_tests": [{"description": "bad"}],
                "bridge_lemmas": ["bad"],
            },
            {
                "title": "Odd-state quotient",
                "worldview_summary": "A quotient on odd states may compress modular and odd-input facts.",
                "concepts": ["Push even transport into a derived odd-state operator."],
                "ontological_moves": ["Odd-state quotient", "Derived transfer operator"],
                "explains_facts": [
                    {
                        "fact_key": "builtin:modular_descent_mod_8",
                        "role": "explains",
                        "note": "The quotient makes mod 8 summaries structural.",
                    },
                    {
                        "fact_key": "builtin:naive_height_survives_odd_inputs",
                        "role": "compresses",
                        "note": "Odd-only survival becomes native in the quotient.",
                    },
                ],
                "tensions": [{"text": "Must still explain global invariant failure."}],
                "kill_tests": [
                    {
                        "description": "Check whether the derived odd operator respects the claimed quotient class.",
                        "expected_failure_signal": "A residue class leaves the quotient immediately.",
                        "suggested_grounding_path": "Shadow should formalize the quotient interface only.",
                    }
                ],
                "bridge_lemmas": [
                    "Define the odd-state quotient map and prove one-step compatibility."
                ],
                "reduce_frontier_or_rename": "This reduces the frontier only if the odd-only signal is predicted, not merely renamed.",
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
                        "title": "First handoff",
                        "summary": "Operationalize the quotient.",
                        "why_compressive": "Connects modular and odd-input facts.",
                        "bridge_lemmas": ["Lemma A"],
                        "shadow_task": "Build the proof program around the quotient.",
                        "recommended_next_step": "Define the quotient operator.",
                        "target_id": "should_not_survive",
                    },
                    {
                        "title": "Second handoff",
                        "summary": "This one should be capped away.",
                        "why_compressive": "Also compressive.",
                        "bridge_lemmas": ["Lemma B"],
                        "shadow_task": "Do more work.",
                        "recommended_next_step": "Another step.",
                        "objective": "should_not_survive",
                    },
                ],
            },
        ],
    }

    normalized, warnings = _normalize_supershadow_response(raw, fact_basis, max_handoffs=1)

    assert normalized["worldview_summary"] == "Try a new odd-state language."
    assert len(normalized["concepts"]) == 1
    concept = normalized["concepts"][0]
    assert concept["title"] == "Odd-state quotient"
    assert len(concept["shadow_handoffs"]) == 1
    assert "target_id" not in concept["shadow_handoffs"][0]
    assert "objective" not in concept["shadow_handoffs"][0]
    assert "concept_missing_explained_facts" in warnings
    assert "handoff_cap_applied" in warnings


def test_run_supershadow_global_lab_can_think_without_emitting_handoffs(
    tmp_path: Path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "run.db"))
    db.initialize()

    async def fake_invoke_llm(*args, **kwargs) -> str:
        return json.dumps(
            {
                "worldview_summary": "Keep searching for a compact odd-state language.",
                "run_summary": "The concept is not ready for Shadow yet.",
                "concepts": [
                    {
                        "title": "Odd-state quotient",
                        "worldview_summary": "A quotient may compress the odd-input survival phenomenon.",
                        "concepts": ["Model even transport as a derived odd-state operator."],
                        "ontological_moves": ["Odd-state quotient"],
                        "explains_facts": [
                            {"fact_key": "builtin:modular_descent_mod_8"},
                            {"fact_key": "builtin:naive_height_survives_odd_inputs"},
                        ],
                        "tensions": [{"text": "Needs to explain the global failure mode."}],
                        "kill_tests": [
                            {
                                "description": "Check quotient invariance on the first obstructing residue.",
                                "expected_failure_signal": "The residue leaves the quotient instantly.",
                                "suggested_grounding_path": "Shadow should only formalize the quotient interface.",
                            }
                        ],
                        "bridge_lemmas": [
                            "Define the odd-state quotient and prove one-step compatibility."
                        ],
                        "reduce_frontier_or_rename": "Only reduces the frontier if it predicts the odd-only invariant window.",
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
                                "title": "Formalize quotient",
                                "summary": "Shadow should operationalize this if budget allows.",
                                "why_compressive": "Connects two grounded facts with one language shift.",
                                "bridge_lemmas": ["Lemma A"],
                                "shadow_task": "Build the quotient proof program.",
                                "recommended_next_step": "Define the quotient operator.",
                            }
                        ],
                    }
                ],
            }
        )

    monkeypatch.setattr("orchestrator.supershadow_agent.invoke_llm", fake_invoke_llm)
    monkeypatch.setattr(app_config, "LLM_API_KEY", "test-key")

    result = asyncio.run(
        run_supershadow_global_lab(
            db,
            goal_text="goal",
            trigger_kind="manual",
            handoff_budget=0,
            suppress_handoffs_reason="Queue already full.",
        )
    )

    assert result["ok"] is True
    assert result["handoff_count"] == 0
    assert "handoff_budget_zero" in result["validation_warnings"]
    assert db.list_supershadow_handoff_requests(SUPERSHADOW_GLOBAL_GOAL_ID, limit=10) == []
