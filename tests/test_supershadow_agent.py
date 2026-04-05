from __future__ import annotations

import asyncio
import json
from pathlib import Path

from orchestrator import config as app_config
from orchestrator.db import Database
from orchestrator.supershadow_agent import (
    SUPERSHADOW_GLOBAL_GOAL_ID,
    SUPERSHADOW_SYSTEM,
    _build_grounded_fact_basis,
    _build_supershadow_user_message,
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


def test_build_grounded_fact_basis_excludes_non_collatz_campaigns(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "facts-filter.db"))
    db.initialize()
    collatz_id = db.create_campaign(
        "collatz campaign",
        workspace_root=str(tmp_path / "ws"),
        workspace_template="minimal",
        research_packet_json=json.dumps(
            {
                "known_true": ["Collatz mod 8 descent is behaving well."],
            }
        ),
    )
    other_id = db.create_campaign(
        "erdos-straus campaign",
        workspace_root=str(tmp_path / "ws"),
        workspace_template="minimal",
        research_packet_json=json.dumps(
            {
                "known_true": ["Egyptian fraction decomposition survives prime reduction."],
            }
        ),
    )
    db.add_targets(collatz_id, ["Prove a Collatz parity lemma."])
    db.add_targets(other_id, ["Prove an Erdos-Straus denominator lemma."])

    facts = _build_grounded_fact_basis(db)
    fact_keys = {fact["fact_key"] for fact in facts}
    assert f"live:{collatz_id}:packet:known_true:0" in fact_keys
    assert f"live:{other_id}:packet:known_true:0" not in fact_keys


def test_build_supershadow_user_message_prioritizes_smallest_new_world(
    tmp_path: Path,
) -> None:
    db = Database(str(tmp_path / "prompt.db"))
    db.initialize()
    db.ensure_supershadow_state_row(
        SUPERSHADOW_GLOBAL_GOAL_ID,
        goal_text="Invent the best Collatz universe.",
    )

    user = _build_supershadow_user_message(
        db,
        "Invent the best Collatz universe.",
        [
            {
                "fact_key": "builtin:modular_descent_mod_8",
                "label": "Mod 8 descent is grounded.",
                "detail": "detail",
                "kind": "modular",
                "provenance": "builtin_seed",
            }
        ],
        [{"cluster": "modular", "fact_keys": ["builtin:modular_descent_mod_8"]}],
        [],
        handoff_budget=1,
    )

    assert "Invent the smallest new mathematical world in which Collatz is almost automatic." in SUPERSHADOW_SYSTEM
    assert "If needed, create the object first and justify it later." in SUPERSHADOW_SYSTEM
    assert "Search across any branch of math" in SUPERSHADOW_SYSTEM


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

    normalized, warnings = _normalize_supershadow_response(
        raw, fact_basis, [], max_handoffs=1
    )

    assert normalized["worldview_summary"] == "Try a new odd-state language."
    assert len(normalized["concepts"]) == 1
    concept = normalized["concepts"][0]
    assert concept["title"] == "Odd-state quotient"
    assert len(concept["shadow_handoffs"]) == 1
    assert "target_id" not in concept["shadow_handoffs"][0]
    assert "objective" not in concept["shadow_handoffs"][0]
    assert "concept_missing_explained_facts" in warnings
    assert "handoff_cap_applied" in warnings


def test_normalize_supershadow_response_prefers_new_family_and_synthesizes_probe_handoff() -> None:
    fact_basis = [
        {
            "fact_key": "builtin:modular_descent_mod_8",
            "label": "Mod 8 descent is grounded.",
            "detail": "detail",
            "kind": "modular",
            "provenance": "builtin_seed",
        },
        {
            "fact_key": "builtin:collatz_2_adic_extension",
            "label": "The 2-adic Collatz extension has been formalized.",
            "detail": "detail",
            "kind": "formalized_extension",
            "provenance": "builtin_seed",
        },
    ]
    family_memory = [
        {
            "concept_family": "odd_state_quotient",
            "family_kind": "established",
            "concept_count": 6,
            "active_incubations": 0,
            "grounded_count": 0,
            "stalled": True,
        }
    ]
    raw = {
        "worldview_summary": "Push on adjacent and new families, not just the exhausted one.",
        "run_summary": "Family-diverse pass.",
        "concepts": [
            {
                "title": "Odd-state quotient reprise",
                "concept_family": "odd_state_quotient",
                "family_kind": "established",
                "worldview_summary": "This is the stale family.",
                "concepts": ["Same lane again."],
                "ontological_moves": ["Odd quotient"],
                "explains_facts": [{"fact_key": "builtin:modular_descent_mod_8"}],
                "tensions": [{"text": "Still unclear."}],
                "kill_tests": [{"description": "Try the same thing."}],
                "bridge_lemmas": ["Lemma stale"],
                "smallest_transfer_probe": "Vague next step.",
                "reduce_frontier_or_rename": "Maybe reduce it.",
                "scores": {
                    "compression_power": 5,
                    "fit_to_known_facts": 5,
                    "bridgeability": 4,
                    "falsifiability": 4,
                    "grounding_cost": 4,
                    "speculative_risk": 3,
                    "family_novelty": 1,
                    "transfer_value": 2,
                    "family_saturation_penalty": 5,
                },
            },
            {
                "title": "Residue sheaf on odd fibers",
                "concept_family": "odd_fiber_residue_sheaf",
                "family_kind": "new",
                "worldview_summary": "A new family with a small probe.",
                "concepts": ["Treat residue behavior as local sections on odd fibers."],
                "ontological_moves": ["Odd-fiber sheaf", "Local residue gluing"],
                "explains_facts": [
                    {"fact_key": "builtin:modular_descent_mod_8"},
                    {"fact_key": "builtin:collatz_2_adic_extension"},
                ],
                "tensions": [{"text": "Needs a compatibility interface."}],
                "kill_tests": [
                    {
                        "description": "Check whether local sections glue across one odd residue obstruction.",
                        "expected_failure_signal": "The local interface breaks on a single residue class.",
                        "suggested_grounding_path": "Formalize the local interface only.",
                    }
                ],
                "bridge_lemmas": ["Define one local section map and prove single-step compatibility."],
                "smallest_transfer_probe": "Formalize a local compatibility interface on one odd residue class.",
                "reduce_frontier_or_rename": "Reduces the frontier if the interface predicts one obstructing class.",
            },
        ],
    }

    normalized, warnings = _normalize_supershadow_response(
        raw, fact_basis, family_memory, max_handoffs=2
    )

    assert len(normalized["concepts"]) == 1
    best = normalized["concepts"][0]
    assert best["concept_family"] == "odd_fiber_residue_sheaf"
    assert best["family_kind"] == "new"
    assert best["scores"]["transfer_value"] >= 4
    assert len(best["shadow_handoffs"]) == 1
    assert "handoff_synthesized_from_probe" in warnings
    assert "stale_family_suppressed" in warnings


def test_normalize_supershadow_response_keeps_discovery_concept_without_bridge_lemma() -> None:
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
        "worldview_summary": "Keep the board focused on one live idea.",
        "run_summary": "One bridge-free concept survives discovery.",
        "concepts": [
            {
                "title": "Odd-state split without bridge",
                "concept_family": "odd_state_split",
                "family_kind": "new",
                "worldview_summary": "Odd dynamics may need their own ambient state space before any formal interface is clear.",
                "concepts": ["Treat even transport as derived structure rather than native state."],
                "ontological_moves": ["Odd-state split", "Derived even transport"],
                "explains_facts": [
                    {"fact_key": "builtin:modular_descent_mod_8"},
                    {"fact_key": "builtin:naive_height_survives_odd_inputs"},
                ],
                "kill_tests": [
                    {
                        "description": "Check whether the induced odd-only update rule preserves the proposed state class.",
                        "expected_failure_signal": "The class breaks on the first odd residue obstruction.",
                    }
                ],
                "smallest_transfer_probe": "Find the first odd-state obstruction class.",
            }
        ],
    }

    normalized, warnings = _normalize_supershadow_response(
        raw, fact_basis, [], max_handoffs=0
    )

    assert len(normalized["concepts"]) == 1
    assert normalized["concepts"][0]["title"] == "Odd-state split without bridge"
    assert normalized["concepts"][0]["bridge_lemmas"] == []
    assert "concept_missing_bridge_lemmas" in warnings


def test_normalize_supershadow_response_preserves_super_universe_candidate() -> None:
    fact_basis = [
        {
            "fact_key": "builtin:modular_descent_mod_8",
            "label": "Mod 8 descent is grounded.",
            "detail": "detail",
            "kind": "modular",
            "provenance": "builtin_seed",
        },
        {
            "fact_key": "builtin:collatz_2_adic_extension",
            "label": "The 2-adic Collatz extension has been formalized.",
            "detail": "detail",
            "kind": "formalized_extension",
            "provenance": "builtin_seed",
        },
    ]
    raw = {
        "worldview_summary": "One universe is surviving repeated internal attacks.",
        "run_summary": "Promote the strongest survivor for review.",
        "concepts": [
            {
                "title": "Odd grammar completion",
                "concept_family": "odd_grammar_completion",
                "family_kind": "new",
                "branch_of_math": "symbolic dynamics",
                "worldview_summary": "Treat odd trajectories as words in a constrained grammar.",
                "solved_world": "Collatz is a grammar theorem forbidding admissible bad infinite words.",
                "why_collatz_is_easy_here": "Admissibility rules force negative drift on every legal infinite word.",
                "universe_thesis": "A grammar on odd states may forbid all nontrivial infinite words.",
                "conditional_theorem": "If every admissible infinite odd word has negative drift, Collatz follows.",
                "concepts": ["Encode odd trajectories as admissible words."],
                "fundamental_entities": [
                    "Admissible odd-word alphabet",
                    "Drift functional",
                ],
                "ontological_moves": ["Odd-word grammar", "Drift functional"],
                "backward_translation": [
                    "Map each odd iterate to a grammar letter using its residue and valuation."
                ],
                "explains_facts": [
                    {"fact_key": "builtin:modular_descent_mod_8"},
                    {"fact_key": "builtin:collatz_2_adic_extension"},
                ],
                "kill_tests": [
                    {
                        "description": "Find an admissible infinite odd word with nonnegative drift.",
                        "expected_failure_signal": "A legal word survives every local drift bound.",
                    }
                ],
                "self_test_results": [
                    {
                        "attack": "Try to build a legal infinite odd word that evades the drift bound.",
                        "result": "survived",
                        "note": "The grammar appears to ban the obvious constructions.",
                    },
                    {
                        "attack": "Push the universe against the formal 2-adic extension.",
                        "result": "strengthened",
                        "note": "The completion suggests a natural ambient space for the grammar.",
                    },
                ],
                "signs_of_life": [
                    "The same grammar language compresses both odd-input structure and the 2-adic anchor.",
                    "The drift claim sharpens into a theorem-shaped statement rather than a slogan.",
                ],
                "negative_signs": [
                    "It is still unclear whether every legal word corresponds to a real orbit."
                ],
                "universe_status": "super_candidate",
                "invention_lesson": "Grammar universes improve when they produce a drift theorem quickly.",
                "bridge_lemmas": [
                    "Define admissible odd words and prove one-step compatibility with T."
                ],
                "smallest_transfer_probe": "Formalize one admissible-word compatibility lemma.",
                "super_universe_candidate": {
                    "why_now": "This is the first universe that survives multiple attacks and still names a narrow formal probe.",
                    "survived_attacks": [
                        "Could not produce a legal infinite odd word with nonnegative drift.",
                        "The 2-adic completion did not break the grammar interpretation.",
                    ],
                    "full_fact_audit": {
                        "explains": [
                            "builtin:modular_descent_mod_8",
                            "builtin:collatz_2_adic_extension",
                        ],
                        "awkward": [],
                    },
                    "smallest_aristotle_probe": "Formalize the admissible-word interface and test one local drift lemma.",
                },
            }
        ],
    }

    normalized, warnings = _normalize_supershadow_response(
        raw, fact_basis, [], max_handoffs=1
    )

    assert len(normalized["concepts"]) == 1
    concept = normalized["concepts"][0]
    assert concept["universe_status"] == "super_candidate"
    assert concept["branch_of_math"] == "symbolic dynamics"
    assert concept["solved_world"].startswith("Collatz is a grammar theorem")
    assert concept["why_collatz_is_easy_here"].startswith("Admissibility rules force")
    assert concept["fundamental_entities"] == [
        "Admissible odd-word alphabet",
        "Drift functional",
    ]
    assert concept["backward_translation"][0].startswith(
        "Map each odd iterate to a grammar letter"
    )
    assert len(concept["self_test_results"]) == 2
    assert len(concept["signs_of_life"]) == 2
    assert concept["super_universe_candidate"]["why_now"].startswith("This is the first universe")
    assert len(concept["shadow_handoffs"]) == 1
    assert concept["shadow_handoffs"][0]["review_kind"] == "super_universe_candidate"
    assert "super_universe_downgraded" not in warnings


def test_normalize_supershadow_response_blocks_family_on_cooldown() -> None:
    fact_basis = [
        {
            "fact_key": "builtin:modular_descent_mod_8",
            "label": "Mod 8 descent is grounded.",
            "detail": "detail",
            "kind": "modular",
            "provenance": "builtin_seed",
        }
    ]
    family_memory = [
        {
            "concept_family": "graded_2_adic_module",
            "family_kind": "established",
            "concept_count": 8,
            "active_incubations": 0,
            "grounded_count": 0,
            "stalled": True,
            "cooldown_runs_remaining": 2,
            "recent_titles": ["Graded 2-adic module with descent filtration"],
        }
    ]
    raw = {
        "worldview_summary": "Do not allow the stale family back immediately.",
        "run_summary": "Cooldown should block the family.",
        "concepts": [
            {
                "title": "Graded 2-adic module with better wording",
                "concept_family": "graded_2_adic_module",
                "family_kind": "established",
                "worldview_summary": "Same family during cooldown.",
                "concepts": ["Restate the filtration idea."],
                "ontological_moves": ["2-adic filtration"],
                "explains_facts": [{"fact_key": "builtin:modular_descent_mod_8"}],
                "kill_tests": [{"description": "Try the same family again."}],
                "bridge_lemmas": ["Lemma stale"],
                "smallest_transfer_probe": "Another bounded filtration probe.",
            }
        ],
    }

    normalized, warnings = _normalize_supershadow_response(
        raw, fact_basis, family_memory, max_handoffs=0
    )

    assert normalized["concepts"] == []
    assert "family_cooldown_active" in warnings
    assert "stale_family_suppressed" in warnings


def test_normalize_supershadow_response_blocks_stale_exact_title_repeats() -> None:
    fact_basis = [
        {
            "fact_key": "builtin:modular_descent_mod_8",
            "label": "Mod 8 descent is grounded.",
            "detail": "detail",
            "kind": "modular",
            "provenance": "builtin_seed",
        }
    ]
    family_memory = [
        {
            "concept_family": "graded_2_adic_module",
            "family_kind": "established",
            "concept_count": 12,
            "active_incubations": 0,
            "grounded_count": 0,
            "stalled": True,
            "recent_titles": ["Graded 2-adic module with descent filtration"],
        }
    ]
    raw = {
        "worldview_summary": "Avoid circling stale families.",
        "run_summary": "One stale repeat, one adjacent escape hatch.",
        "concepts": [
            {
                "title": "Graded 2-adic module with descent filtration",
                "concept_family": "graded_2_adic_module",
                "family_kind": "established",
                "worldview_summary": "Same title, same family, no new transfer path.",
                "concepts": ["Repeat the same filtration story."],
                "ontological_moves": ["2-adic filtration"],
                "explains_facts": [{"fact_key": "builtin:modular_descent_mod_8"}],
                "kill_tests": [{"description": "Repeat the old filter."}],
                "bridge_lemmas": ["Lemma stale"],
                "smallest_transfer_probe": "Try it again.",
            },
            {
                "title": "Filtered odd-fiber module",
                "concept_family": "odd_fiber_filtered_module",
                "family_kind": "adjacent",
                "parent_family": "graded_2_adic_module",
                "why_not_same_as_existing_family": "Moves the descent story onto odd fibers and adds a bounded compatibility probe.",
                "worldview_summary": "Adjacent family with a cheaper probe.",
                "concepts": ["Shift the filtration onto odd fibers only."],
                "ontological_moves": ["Odd-fiber filtration"],
                "explains_facts": [{"fact_key": "builtin:modular_descent_mod_8"}],
                "tensions": [{"text": "Needs one compatibility bridge."}],
                "kill_tests": [
                    {
                        "description": "Check compatibility on one odd residue class.",
                        "expected_failure_signal": "Odd residue transport breaks immediately.",
                        "suggested_grounding_path": "Formalize one bounded odd-fiber interface.",
                    }
                ],
                "bridge_lemmas": ["Define the odd-fiber filtration interface."],
                "smallest_transfer_probe": "Formalize a bounded odd-fiber compatibility interface on one residue class.",
                "reduce_frontier_or_rename": "Reduces the frontier if the odd-fiber interface isolates one residue obstruction.",
            },
        ],
    }

    normalized, warnings = _normalize_supershadow_response(
        raw, fact_basis, family_memory, max_handoffs=2
    )

    assert [concept["concept_family"] for concept in normalized["concepts"]] == [
        "odd_fiber_filtered_module"
    ]
    assert "stale_family_suppressed" in warnings


def test_run_supershadow_global_lab_can_think_without_emitting_handoffs(
    tmp_path: Path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "run.db"))
    db.initialize()
    calls = {"count": 0}

    async def fake_invoke_llm(*args, **kwargs) -> str:
        calls["count"] += 1
        if calls["count"] == 1:
            return json.dumps(
                {
                    "worldview_summary": "Keep searching for a compact odd-state language.",
                    "run_summary": "The concept is promising but still underdistilled.",
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
                            "smallest_transfer_probe": "Identify the first obstructing odd residue class.",
                            "reduce_frontier_or_rename": "Only reduces the frontier if it predicts the odd-only invariant window.",
                            "scores": {
                                "compression_power": 5,
                                "fit_to_known_facts": 5,
                                "ontological_delta": 4,
                                "falsifiability": 4,
                                "bridgeability": 1,
                                "grounding_cost": 2,
                                "speculative_risk": 2,
                            },
                        }
                    ],
                }
            )
        return json.dumps(
            {
                "worldview_summary": "Distill the quotient into one falsifier and one bridge.",
                "run_summary": "The concept is not ready for Shadow yet.",
                "concepts": [
                    {
                        "title": "Odd-state quotient",
                        "concept_family": "odd_state_quotient",
                        "family_kind": "new",
                        "worldview_summary": "Sharpen the quotient into a single falsifiable interface claim.",
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
                        "smallest_transfer_probe": "Formalize the quotient operator on one odd residue class.",
                        "reduce_frontier_or_rename": "Only reduces the frontier if it predicts the odd-only invariant window.",
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
    assert calls["count"] == 2
    assert result["handoff_count"] == 0
    assert "handoff_budget_zero" in result["validation_warnings"]
    assert db.list_supershadow_handoff_requests(SUPERSHADOW_GLOBAL_GOAL_ID, limit=10) == []


def test_run_supershadow_global_lab_distills_top_concept_before_handoff(
    tmp_path: Path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "distill.db"))
    db.initialize()
    calls = {"count": 0}

    async def fake_invoke_llm(*args, **kwargs) -> str:
        calls["count"] += 1
        if calls["count"] == 1:
            return json.dumps(
                {
                    "worldview_summary": "One odd-state line is clearly strongest.",
                    "run_summary": "Discovery keeps one dominant candidate and one backup.",
                    "concepts": [
                        {
                            "title": "Odd-state quotient",
                            "concept_family": "odd_state_quotient",
                            "family_kind": "new",
                            "worldview_summary": "A quotient may make modular and odd-input behavior structural at once.",
                            "concepts": ["Push even transport into a derived odd-state operator."],
                            "ontological_moves": ["Odd-state quotient"],
                            "explains_facts": [
                                {"fact_key": "builtin:modular_descent_mod_8"},
                                {"fact_key": "builtin:naive_height_survives_odd_inputs"},
                            ],
                            "tensions": [{"text": "Still must explain the global height failure."}],
                            "kill_tests": [
                                {
                                    "description": "Check whether the derived odd operator preserves the proposed quotient class.",
                                    "expected_failure_signal": "The quotient breaks on the first obstructing residue.",
                                }
                            ],
                            "smallest_transfer_probe": "Locate the first obstructing odd residue class.",
                        },
                        {
                            "title": "Backup lens",
                            "concept_family": "backup_lens",
                            "family_kind": "adjacent",
                            "worldview_summary": "A weaker backup language.",
                            "concepts": ["Backup."],
                            "ontological_moves": ["Backup move"],
                            "explains_facts": [{"fact_key": "builtin:modular_descent_mod_8"}],
                            "kill_tests": [{"description": "Try it."}],
                            "bridge_lemmas": ["Backup lemma"],
                        },
                    ],
                }
            )
        return json.dumps(
            {
                "worldview_summary": "Distill the dominant line only.",
                "run_summary": "The quotient now has one sharp bridge.",
                "concepts": [
                    {
                        "title": "Odd-state quotient",
                        "concept_family": "odd_state_quotient",
                        "family_kind": "new",
                        "worldview_summary": "A quotient may make modular and odd-input behavior structural at once.",
                        "concepts": ["Push even transport into a derived odd-state operator."],
                        "ontological_moves": ["Odd-state quotient"],
                        "explains_facts": [
                            {"fact_key": "builtin:modular_descent_mod_8"},
                            {"fact_key": "builtin:naive_height_survives_odd_inputs"},
                        ],
                        "tensions": [{"text": "Still must explain the global height failure."}],
                        "kill_tests": [
                            {
                                "description": "Check whether the derived odd operator preserves the proposed quotient class.",
                                "expected_failure_signal": "The quotient breaks on the first obstructing residue.",
                                "suggested_grounding_path": "Formalize the quotient interface on one residue class.",
                            }
                        ],
                        "bridge_lemmas": [
                            "Define the odd-state quotient and prove one-step compatibility."
                        ],
                        "smallest_transfer_probe": "Formalize the quotient operator on one odd residue class.",
                        "shadow_handoffs": [
                            {
                                "title": "Distill the odd-state quotient",
                                "summary": "Shadow should pressure-test the first bridge lemma.",
                                "why_compressive": "This one line explains modular and odd-input structure together.",
                                "bridge_lemmas": [
                                    "Define the odd-state quotient and prove one-step compatibility."
                                ],
                                "shadow_task": "Build the proof program around the quotient interface.",
                                "recommended_next_step": "Formalize the quotient operator on one residue class.",
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
            handoff_budget=1,
        )
    )

    assert result["ok"] is True
    assert calls["count"] == 2
    assert result["handoff_count"] == 1
    handoffs = db.list_supershadow_handoff_requests(SUPERSHADOW_GLOBAL_GOAL_ID, limit=10)
    assert len(handoffs) == 1
    concepts = db.list_supershadow_concepts(SUPERSHADOW_GLOBAL_GOAL_ID, limit=10)
    assert concepts[0]["title"] == "Odd-state quotient"


def test_run_supershadow_global_lab_updates_universe_memory(
    tmp_path: Path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "universe-memory.db"))
    db.initialize()
    calls = {"count": 0}

    async def fake_invoke_llm(*args, **kwargs) -> str:
        calls["count"] += 1
        if calls["count"] == 1:
            return json.dumps(
                {
                    "worldview_summary": "Invent one bold universe and one backup.",
                    "run_summary": "Discovery found one alive grammar universe.",
                    "concepts": [
                        {
                            "title": "Odd grammar completion",
                            "concept_family": "odd_grammar_completion",
                            "family_kind": "new",
                            "branch_of_math": "symbolic dynamics",
                            "worldview_summary": "Treat odd trajectories as words in a constrained grammar.",
                            "solved_world": "Collatz is a grammar theorem forbidding admissible bad infinite words.",
                            "why_collatz_is_easy_here": "Admissibility rules force negative drift on every legal infinite word.",
                            "universe_thesis": "A grammar on odd states may forbid all nontrivial infinite words.",
                            "conditional_theorem": "If every admissible infinite odd word has negative drift, Collatz follows.",
                            "concepts": ["Encode odd trajectories as admissible words."],
                            "fundamental_entities": [
                                "Admissible odd-word alphabet",
                                "Drift functional",
                            ],
                            "ontological_moves": ["Odd-word grammar", "Drift functional"],
                            "backward_translation": [
                                "Map each odd iterate to a grammar letter via its residue and valuation."
                            ],
                            "explains_facts": [
                                {"fact_key": "builtin:modular_descent_mod_8"},
                                {"fact_key": "builtin:collatz_2_adic_extension"},
                            ],
                            "kill_tests": [
                                {
                                    "description": "Find an admissible infinite odd word with nonnegative drift.",
                                    "expected_failure_signal": "A legal word survives every local drift bound.",
                                }
                            ],
                            "smallest_transfer_probe": "Formalize one admissible-word compatibility lemma.",
                        }
                    ],
                }
            )
        return json.dumps(
            {
                "worldview_summary": "Self-test the grammar universe and keep only the survivor.",
                "run_summary": "The grammar universe survived multiple attacks and now looks like a super candidate.",
                "concepts": [
                    {
                        "title": "Odd grammar completion",
                        "concept_family": "odd_grammar_completion",
                        "family_kind": "new",
                        "branch_of_math": "symbolic dynamics",
                        "worldview_summary": "Treat odd trajectories as words in a constrained grammar.",
                        "solved_world": "Collatz is a grammar theorem forbidding admissible bad infinite words.",
                        "why_collatz_is_easy_here": "Admissibility rules force negative drift on every legal infinite word.",
                        "universe_thesis": "A grammar on odd states may forbid all nontrivial infinite words.",
                        "conditional_theorem": "If every admissible infinite odd word has negative drift, Collatz follows.",
                        "concepts": ["Encode odd trajectories as admissible words."],
                        "fundamental_entities": [
                            "Admissible odd-word alphabet",
                            "Drift functional",
                        ],
                        "ontological_moves": ["Odd-word grammar", "Drift functional"],
                        "backward_translation": [
                            "Map each odd iterate to a grammar letter via its residue and valuation."
                        ],
                        "explains_facts": [
                            {"fact_key": "builtin:modular_descent_mod_8"},
                            {"fact_key": "builtin:collatz_2_adic_extension"},
                        ],
                        "kill_tests": [
                            {
                                "description": "Find an admissible infinite odd word with nonnegative drift.",
                                "expected_failure_signal": "A legal word survives every local drift bound.",
                                "suggested_grounding_path": "Formalize the admissible-word interface first.",
                            }
                        ],
                        "self_test_results": [
                            {
                                "attack": "Try to build an admissible infinite odd word.",
                                "result": "survived",
                                "note": "The local grammar keeps rejecting obvious counterexamples.",
                            },
                            {
                                "attack": "Push the universe against the 2-adic anchor.",
                                "result": "strengthened",
                                "note": "The completion suggests a natural ambient space.",
                            },
                        ],
                        "signs_of_life": [
                            "The same universe compresses odd-input structure and the 2-adic anchor.",
                            "The drift statement is theorem-shaped and locally falsifiable.",
                        ],
                        "negative_signs": [
                            "The grammar-to-orbit correspondence is still incomplete."
                        ],
                        "universe_status": "super_candidate",
                        "invention_lesson": "Grammar universes should be kept only when they quickly yield a drift theorem.",
                        "bridge_lemmas": [
                            "Define admissible odd words and prove one-step compatibility with T."
                        ],
                        "smallest_transfer_probe": "Formalize one admissible-word compatibility lemma.",
                        "super_universe_candidate": {
                            "why_now": "This universe survived the strongest local attacks and still has a tiny formal probe.",
                            "survived_attacks": [
                                "Could not produce an admissible infinite odd word with nonnegative drift.",
                                "The 2-adic anchor strengthened the universe instead of breaking it.",
                            ],
                            "full_fact_audit": {
                                "explains": [
                                    "builtin:modular_descent_mod_8",
                                    "builtin:collatz_2_adic_extension",
                                ],
                                "awkward": [],
                            },
                            "smallest_aristotle_probe": "Formalize the admissible-word interface and test one local drift lemma.",
                        },
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
            handoff_budget=1,
        )
    )

    assert result["ok"] is True
    assert calls["count"] == 2
    state = db.get_supershadow_state(SUPERSHADOW_GLOBAL_GOAL_ID)
    policy = json.loads(state["policy_json"])
    memory = policy["_supershadow_universe_memory"]["odd_grammar_completion"]
    assert memory["status"] == "super_candidate"
    assert memory["branch_of_math"] == "symbolic dynamics"
    assert memory["solved_world"].startswith("Collatz is a grammar theorem")
    assert memory["fundamental_entities"] == [
        "Admissible odd-word alphabet",
        "Drift functional",
    ]
    assert memory["tests_run"] == 2
    assert memory["super_candidate_runs"] == 1
    assert policy["_supershadow_invention_lessons_tail"][-1].startswith(
        "Grammar universes should be kept only"
    )
    concepts = db.list_supershadow_concepts(SUPERSHADOW_GLOBAL_GOAL_ID, limit=10)
    assert concepts[0]["universe_status"] == "super_candidate"
    assert concepts[0]["branch_of_math"] == "symbolic dynamics"
    assert concepts[0]["solved_world"].startswith("Collatz is a grammar theorem")
