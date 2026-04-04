from __future__ import annotations

from orchestrator.supershadow_presenter import build_supershadow_ui_context


def test_supershadow_presenter_prefers_compression_over_delta() -> None:
    ctx = build_supershadow_ui_context(
        concepts=[
            {
                "id": "c-low-compression",
                "title": "High delta, low compression",
                "concepts_json": '["Wild renaming"]',
                "ontological_moves_json": '["Big categorical jump"]',
                "bridge_lemmas_json": '["Hard bridge"]',
                "compression_power": 2,
                "fit_to_known_facts": 2,
                "ontological_delta": 5,
                "falsifiability": 3,
                "bridgeability": 2,
                "grounding_cost": 4,
                "speculative_risk": 4,
                "fact_links": [{"fact_label": "One fact"}],
                "tensions": [],
                "kill_tests": [{"description": "Try to refute it"}],
            },
            {
                "id": "c-high-compression",
                "title": "More compressive",
                "concepts_json": '["Explains several facts"]',
                "ontological_moves_json": '["Odd-state quotient"]',
                "bridge_lemmas_json": '["Define the quotient operator"]',
                "compression_power": 5,
                "fit_to_known_facts": 5,
                "ontological_delta": 3,
                "falsifiability": 4,
                "bridgeability": 4,
                "grounding_cost": 2,
                "speculative_risk": 2,
                "fact_links": [{"fact_label": "Fact A"}, {"fact_label": "Fact B"}],
                "tensions": [{"tension_text": "Needs a clean odd/even boundary."}],
                "kill_tests": [{"description": "Check the induced odd operator."}],
            },
        ],
        handoffs=[
            {
                "id": "h1",
                "status": "pending",
                "payload_json": (
                    '{"title":"Handoff title","summary":"Shadow should operationalize this.",'
                    '"why_compressive":"Explains multiple facts at once.",'
                    '"bridge_lemmas":["Lemma A"],'
                    '"shadow_task":"Build the proof program.",'
                    '"recommended_next_step":"Start with Lemma A."}'
                ),
            }
        ],
        incubations=[
            {
                "id": "i1",
                "status": "operationalized",
                "title": "Odd-state quotient incubation",
                "concept_packet_json": (
                    '{"title":"Odd-state quotient incubation","summary":"Shadow is translating the concept.",'
                    '"why_compressive":"Explains multiple facts at once.",'
                    '"bridge_lemmas":["Lemma A"],'
                    '"shadow_task":"Operationalize it."}'
                ),
                "grounded_promotion_ids_json": '[]',
                "events": [
                    {
                        "event_kind": "shadow_operationalized",
                        "event_summary": "Shadow operationalized it.",
                    }
                ],
            }
        ],
        runs=[{"id": "r1", "run_summary": "Conceptual sweep", "trigger_kind": "manual"}],
    )
    assert ctx["supershadow_best_concept"]["id"] == "c-high-compression"
    assert ctx["supershadow_pending_handoffs"][0]["preview"]["action_label"] == "Handoff to Shadow"
    assert ctx["supershadow_next_step"]["title"] == "Review the Shadow handoff queue"
    assert ctx["supershadow_active_incubations"][0]["preview"]["status"] == "operationalized"


def test_supershadow_presenter_handles_empty_state() -> None:
    ctx = build_supershadow_ui_context(concepts=[], handoffs=[], incubations=[], runs=[])
    assert ctx["supershadow_best_concept"] is None
    assert ctx["supershadow_pending_handoffs"] == []
    assert ctx["supershadow_active_incubations"] == []
    assert ctx["supershadow_primary_cta"] == "Generate first sweep"


def test_supershadow_presenter_surfaces_super_universe_review() -> None:
    ctx = build_supershadow_ui_context(
        concepts=[
            {
                "id": "c-super",
                "title": "Odd grammar completion",
                "concepts_json": '["Encode odd trajectories as admissible words."]',
                "fundamental_entities_json": '["Admissible odd-word alphabet","Drift functional"]',
                "ontological_moves_json": '["Odd-word grammar"]',
                "backward_translation_json": '["Map each odd iterate to a grammar letter via its residue and valuation."]',
                "bridge_lemmas_json": '["Define admissible odd words."]',
                "self_test_results_json": '[{"attack":"Try to build a bad word","result":"survived","note":"The grammar rejects it."}]',
                "signs_of_life_json": '["The drift statement became theorem-shaped."]',
                "negative_signs_json": '["Orbit correspondence is still incomplete."]',
                "super_universe_json": '{"why_now":"This survived multiple attacks.","survived_attacks":["Attack A"],"full_fact_audit":{"explains":[],"awkward":[]},"smallest_aristotle_probe":"Formalize one drift lemma."}',
                "branch_of_math": "symbolic dynamics",
                "solved_world": "Collatz becomes a grammar theorem forbidding admissible bad infinite words.",
                "why_collatz_is_easy_here": "Negative drift is baked into the admissibility rules.",
                "universe_status": "super_candidate",
                "universe_thesis": "A grammar on odd states may forbid bad infinite words.",
                "conditional_theorem": "If every admissible infinite odd word has negative drift, Collatz follows.",
                "compression_power": 5,
                "fit_to_known_facts": 5,
                "ontological_delta": 4,
                "falsifiability": 4,
                "bridgeability": 4,
                "grounding_cost": 3,
                "speculative_risk": 3,
                "fact_links": [{"fact_label": "Fact A"}],
                "tensions": [],
                "kill_tests": [{"description": "Find a bad admissible word"}],
            }
        ],
        handoffs=[
            {
                "id": "h-super",
                "status": "pending",
                "payload_json": (
                    '{"title":"Super-universe review: Odd grammar completion",'
                    '"summary":"Review whether this universe deserves scarce grounding.",'
                    '"why_compressive":"It compresses multiple facts while surviving self-tests.",'
                    '"bridge_lemmas":["Define admissible odd words."],'
                    '"shadow_task":"Review the first bridge and the narrow Aristotle probe.",'
                    '"recommended_next_step":"Check the admissible-word interface.",'
                    '"review_kind":"super_universe_candidate",'
                    '"super_universe_candidate":{"why_now":"This survived multiple attacks.","survived_attacks":["Attack A"],"full_fact_audit":{"explains":[],"awkward":[]},"smallest_aristotle_probe":"Formalize one drift lemma."}}'
                ),
            }
        ],
        incubations=[],
        runs=[{"id": "r1", "run_summary": "Universe sweep", "trigger_kind": "manual"}],
    )

    assert ctx["supershadow_best_concept"]["universe_status"] == "super_candidate"
    assert ctx["supershadow_best_concept"]["branch_of_math"] == "symbolic dynamics"
    assert ctx["supershadow_best_concept"]["fundamental_entities"] == [
        "Admissible odd-word alphabet",
        "Drift functional",
    ]
    assert ctx["supershadow_best_concept"]["backward_translation"][0].startswith(
        "Map each odd iterate"
    )
    assert (
        ctx["supershadow_pending_handoffs"][0]["preview"]["action_label"]
        == "Super-universe review"
    )
