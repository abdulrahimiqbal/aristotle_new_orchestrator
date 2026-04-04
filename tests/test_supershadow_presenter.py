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
