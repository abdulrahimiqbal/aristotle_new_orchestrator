from __future__ import annotations

from orchestrator.shadow_presenter import build_shadow_ui_context


def test_build_shadow_ui_context_prioritizes_pending_queue() -> None:
    ctx = build_shadow_ui_context(
        hypotheses=[
            {
                "id": "h-low",
                "title": "Lower score",
                "score_0_100": 42,
                "groundability_tier": "B",
                "evidence_rows": [],
            },
            {
                "id": "h-high",
                "title": "Higher score",
                "score_0_100": 88,
                "groundability_tier": "A",
                "evidence_rows": [{"note": "e1"}],
            },
        ],
        promotions=[
            {
                "id": "p1",
                "status": "pending",
                "payload_json": (
                    '{"kind":"new_experiment","campaign_id":"c1","target_id":"t1",'
                    '"objective":"Test the strongest route","move_kind":"explore",'
                    '"proof_program_role":"bridge_lemma",'
                    '"grounding_reason":"Ground the bridge lemma before wider branching.",'
                    '"expected_signal":"If this fails, we know the proof program needs a different interface.",'
                    '"novelty_reason":"First live request in this lane.",'
                    '"rubric_total_0_15":13}'
                ),
            },
            {
                "id": "p2",
                "status": "approved",
                "payload_json": '{"kind":"new_target","campaign_id":"c1","description":"Promote a new target"}',
            },
        ],
        runs=[{"id": "r1", "summary": "A useful run", "trigger_kind": "manual"}],
    )
    assert ctx["shadow_next_step"]["title"] == "Review the live promotion queue"
    assert ctx["shadow_best_hypothesis"]["id"] == "h-high"
    assert ctx["shadow_pending_promotions"][0]["preview"]["action_label"] == "Launch live experiment"
    assert ctx["shadow_pending_promotions"][0]["preview"]["summary"] == "Ground the bridge lemma before wider branching."
    assert ctx["shadow_pending_promotions"][0]["preview"]["proof_program_role"] == "bridge_lemma"
    assert ctx["shadow_pending_promotions"][0]["preview"]["rubric_total_0_15"] == 13
    assert ctx["shadow_reviewed_promotions"][0]["preview"]["action_label"] == "Create live target"
    assert ctx["shadow_primary_cta"] == "Generate another batch"


def test_build_shadow_ui_context_handles_empty_state() -> None:
    ctx = build_shadow_ui_context(hypotheses=[], promotions=[], runs=[])
    assert ctx["shadow_next_step"]["title"] == "Generate the first idea batch"
    assert ctx["shadow_best_hypothesis"] is None
    assert ctx["shadow_pending_promotions"] == []
    assert ctx["shadow_primary_cta"] == "Generate first batch"
