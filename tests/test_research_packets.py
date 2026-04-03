from __future__ import annotations

from orchestrator.research_packets import (
    format_research_packet_markdown,
    parse_research_packet,
    research_packet_json_from_input,
    select_attack_families,
)


def test_research_packet_plain_text_becomes_summary() -> None:
    raw = research_packet_json_from_input("push the constructive extension lemma")
    assert raw == '{"summary": "push the constructive extension lemma"}'
    parsed = parse_research_packet(raw)
    assert parsed["summary"] == "push the constructive extension lemma"


def test_select_attack_families_prefers_active_front_matches() -> None:
    packet = {
        "attack_families": [
            {"id": "a", "title": "A", "status": "secondary", "fronts": ["x"]},
            {"id": "b", "title": "B", "status": "primary", "fronts": ["target_1"]},
            {"id": "c", "title": "C", "status": "primary", "fronts": ["other"]},
        ]
    }
    selected = select_attack_families(packet, ["target_1"], limit=2)
    assert [row["id"] for row in selected] == ["b", "c"]


def test_format_research_packet_markdown_includes_routes_and_antigoals() -> None:
    packet = {
        "summary": "Solve the current bottleneck.",
        "current_frontier": ["target_1 is open"],
        "anti_goals": ["do not retry false union lemmas"],
        "attack_families": [
            {
                "id": "template_transfer",
                "title": "Template transfer",
                "status": "primary",
                "fronts": ["target_1"],
                "why_now": "Best route.",
                "lemma_templates": ["prove a compatibility lemma"],
            }
        ],
    }
    text = format_research_packet_markdown(packet, active_fronts=["target_1"])
    assert "Research packet" in text
    assert "Template transfer" in text
    assert "do not retry false union lemmas" in text
