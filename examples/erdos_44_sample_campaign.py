#!/usr/bin/env python3
"""Deterministic first-click view of the Erdős 44 research packet."""

from __future__ import annotations

import json
from pathlib import Path

from orchestrator.research_packets import (
    format_research_packet_markdown,
    parse_research_packet,
    select_attack_families,
)


ROOT = Path(__file__).resolve().parents[1]
PACKET_PATH = ROOT / "docs" / "research_packets" / "erdos_44_packet.json"


def main() -> None:
    packet = parse_research_packet(PACKET_PATH.read_text(encoding="utf-8"))
    active_fronts = ["738454173d5e", "sidon_extension_exists"]
    selected = select_attack_families(packet, active_fronts, limit=2)

    print("Erdos 44 sample campaign")
    print("========================")
    print()
    print("research question")
    print("-----------------")
    print(
        "Can the Sidon-set campaign be forced onto the constructive "
        "extension lemma instead of generic background?"
    )
    print()
    print("packet summary")
    print("--------------")
    print(packet.get("summary", "").strip())
    print()
    print("active frontier")
    print("---------------")
    for item in packet.get("current_frontier", []):
        print(f"- {item}")
    print()
    print("selected attack families")
    print("------------------------")
    for family in selected:
        print(f"- {family.get('id')}: {family.get('title')} ({family.get('status')})")
        why_now = family.get("why_now")
        if why_now:
            print(f"  why: {why_now}")
        templates = family.get("experiment_templates", [])
        if templates:
            print(f"  first experiment: {templates[0]}")
    print()
    print("anti-goals")
    print("----------")
    for item in packet.get("anti_goals", []):
        print(f"- {item}")
    print()
    print("machine-readable selection")
    print("--------------------------")
    print(
        json.dumps(
            {
                "packet": str(PACKET_PATH.relative_to(ROOT)),
                "active_fronts": active_fronts,
                "selected_attack_family_ids": [row.get("id") for row in selected],
                "next_result_to_request": (
                    "one Aristotle bundle that either proves a bad-placement "
                    "counting lemma or returns a counterexample that narrows it"
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )
    print()
    print("rendered manager context excerpt")
    print("--------------------------------")
    rendered = format_research_packet_markdown(packet, active_fronts=active_fronts)
    excerpt = "\n".join(rendered.splitlines()[:28])
    print(excerpt)


if __name__ == "__main__":
    main()
