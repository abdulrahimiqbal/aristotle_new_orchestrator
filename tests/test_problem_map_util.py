from __future__ import annotations

import json

from orchestrator.problem_map_util import (
    coerce_llm_problem_map,
    map_needs_init,
    map_progress_stats,
    normalize_move_kind,
    parse_problem_map,
    seed_problem_map_json,
)


def test_seed_and_parse_roundtrip() -> None:
    raw = seed_problem_map_json("Prove Fermat")
    d = parse_problem_map(raw)
    assert not map_needs_init(d)
    assert any(n.get("id") == "root" for n in d.get("nodes", []))


def test_normalize_move_kind() -> None:
    assert normalize_move_kind("promote") == "promote"
    assert normalize_move_kind("nope") == "explore"


def test_map_progress_stats() -> None:
    m = {
        "summary": "x",
        "nodes": [
            {"id": "a", "label": "A", "status": "proved"},
            {"id": "b", "label": "B", "status": "open"},
        ],
        "edges": [],
        "active_fronts": ["b"],
        "last_tick_updated": 3,
    }
    p = map_progress_stats(m)
    assert p["total_nodes"] == 2
    assert p["resolved_nodes"] == 1
    assert p["progress_percent"] == 50


def test_coerce_llm_problem_map() -> None:
    prev = json.loads(seed_problem_map_json("p"))
    out = coerce_llm_problem_map(
        {"summary": "new", "nodes": [{"id": "root", "label": "R", "status": "active"}]},
        previous=prev,
        tick_number=7,
    )
    assert out["last_tick_updated"] == 7
    assert out["summary"] == "new"
