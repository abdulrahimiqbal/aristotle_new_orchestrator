from __future__ import annotations

from orchestrator.mathlib_knowledge import (
    _parse_leansearch_response,
    extract_lean_symbols,
    format_library_anchors_markdown,
)


def test_extract_lean_symbols_backticks_and_dots() -> None:
    text = "unknown identifier `Foo.bar.Baz` and also Mathlib.Topology.Compactness"
    syms = extract_lean_symbols(text, max_symbols=10)
    assert "Foo.bar.Baz" in syms
    assert any("Mathlib.Topology" in s or "Compactness" in s for s in syms)


def test_extract_lean_symbols_max_zero() -> None:
    assert extract_lean_symbols("a.b.c", max_symbols=0) == []


def test_parse_leansearch_response_shape() -> None:
    body = [
        [
            {
                "result": {
                    "module_name": ["Mathlib", "Test"],
                    "kind": "theorem",
                    "name": ["Hello", "world"],
                    "type": "∀ x, True",
                    "informal_name": "Test",
                    "informal_description": "Desc",
                    "doc_url": "",
                },
                "distance": 0.1,
            }
        ]
    ]
    hits = _parse_leansearch_response(body)
    assert len(hits) == 1
    assert hits[0].name == "Hello.world"
    assert "Mathlib.Test" in hits[0].module


def test_format_library_anchors_markdown() -> None:
    md = format_library_anchors_markdown(
        [
            {
                "query": "compact space",
                "hits": [
                    {
                        "name": "T",
                        "module": "Mathlib.X",
                        "informal_name": "Compact",
                        "informal_description": "A compact space",
                        "doc_url": "",
                    }
                ],
            }
        ]
    )
    assert "compact space" in md
    assert "Mathlib.X" in md or "T" in md
