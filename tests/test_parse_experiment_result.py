from __future__ import annotations

import json
import zipfile
from pathlib import Path

from orchestrator.aristotle import (
    ExtractedArchive,
    extract_archive,
    parse_experiment_result,
    parse_result_json,
    with_synthesized_json_if_needed,
)
from orchestrator.models import Verdict


def test_parse_result_json_v1() -> None:
    raw = {
        "schema_version": 1,
        "verdict": "proved",
        "proved_lemmas": [{"label": "main"}, "extra"],
        "blockers": [],
    }
    parsed, warnings = parse_result_json(raw)
    assert parsed.verdict == Verdict.PROVED
    assert parsed.proved_lemmas == ["main", "extra"]
    assert parsed.parse_source == "json"
    assert parsed.parse_schema_version == 1
    assert isinstance(warnings, list)


def test_parse_experiment_result_prefers_json() -> None:
    js = json.dumps(
        {
            "schema_version": 1,
            "verdict": "disproved",
            "counterexamples": ["found a witness"],
        }
    )
    md = "# x\n## Completed\n- lemma foo : trivial\n"
    out = parse_experiment_result(md, js)
    assert out.verdict == Verdict.DISPROVED
    assert out.counterexamples == ["found a witness"]
    assert out.parse_source == "json"
    assert md[:20] in out.summary_text or len(out.summary_text) >= 10


def test_parse_experiment_result_json_invalid_falls_back_to_markdown() -> None:
    md = """## Completed
- lemma bar : Nat := by trivial
"""
    out = parse_experiment_result(md, "{not json")
    assert out.parse_source == "markdown"
    assert any("decode" in w.lower() for w in out.parse_warnings)


def test_shim_then_parse_is_markdown_derived() -> None:
    md = """# Summary\n## Completed\n- lemma shim_ok : True := by trivial\n"""
    bundle = with_synthesized_json_if_needed(ExtractedArchive(markdown=md))
    assert bundle.structured_json_raw
    out = parse_experiment_result(bundle.markdown, bundle.structured_json_raw)
    assert out.parse_source == "markdown_derived"
    assert out.verdict == Verdict.PROVED
    assert any("shim_ok" in x for x in out.proved_lemmas)


def test_parse_experiment_result_markdown_only() -> None:
    md = """## Partial
- sorry on goal
"""
    out = parse_experiment_result(md, None)
    assert out.parse_source == "markdown"
    assert out.verdict == Verdict.PARTIAL


def test_extract_archive_reads_json_and_md(tmp_path: Path) -> None:
    root = tmp_path / "out"
    root.mkdir()
    (root / "ARISTOTLE_SUMMARY.md").write_text("# S\n## Completed\n- ok\n", encoding="utf-8")
    (root / "aristotle_result.json").write_text(
        '{"schema_version":1,"verdict":"proved","proved_lemmas":["a"]}',
        encoding="utf-8",
    )
    zpath = tmp_path / "bundle.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        for p in root.rglob("*"):
            if p.is_file():
                zf.write(p, arcname=p.relative_to(root))

    bundle = extract_archive(zpath)
    assert "Completed" in bundle.markdown
    assert bundle.structured_json_raw
    assert "proved" in bundle.structured_json_raw


def test_extracted_archive_empty_for_missing_file(tmp_path: Path) -> None:
    bundle = extract_archive(tmp_path / "nope.bin")
    assert bundle == ExtractedArchive()
