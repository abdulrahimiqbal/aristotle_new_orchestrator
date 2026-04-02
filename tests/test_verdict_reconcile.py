from __future__ import annotations

from orchestrator.models import AristotleParsedResult, Verdict
from orchestrator.verdict_reconcile import reconcile_verdict_with_summary


def test_reconcile_promotes_when_summary_and_lemmas_align() -> None:
    p = AristotleParsedResult(
        verdict=Verdict.INCONCLUSIVE,
        proved_lemmas=["collatz_pos"],
        parse_warnings=[],
    )
    s = "Verdict: ✅ Proved. Key lemma collatz_pos established; fully verified."
    out = reconcile_verdict_with_summary(p, s, enabled=True)
    assert out.verdict == Verdict.PROVED
    assert any("verdict_reconciled" in w for w in out.parse_warnings)


def test_reconcile_disabled_noop() -> None:
    p = AristotleParsedResult(verdict=Verdict.INCONCLUSIVE, proved_lemmas=["a"])
    out = reconcile_verdict_with_summary(p, "Fully verified proved", enabled=False)
    assert out.verdict == Verdict.INCONCLUSIVE


def test_reconcile_respects_counterexamples() -> None:
    p = AristotleParsedResult(
        verdict=Verdict.INCONCLUSIVE,
        counterexamples=["n=27"],
        proved_lemmas=[],
    )
    out = reconcile_verdict_with_summary(p, "Verdict: proved (oops)", enabled=True)
    assert out.verdict == Verdict.INCONCLUSIVE


def test_reconcile_needs_evidence_not_just_summary() -> None:
    p = AristotleParsedResult(verdict=Verdict.INCONCLUSIVE, proved_lemmas=[])
    out = reconcile_verdict_with_summary(
        p, "Fully verified excellent proof", enabled=True
    )
    assert out.verdict == Verdict.INCONCLUSIVE
