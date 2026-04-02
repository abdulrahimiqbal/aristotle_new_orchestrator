"""Align structured verdict with dashboard summary when JSON is stuck on inconclusive."""

from __future__ import annotations

import re

from orchestrator.models import AristotleParsedResult, Verdict

# Strong success signals often present in LLM-written summaries when Aristotle succeeded
# but structured JSON still says inconclusive.
_PROVED_HINTS = [
    re.compile(r"verdict\s*:\s*✅\s*proved", re.I),
    re.compile(r"verdict\s*:\s*proved\b", re.I),
    re.compile(r"\bfully\s+verified\b", re.I),
    re.compile(r"\bproof\s+complete\b", re.I),
    re.compile(r"\bcomplete\s+proof\b", re.I),
    re.compile(r"successfully\s+proved\b", re.I),
    re.compile(r"key\s+lemma\s+['\"]", re.I),
]

# Signals we should not upgrade over
_VETO_SUBSTRINGS = (
    "counterexample found",
    "disprov",
    "refuted",
    "failed to prove",
    "could not prove",
    "still open",
    "incomplete proof",
    "proof failed",
    "build failed",
    "error:",
    "timeout",
)

# If summary explicitly says inconclusive as the final verdict, respect it
_EXPLICIT_INCONCLUSIVE = re.compile(
    r"verdict\s*:\s*(inconclusive|partial|unknown)\b", re.I
)


def reconcile_verdict_with_summary(
    parsed: AristotleParsedResult,
    result_summary: str | None,
    *,
    enabled: bool = True,
) -> AristotleParsedResult:
    """If enabled, promote INCONCLUSIVE/PARTIAL to PROVED when summary strongly agrees.

    Never overrides DISPROVED, INFRA_ERROR, or existing PROVED. Appends a parse_warning when
    reconciliation changes the verdict.
    """
    if not enabled or not (result_summary or "").strip():
        return parsed

    if parsed.verdict in (Verdict.DISPROVED, Verdict.INFRA_ERROR, Verdict.PROVED):
        return parsed

    if parsed.counterexamples:
        return parsed

    s = result_summary.strip()
    low = s.lower()

    for v in _VETO_SUBSTRINGS:
        if v in low:
            return parsed

    if _EXPLICIT_INCONCLUSIVE.search(s) and not any(h.search(s) for h in _PROVED_HINTS):
        return parsed

    if parsed.blockers and len(parsed.blockers) >= 4:
        return parsed

    if parsed.unsolved_goals and len(parsed.unsolved_goals) >= 5 and not parsed.proved_lemmas:
        return parsed

    has_hint = any(h.search(s) for h in _PROVED_HINTS)
    emoji_proved = "✅" in s and "proved" in low
    if not (has_hint or emoji_proved):
        return parsed

    # Require some structural evidence of real progress
    if not (
        parsed.proved_lemmas
        or ("lemma" in low and ("proved" in low or "established" in low))
        or ("theorem" in low and "proved" in low)
    ):
        return parsed

    warnings = list(parsed.parse_warnings)
    warnings.append("verdict_reconciled:summary_heuristic")
    return parsed.model_copy(
        update={
            "verdict": Verdict.PROVED,
            "parse_warnings": warnings,
        }
    )
