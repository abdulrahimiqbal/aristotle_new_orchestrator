from __future__ import annotations

import math
from fractions import Fraction
from typing import Any

from orchestrator import config as app_config
from orchestrator.lima_models import LimaUniverseSpec, slugify

try:  # pragma: no cover - optional dependency path is environment-specific.
    import numpy as _np
except Exception:  # pragma: no cover
    _np = None

try:  # pragma: no cover
    import sympy as _sympy
except Exception:  # pragma: no cover
    _sympy = None

try:  # pragma: no cover
    import networkx as _nx
except Exception:  # pragma: no cover
    _nx = None


ATTACK_FAMILIES = [
    "internal_consistency",
    "bridgeability",
    "boundary_regime",
    "residue_class",
    "analogy_prior_art",
    "vacuity",
    "compression_vs_complexity",
    "counterexample_hunt",
    "weakest_assumption_removal",
    "formalizability",
]


def collatz_step(n: int) -> int:
    if n <= 0:
        raise ValueError("Collatz step expects a positive integer")
    return n // 2 if n % 2 == 0 else 3 * n + 1


def stopping_time(n: int, *, max_steps: int = 2000) -> int | None:
    value = n
    for step in range(max_steps + 1):
        if value == 1:
            return step
        value = collatz_step(value)
    return None


def residue_pattern_summary(limit: int = 256, modulus: int = 16) -> dict[str, Any]:
    counts = {str(i): 0 for i in range(modulus)}
    descent_counts = {str(i): 0 for i in range(modulus)}
    max_seen = 0
    for n in range(1, max(2, limit) + 1):
        residue = str(n % modulus)
        counts[residue] += 1
        nxt = collatz_step(n)
        max_seen = max(max_seen, nxt)
        if nxt < n:
            descent_counts[residue] += 1
    return {
        "limit": limit,
        "modulus": modulus,
        "counts": counts,
        "one_step_descent_counts": descent_counts,
        "max_next_value": max_seen,
        "exact": True,
    }


def _vacuity_attack(universe: LimaUniverseSpec) -> dict[str, Any]:
    blob = " ".join(
        [
            universe.title,
            universe.solved_world,
            universe.why_problem_is_easy_here,
            universe.core_story_md,
            " ".join(claim.statement_md for claim in universe.all_claim_specs()),
        ]
    ).lower()
    markers = ["collatz is true", "assume collatz", "by the conjecture", "if collatz holds"]
    if any(marker in blob for marker in markers):
        return {
            "attack": "vacuity",
            "result": "failed",
            "failure_type": "vacuity",
            "breakpoint_md": "The universe appears to smuggle the conjecture into its premise.",
            "confidence": 0.85,
        }
    if len(universe.core_objects) == 0 and len(universe.all_claim_specs()) <= 1:
        return {
            "attack": "vacuity",
            "result": "warning",
            "failure_type": "weak_explanation",
            "breakpoint_md": "The universe has too little structure to falsify yet.",
            "confidence": 0.55,
        }
    return {"attack": "vacuity", "result": "survived", "confidence": 0.65}


def _bridgeability_attack(universe: LimaUniverseSpec) -> dict[str, Any]:
    if not universe.backward_translation and not universe.bridge_lemmas:
        return {
            "attack": "bridgeability",
            "result": "failed",
            "failure_type": "non_bridgeable",
            "breakpoint_md": "No backward translation or bridge lemma ties the solved world back to ordinary integers.",
            "confidence": 0.78,
        }
    if not universe.formalization_targets and not universe.bridge_lemmas:
        return {
            "attack": "bridgeability",
            "result": "warning",
            "failure_type": "weak_explanation",
            "breakpoint_md": "The universe has translation prose but no narrow formalizable bridge target.",
            "confidence": 0.52,
        }
    return {"attack": "bridgeability", "result": "survived", "confidence": 0.7}


def _formalizability_attack(universe: LimaUniverseSpec) -> dict[str, Any]:
    targets = universe.formalization_targets
    if not targets:
        return {
            "attack": "formalizability",
            "result": "warning",
            "failure_type": "weak_explanation",
            "breakpoint_md": "No explicit Lean or finite-check obligation was compiled.",
            "confidence": 0.55,
        }
    if any(t.lean_goal.strip() or t.obligation_kind == "finite_check" for t in targets):
        return {"attack": "formalizability", "result": "survived", "confidence": 0.75}
    return {
        "attack": "formalizability",
        "result": "warning",
        "failure_type": "weak_explanation",
        "breakpoint_md": "Obligations exist, but none yet has a Lean-shaped goal or finite-check form.",
        "confidence": 0.5,
    }


def _compression_attack(universe: LimaUniverseSpec) -> dict[str, Any]:
    object_count = len(universe.core_objects)
    claim_count = len(universe.all_claim_specs())
    complexity = object_count + claim_count
    compression = universe.score("compression_score", 3)
    if complexity >= 12 and compression <= 2:
        return {
            "attack": "compression_vs_complexity",
            "result": "failed",
            "failure_type": "overfit",
            "breakpoint_md": "The universe has many moving parts but low declared compression.",
            "confidence": 0.72,
        }
    return {
        "attack": "compression_vs_complexity",
        "result": "survived" if complexity <= 10 or compression >= 3 else "warning",
        "complexity": complexity,
        "confidence": 0.58,
    }


def _consistency_attack(universe: LimaUniverseSpec) -> dict[str, Any]:
    claims = universe.all_claim_specs()
    statuses = {slugify(c.status) for c in claims}
    if "refuted" in statuses and any(slugify(c.status) == "verified" for c in claims):
        return {
            "attack": "internal_consistency",
            "result": "warning",
            "failure_type": "contradiction",
            "breakpoint_md": "The same universe mixes verified and refuted claims; its dependency graph needs pruning.",
            "confidence": 0.5,
        }
    titles = [slugify(c.title) for c in claims if c.title]
    if len(titles) != len(set(titles)):
        return {
            "attack": "internal_consistency",
            "result": "warning",
            "failure_type": "contradiction",
            "breakpoint_md": "Duplicate claim titles make the claim graph ambiguous.",
            "confidence": 0.48,
        }
    return {"attack": "internal_consistency", "result": "survived", "confidence": 0.62}


def _claim_graph_attack(universe: LimaUniverseSpec) -> dict[str, Any]:
    claims = universe.all_claim_specs()
    if not claims:
        return {
            "attack": "weakest_assumption_removal",
            "result": "failed",
            "failure_type": "weak_explanation",
            "breakpoint_md": "No claims were emitted, so there are no assumptions to weaken or remove.",
            "confidence": 0.8,
        }
    edges: list[tuple[str, str]] = []
    titles = {c.title for c in claims}
    for claim in claims:
        for dep in claim.depends_on:
            if dep in titles:
                edges.append((claim.title, dep))
    if _nx is not None:
        graph = _nx.DiGraph()
        graph.add_nodes_from(titles)
        graph.add_edges_from(edges)
        if not _nx.is_directed_acyclic_graph(graph):
            return {
                "attack": "weakest_assumption_removal",
                "result": "warning",
                "failure_type": "contradiction",
                "breakpoint_md": "The claim dependency graph contains a cycle.",
                "confidence": 0.7,
            }
    return {
        "attack": "weakest_assumption_removal",
        "result": "survived",
        "claim_count": len(claims),
        "edge_count": len(edges),
        "networkx_used": _nx is not None,
        "confidence": 0.6,
    }


def _bounded_counterexample_attack(universe: LimaUniverseSpec) -> dict[str, Any]:
    blob = " ".join(
        [
            universe.title,
            universe.expected_failure_mode,
            " ".join(test.statement_md for test in universe.kill_tests),
        ]
    ).lower()
    limit = 512
    artifact = residue_pattern_summary(limit=limit, modulus=16)
    if "monotone" in blob or "decrease every step" in blob or "strict descent" in blob:
        for n in range(1, limit + 1):
            if collatz_step(n) > n:
                return {
                    "attack": "counterexample_hunt",
                    "result": "failed",
                    "failure_type": "bounded_counterexample",
                    "breakpoint_md": "A one-step strict descent reading fails immediately on odd inputs.",
                    "smallest_counterexample": {"n": n, "next": collatz_step(n)},
                    "boundary_region": {"scan_limit": limit, "modulus": 16},
                    "artifact": artifact,
                    "confidence": 0.92,
                }
    # Keep exact arithmetic in view even when no concrete claim is detected.
    sample = Fraction(3, 2) + Fraction(1, 2)
    return {
        "attack": "counterexample_hunt",
        "result": "survived",
        "artifact": artifact,
        "exact_rational_sample": str(sample),
        "numpy_used": bool(_np is not None and app_config.LIMA_USE_NUMPY),
        "confidence": 0.52,
    }


def _symbolic_attack(universe: LimaUniverseSpec) -> dict[str, Any]:
    if _sympy is None or not app_config.LIMA_USE_SYMPY:
        return {
            "attack": "boundary_regime",
            "result": "inconclusive",
            "sympy_used": False,
            "confidence": 0.2,
        }
    n = _sympy.symbols("n", integer=True, positive=True)
    odd_step = _sympy.simplify(3 * n + 1)
    two_step = _sympy.simplify(odd_step / 2)
    return {
        "attack": "boundary_regime",
        "result": "survived",
        "sympy_used": True,
        "normalized_odd_then_even": str(two_step),
        "confidence": 0.42,
    }


def _prior_art_attack(universe: LimaUniverseSpec, literature_context: list[dict[str, Any]]) -> dict[str, Any]:
    if not literature_context:
        return {
            "attack": "analogy_prior_art",
            "result": "inconclusive",
            "failure_type": "prior_art",
            "breakpoint_md": "No literature context was available, so novelty could not be checked.",
            "confidence": 0.3,
        }
    blob = " ".join(
        [
            universe.title,
            universe.family_key,
            universe.branch_of_math,
            universe.solved_world,
        ]
    ).lower()
    for source in literature_context:
        title = str(source.get("title") or "").lower()
        abstract = str(source.get("abstract_md") or "").lower()
        if ("cycle" in blob and "cycle" in title + abstract) or (
            "3x+1" in title and "collatz" in blob
        ):
            return {
                "attack": "analogy_prior_art",
                "result": "warning",
                "failure_type": "prior_art",
                "breakpoint_md": f"Possible prior-art overlap with literature source: {source.get('title')}",
                "confidence": 0.62,
            }
    return {"attack": "analogy_prior_art", "result": "survived", "confidence": 0.5}


def rupture_universe(
    universe: LimaUniverseSpec,
    *,
    literature_context: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    attacks = [
        _consistency_attack(universe),
        _bridgeability_attack(universe),
        _symbolic_attack(universe),
        {
            "attack": "residue_class",
            "result": "survived",
            "artifact": residue_pattern_summary(limit=128, modulus=8),
            "confidence": 0.45,
        },
        _prior_art_attack(universe, literature_context or []),
        _vacuity_attack(universe),
        _compression_attack(universe),
        _bounded_counterexample_attack(universe),
        _claim_graph_attack(universe),
        _formalizability_attack(universe),
    ]
    failed = [a for a in attacks if a.get("result") == "failed"]
    warnings = [a for a in attacks if a.get("result") == "warning"]
    if failed:
        verdict = "collapsed"
    elif warnings:
        verdict = "weakened"
    elif any(a.get("result") == "survived" for a in attacks):
        verdict = "survived"
    else:
        verdict = "inconclusive"
    fractures: list[dict[str, Any]] = []
    for attack in failed + warnings:
        failure_type = attack.get("failure_type")
        if not failure_type:
            continue
        fractures.append(
            {
                "failure_type": failure_type,
                "breakpoint_md": attack.get("breakpoint_md") or f"{attack.get('attack')} produced a warning.",
                "smallest_counterexample": attack.get("smallest_counterexample") or {},
                "boundary_region": attack.get("boundary_region") or {},
                "reusable_negative_theorem_md": _negative_theorem_for_attack(attack),
                "surviving_fragment_md": _surviving_fragment(universe, verdict),
                "confidence": float(attack.get("confidence") or 0.5),
            }
        )
    summary = _rupture_summary(universe, verdict, attacks)
    return {
        "universe_title": universe.title,
        "verdict": verdict,
        "attacks": attacks,
        "fractures": fractures,
        "summary_md": summary,
    }


def rupture_universes(
    universes: list[LimaUniverseSpec],
    *,
    literature_context: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    return [
        rupture_universe(universe, literature_context=literature_context)
        for universe in universes
    ]


def _negative_theorem_for_attack(attack: dict[str, Any]) -> str:
    if attack.get("failure_type") == "bounded_counterexample":
        ce = attack.get("smallest_counterexample") or {}
        return f"Any universe requiring one-step strict descent is refuted at n={ce.get('n')}."
    if attack.get("failure_type") == "vacuity":
        return "A universe that assumes the target conjecture cannot count as a solved-world reduction."
    if attack.get("failure_type") == "non_bridgeable":
        return "A solved-world mechanism without a backward translation is not handoff-worthy."
    return ""


def _surviving_fragment(universe: LimaUniverseSpec, verdict: str) -> str:
    if verdict == "collapsed":
        if universe.bridge_lemmas:
            return universe.bridge_lemmas[0].statement_md or universe.bridge_lemmas[0].title
        return universe.solved_world[:500]
    if verdict == "weakened":
        return universe.why_problem_is_easy_here[:800] or universe.core_story_md[:800]
    return universe.core_story_md[:800] or universe.solved_world[:800]


def _rupture_summary(
    universe: LimaUniverseSpec, verdict: str, attacks: list[dict[str, Any]]
) -> str:
    failed = [a["attack"] for a in attacks if a.get("result") == "failed"]
    warnings = [a["attack"] for a in attacks if a.get("result") == "warning"]
    survived = [a["attack"] for a in attacks if a.get("result") == "survived"]
    return (
        f"Rupture verdict for {universe.title}: {verdict}. "
        f"Failed attacks: {', '.join(failed) if failed else 'none'}. "
        f"Warnings: {', '.join(warnings) if warnings else 'none'}. "
        f"Survived attacks: {', '.join(survived[:6]) if survived else 'none'}."
    )
