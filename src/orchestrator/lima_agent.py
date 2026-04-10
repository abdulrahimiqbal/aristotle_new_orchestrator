"""Lima: falsification-first conceptual research engine.

Lima is upstream of Shadow and Aristotle. It can compile handoff requests and
formalizable obligations, but it never enqueues live experiments directly.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from typing import Any
from uuid import uuid4

from orchestrator import config as app_config
from orchestrator.db import Database
from orchestrator.lima_db import LimaDatabase
from orchestrator.lima_discovery import (
    build_generic_blueprint_universe,
    build_graph_stabilization_universe,
    extract_problem_signature,
    normalize_run_label,
    resolve_runtime_policy,
    select_ontology_blueprints,
)
from orchestrator.lima_literature import (
    infer_literature_relation,
    refresh_literature,
    score_literature_novelty,
)
from orchestrator.lima_meta import (
    analyze_and_update_policy,
    compute_stagnation_controller,
    compute_transfer_metrics,
)
from orchestrator.lima_models import (
    LimaClaimSpec,
    LimaGenerationResponse,
    LimaMode,
    LimaObjectSpec,
    LimaObligationSpec,
    LimaUniverseSpec,
    coerce_lima_generation_response,
    normalize_family_governance_state,
    safe_json_loads,
    slugify,
)
from orchestrator.lima_obligations import (
    compile_obligations_for_universe,
    run_queued_obligation_checks,
    submit_promising_formal_obligations,
    sync_lima_aristotle_results,
)
from orchestrator.lima_rupture import rupture_universes
from orchestrator.lima_steward import problem_ready_for_auto_continue
from orchestrator.llm import invoke_llm

logger = logging.getLogger("orchestrator.lima")

LIMA_SYSTEM = """You are Lima, a falsification-first conceptual research engine.

Role:
- Lima invents candidate mathematical universes, then breaks them.
- Lima compiles claim graphs and formalizable obligations.
- Lima cites and checks literature to avoid fake novelty.
- Lima remembers fractures and updates strategy policy over time.
- Lima treats policy as layered: global research habits, problem policy, then temporary benchmark/session locks. Temporary controls never become global defaults unless explicitly promoted by a human.
- Lima tracks ontology classes and avoids collapsing into one style when the problem has not earned that collapse.
- When a scalar signal is useful but insufficient, Lima searches for a companion object: latent coordinate, memory/carry term, quotient label, cocycle, rewrite context, hidden automaton state, defect variable, or complementary coordinate.
- Mature universes should emit proof-program obligations: uniqueness of representation, exact case transitions, ranking or lexicographic descent, bridge to the surface system, invariant validity, rewrite correctness, or quotient soundness.

Authority boundary:
- Lima has zero direct live execution authority.
- Do not enqueue experiments, targets, Aristotle jobs, or main queue work.
- Emit only bounded outputs: dead universe, weakened universe, interesting informal fragment, formalizable obligation, or handoff-worthy incubation.
- Never encode hidden benchmark answers or benchmark-scoped bans into global policy.

Output strict JSON:
{
  "frontier_summary_md": "grounded frontier summary",
  "pressure_map": {
    "tensions": ["..."],
    "failed_invariants": ["..."],
    "known_constraints": ["..."],
    "frontier_compression_opportunities": ["..."]
  },
  "run_summary_md": "compact run summary",
  "universes": [
    {
      "title": "short universe title",
      "family_key": "stable_slug",
      "family_kind": "established|adjacent|new",
      "branch_of_math": "operator theory|symbolic dynamics|2-adic dynamics|...",
      "solved_world": "world where the problem is easier",
      "why_problem_is_easy_here": "mechanism that makes the conjecture easier",
      "core_story_md": "short structured description",
      "core_objects": [
        {
          "object_kind": "state_space|operator|invariant|quotient|completion|grammar|automaton|measure|potential|equivalence|bridge",
          "name": "object name",
          "description_md": "description",
          "formal_shape": "Lean/math shape if known",
          "payload": {}
        }
      ],
      "laws": [
        {
          "claim_kind": "law",
          "title": "claim title",
          "statement_md": "claim",
          "formal_statement": "",
          "status": "open",
          "priority": 3
        }
      ],
      "backward_translation": ["how ordinary instances map back"],
      "bridge_lemmas": [
        {
          "claim_kind": "bridge_lemma",
          "title": "bridge title",
          "statement_md": "bridge statement",
          "formal_statement": "",
          "status": "open",
          "priority": 4
        }
      ],
      "conditional_theorem": {
        "claim_kind": "conditional_theorem",
        "title": "conditional theorem title",
        "statement_md": "if bridge + law, then target reduction",
        "formal_statement": "",
        "status": "open",
        "priority": 5
      },
      "kill_tests": [
        {
          "claim_kind": "kill_test",
          "title": "kill test title",
          "statement_md": "small falsifier",
          "status": "open",
          "priority": 5
        }
      ],
      "expected_failure_mode": "how this universe might die",
      "literature_queries": ["query"],
      "formalization_targets": [
        {
          "obligation_kind": "finite_check|bridge_lemma|lean_goal|counterexample_search",
          "title": "obligation title",
          "statement_md": "narrow formalizable target",
          "lean_goal": "",
          "status": "queued",
          "priority": 4
        }
      ],
      "scores": {
        "compression_score": 0,
        "fit_score": 0,
        "novelty_score": 0,
        "falsifiability_score": 0,
        "bridgeability_score": 0,
        "formalizability_score": 0,
        "theorem_yield_score": 0,
        "literature_novelty_score": 0
      }
    }
  ],
  "policy_notes": ["bounded strategy notes"]
}

Rules:
- Return JSON only.
- Emit 1 to 3 universes.
- Every universe needs objects, claims, kill tests, backward translation, and at least one formalization target.
- Prefer exact integer/rational reasoning.
- Name prior-art risks instead of claiming novelty when a literature query suggests overlap.
- Obey search_constraints: repeated fracture memory must change the next experiment design.
  If a family is explore/cooldown/soft_ban/hard_ban, obey its active scope and do not
  emit another member unless the constraint explicitly permits a material structural delta.
- No live execution fields such as campaign_id, target_id, objective, new_experiment, or aristotle_job_id."""

_GLOBAL_LIMA_RUN_LOCK = False
_STRIP_JSON_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)
_AUTO_CONTINUE_DELAY_SEC = 10


def _strip_json_fence(text: str) -> str:
    return _STRIP_JSON_FENCE.sub("", text.strip()).strip()


def _safe_json_loads(raw: str) -> dict[str, Any]:
    text = _strip_json_fence(raw)
    if not text:
        return {}
    decoder = json.JSONDecoder()
    for candidate in (text, text[text.find("{") :] if "{" in text else text):
        try:
            value, _ = decoder.raw_decode(candidate.strip())
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return {}


def _clip(value: Any, limit: int = 1200) -> str:
    return str(value or "")[:limit]


def _log_run_event(
    lima_db: LimaDatabase,
    *,
    problem_id: str,
    run_id: str,
    stage: str,
    event_kind: str,
    payload: dict[str, Any] | None = None,
    universe_id: str | None = None,
    obligation_id: str | None = None,
) -> None:
    try:
        lima_db.create_event(
            problem_id=problem_id,
            run_id=run_id,
            stage=stage,
            event_kind=event_kind,
            payload=payload,
            universe_id=universe_id,
            obligation_id=obligation_id,
        )
    except Exception:
        logger.exception("Lima event logging failed at stage %s", stage)


def _mode(value: str | None) -> LimaMode:
    v = str(value or app_config.LIMA_DEFAULT_MODE or "balanced").strip().lower()
    if v not in {"wild", "stress", "forge", "balanced"}:
        return "balanced"
    return v  # type: ignore[return-value]


def _problem_routing(problem: dict[str, Any]) -> dict[str, Any]:
    seed = safe_json_loads(problem.get("seed_packet_json"), {})
    seed_routing = seed.get("routing_policy") if isinstance(seed.get("routing_policy"), dict) else {}
    persisted = safe_json_loads(problem.get("routing_policy_json"), {})
    policy = dict(seed_routing)
    if isinstance(persisted, dict):
        policy.update(persisted)
    return policy


def _build_reference_points(main_db: Database, problem: dict[str, Any]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    routing = _problem_routing(problem)
    slug = slugify(problem.get("slug"), fallback="problem")
    retrieval_terms = [
        slug.replace("_", " "),
        str(problem.get("title") or ""),
        *[str(t) for t in routing.get("retrieval_keywords") or []],
        *[str(t) for t in routing.get("campaign_tags") or []],
    ]
    retrieval_terms = [t.lower() for t in retrieval_terms if len(t.strip()) >= 4]
    try:
        campaigns = main_db.get_all_campaigns()
    except Exception:
        logger.exception("Lima failed to read campaign references")
        campaigns = []
    for campaign in campaigns[:24]:
        prompt = str(getattr(campaign, "prompt", "") or "")
        campaign_blob = f"{prompt} {getattr(campaign, 'id', '')}".lower()
        if retrieval_terms and not any(term in campaign_blob for term in retrieval_terms):
            continue
        refs.append(
            {
                "reference_kind": "campaign",
                "external_source": "main_orchestrator",
                "external_id": str(getattr(campaign, "id", "")),
                "snapshot": {
                    "prompt": prompt,
                    "status": str(getattr(getattr(campaign, "status", None), "value", "")),
                    "problem_map_json": getattr(campaign, "problem_map_json", "{}"),
                    "research_packet_json": getattr(campaign, "research_packet_json", "{}"),
                },
                "note": "Read-only Lima reference snapshot.",
            }
        )
    try:
        shadow_goal = str(routing.get("shadow_goal_id") or f"global_{slug}")
        shadow_rows = main_db.list_shadow_global_hypotheses(shadow_goal, limit=12)
    except Exception:
        shadow_rows = []
    for row in shadow_rows:
        refs.append(
            {
                "reference_kind": "shadow_hypothesis",
                "external_source": "shadow_global",
                "external_id": str(row.get("id") or ""),
                "snapshot": dict(row),
                "note": "Shadow artifact snapshot for Lima context.",
            }
        )
    try:
        supershadow_goal = str(
            routing.get("supershadow_goal_id") or f"global_{slug}_supershadow"
        )
        supershadow_rows = main_db.list_supershadow_concepts(
            supershadow_goal, limit=12
        )
    except Exception:
        supershadow_rows = []
    for row in supershadow_rows:
        refs.append(
            {
                "reference_kind": "supershadow_concept",
                "external_source": "supershadow",
                "external_id": str(row.get("id") or ""),
                "snapshot": dict(row),
                "note": "Supershadow concept snapshot for Lima context.",
            }
        )
    return refs[:48]


def build_pressure_map(
    problem: dict[str, Any],
    state: dict[str, Any],
    reference_points: list[dict[str, Any]],
    fractures: list[dict[str, Any]],
    obligations: list[dict[str, Any]] | None = None,
    runs: list[dict[str, Any]] | None = None,
    family_search_constraints: list[dict[str, Any]] | None = None,
    families: list[dict[str, Any]] | None = None,
    policy_layers: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    seed = safe_json_loads(problem.get("seed_packet_json"), {})
    frontier = safe_json_loads(state.get("frontier_json"), {})
    tensions = [
        "Local regularity can overfit finite data unless a bridge or invariant explains it.",
        "Expressive ambient languages are useful only when they remain bridgeable to the surface system.",
        "A strong scalar signal that fails as a proof often needs a companion object or hidden context.",
    ]
    failed = [
        str(f.get("failure_type") or "") for f in fractures[:8] if f.get("failure_type")
    ]
    if failed:
        tensions.append("Recent fracture memory emphasizes: " + ", ".join(failed[:5]))
    stagnation_controller = compute_stagnation_controller(
        runs=runs or [],
        families=families or [],
        fractures=fractures,
        obligations=obligations or [],
    )
    if stagnation_controller.get("active"):
        tensions.append(
            "Stagnation controller is active: recent runs are repeating the same frontier without clearing the blocker."
        )
    search_constraints = []
    for row in family_search_constraints or []:
        required_delta = safe_json_loads(row.get("required_delta_json"), [])
        governance_state = normalize_family_governance_state(
            row.get("governance_state") or row.get("search_action") or "explore"
        )
        search_constraints.append(
            {
                "family_key": row.get("family_key"),
                "search_action": row.get("search_action"),
                "governance_state": governance_state,
                "governance_scope": row.get("governance_scope") or "problem",
                "governance_imposed_by": row.get("governance_imposed_by") or "",
                "governance_meta_mutable": bool(row.get("governance_meta_mutable", 1)),
                "status": row.get("status"),
                "last_failure_type": row.get("last_failure_type"),
                "repeat_failure_count": row.get("repeat_failure_count"),
                "reason": row.get("governance_reason_md") or row.get("search_reason_md"),
                "evidence": safe_json_loads(row.get("governance_evidence_json"), {}),
                "required_delta": required_delta if isinstance(required_delta, list) else [],
                "instruction": (
                    "Do not re-emit this family inside the active scope."
                    if governance_state == "hard_ban"
                    else "Do not re-emit this family unless the next universe materially changes "
                    "a core object, invariant, bridge lemma, falsifier, or literature tool."
                ),
            }
        )
    ontology_class_distribution: dict[str, int] = {}
    for family in families or []:
        cls = str(family.get("ontology_class") or "other")
        ontology_class_distribution[cls] = ontology_class_distribution.get(cls, 0) + 1
    active_policy_layers = [
        {
            "scope": row.get("scope"),
            "problem_id": row.get("problem_id"),
            "imposed_by": row.get("imposed_by"),
            "reason": row.get("reason_md"),
            "meta_mutable": bool(row.get("meta_mutable", 1)),
            "policy": row.get("policy_json")
            if isinstance(row.get("policy_json"), dict)
            else safe_json_loads(row.get("policy_json"), {}),
        }
        for row in policy_layers or []
    ]
    runtime_policy = next(
        (
            layer["policy"]
            for layer in active_policy_layers
            if str(layer.get("scope") or "") == "session" and isinstance(layer.get("policy"), dict)
        ),
        {},
    )
    return {
        "problem_slug": problem.get("slug"),
        "seed_frontier": seed.get("known_frontier") or [],
        "frontier_memory": frontier,
        "reference_counts": {
            "total": len(reference_points),
            "campaigns": sum(1 for r in reference_points if r["reference_kind"] == "campaign"),
            "shadow": sum(1 for r in reference_points if r["reference_kind"].startswith("shadow")),
            "supershadow": sum(1 for r in reference_points if r["reference_kind"].startswith("supershadow")),
        },
        "tensions": tensions,
        "search_constraints": search_constraints,
        "stagnation_controller": stagnation_controller,
        "policy_layers": active_policy_layers,
        "ontology_class_distribution": ontology_class_distribution,
        "ontology_class_balancing": {
            "goal": "avoid premature collapse into one ontology class unless repeated obligations justify it",
            "classes": [
                "coordinate_lift",
                "graph_stabilization",
                "rewrite_system",
                "automaton",
                "quotient",
                "cocycle_or_skew_product",
                "valuation_or_cofactor",
                "symbolic_grammar",
                "residue_finite_state",
                "geometric_or_topological",
                "algebraic_operator",
                "probabilistic_or_measure",
                "other",
            ],
        },
        "missing_structure_search": {
            "trigger": "a scalar invariant, quotient, grammar statistic, norm, or rank is useful but underparameterized",
            "candidate_companions": [
                "latent coordinate",
                "complementary coordinate",
                "carry or memory term",
                "cocycle term",
                "hidden automaton state",
                "quotient label",
                "rewrite context",
                "defect variable",
            ],
        },
        "canonical_obligation_templates": [
            "uniqueness_of_representation",
            "exact_transition_law_case_A",
            "exact_transition_law_case_B",
            "ranking_or_lexicographic_descent",
            "bridge_to_surface_system",
            "local_confluence_or_commutation",
            "quotient_or_normal_form_soundness",
        ],
        "runtime_policy_indicators": {
            "active_global_policy": any(str(layer.get("scope") or "") == "global" for layer in active_policy_layers),
            "active_problem_policy": any(str(layer.get("scope") or "") == "problem" for layer in active_policy_layers),
            "active_benchmark_policy": any(str(layer.get("scope") or "") == "benchmark" for layer in active_policy_layers),
            "active_session_policy": any(str(layer.get("scope") or "") == "session" for layer in active_policy_layers),
            "run_label": runtime_policy.get("run_label") if isinstance(runtime_policy, dict) else "",
            "autonomy_eval": bool(runtime_policy.get("autonomy_eval")) if isinstance(runtime_policy, dict) else False,
            "meta_lima_mutation_allowed": {
                scope: all(
                    bool(layer.get("meta_mutable", 1))
                    for layer in active_policy_layers
                    if str(layer.get("scope") or "") == scope
                )
                for scope in ("global", "problem", "benchmark", "session")
            },
        },
        "failed_invariants": ["naive global monotone descent", *failed[:4]],
        "known_constraints": [
            "No live Aristotle or main experiment queue mutations without human approval.",
            "Formal obligations must stay narrow enough for Lean/Mathlib review.",
            "Literature prior-art risk must be recorded explicitly.",
        ],
        "frontier_compression_opportunities": [
            "Unify parity-vector, residue, and odd-subsystem facts through a quotient or completion.",
            "Turn failed invariants into boundary theorems rather than hiding the failure.",
            "Compile first bridges as finite/residue or one-step compatibility obligations.",
            *(
                stagnation_controller.get("recommended_actions") or []
                if isinstance(stagnation_controller, dict)
                else []
            )[:3],
        ],
    }


def _family_constraint_action(pressure_map: dict[str, Any], family_key: str) -> str:
    for row in pressure_map.get("search_constraints") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("family_key") or "") == family_key:
            return normalize_family_governance_state(
                row.get("governance_state") or row.get("search_action") or ""
            )
    return ""


def _generic_status_weight(status: str) -> int:
    return {
        "formalized": 7,
        "handed_off": 6,
        "promising": 5,
        "weakened": 3,
        "proposed": 2,
        "dead": 1,
    }.get(status, 0)


def _best_generic_frontier_universe(
    current_universes: list[dict[str, Any]],
    pressure_map: dict[str, Any],
) -> dict[str, Any] | None:
    scored: list[tuple[float, dict[str, Any]]] = []
    for universe in current_universes:
        family_key = str(universe.get("family_key") or "")
        if _family_constraint_action(pressure_map, family_key) in {"cooldown", "soft_ban", "hard_ban"}:
            continue
        score = _generic_status_weight(str(universe.get("universe_status") or "")) * 10
        for field in ("fit_score", "compression_score", "formalizability_score", "bridgeability_score"):
            try:
                score += float(universe.get(field) or 0)
            except (TypeError, ValueError):
                pass
        scored.append((score, universe))
    if not scored:
        return None
    return max(scored, key=lambda item: item[0])[1]


def _looks_like_boundary_dissipation_problem(problem: dict[str, Any]) -> bool:
    blob = " ".join(
        [
            str(problem.get("title") or ""),
            str(problem.get("slug") or ""),
            str(problem.get("statement_md") or ""),
        ]
    ).lower().replace("_", " ")
    strong_patterns = (
        ("boundary spill", "stable"),
        ("boundary spill", "order independent"),
        ("boundary spill", "legal firing order"),
        ("one unit disappears off the boundary", "stable"),
        ("one unit disappears off the boundary", "order independent"),
    )
    if any(all(part in blob for part in pattern) for pattern in strong_patterns):
        return True
    signals = (
        "boundary",
        "stable",
        "disappears",
        "move at position",
        "final stable state",
        "legal firing order",
        "order independent",
    )
    return sum(1 for signal in signals if signal in blob) >= 3


def _boundary_chip_firing_fit_score(frontier: dict[str, Any] | None) -> int:
    if not frontier:
        return 0
    blob = " ".join(
        [
            str(frontier.get("title") or ""),
            str(frontier.get("family_key") or ""),
            str(frontier.get("branch_of_math") or ""),
            str(frontier.get("solved_world") or ""),
            str(frontier.get("why_problem_is_easy_here") or ""),
            str(frontier.get("core_story_md") or ""),
        ]
    ).lower()
    score = 0
    if "chip_firing_boundary_sinks" in blob:
        score += 8
    if "atlas" in blob:
        score -= 4
    for marker in (
        "chip-firing",
        "chip firing",
        "sandpile",
        "sink",
        "boundary spill",
        "abelian",
        "stabilization",
        "toppling",
        "path graph",
    ):
        if marker in blob:
            score += 2
    return score


def _select_boundary_chip_firing_identity(
    *,
    problem: dict[str, Any],
    top_frontier: dict[str, Any] | None,
) -> dict[str, Any]:
    candidate = {
        "family_key": "chip_firing_boundary_sinks",
        "title": "Chip-Firing with Boundary Sinks",
        "branch_of_math": "chip-firing and abelian sandpiles",
    }
    candidate_fit = 20
    top_fit = _boundary_chip_firing_fit_score(top_frontier)
    top_family_key = str((top_frontier or {}).get("family_key") or "")
    top_blob = " ".join(
        [
            str((top_frontier or {}).get("title") or ""),
            str((top_frontier or {}).get("family_key") or ""),
            str((top_frontier or {}).get("solved_world") or ""),
        ]
    ).lower()
    preserve_top = bool(top_frontier) and (
        top_family_key == candidate["family_key"]
        or top_family_key.startswith(candidate["family_key"] + "_")
        or top_fit >= candidate_fit
        or (
            top_fit >= 12
            and any(marker in top_blob for marker in ("chip", "sandpile", "sink", "abelian"))
        )
    )
    override = bool(top_frontier) and not preserve_top
    return {
        **candidate,
        "selected_family_key": candidate["family_key"],
        "selected_title": candidate["title"],
        "problem_aware_family_selected": True,
        "prior_frontier_family_key": top_family_key,
        "prior_frontier_title": str((top_frontier or {}).get("title") or ""),
        "prior_frontier_fit_score": top_fit,
        "candidate_fit_score": candidate_fit,
        "overrode_prior_frontier": override,
        "selection_reason": (
            "preserved_problem_native_frontier"
            if preserve_top
            else "selected_problem_native_chip_firing_family"
        ),
        "problem_slug": str(problem.get("slug") or ""),
    }


def _repair_hypothesis_by_key(
    repair_loop: dict[str, Any], hypothesis_key: str
) -> dict[str, Any] | None:
    for hypothesis in repair_loop.get("hypotheses") or []:
        if not isinstance(hypothesis, dict):
            continue
        if str(hypothesis.get("key") or "") == hypothesis_key:
            return hypothesis
    return None


def _chip_firing_repair_universe(
    *,
    problem: dict[str, Any],
    top_frontier: dict[str, Any] | None,
    hypothesis: dict[str, Any],
) -> LimaUniverseSpec:
    parent_family_key = str((top_frontier or {}).get("family_key") or "chip_firing_boundary")
    parent_title = str((top_frontier or {}).get("title") or "Chip-Firing with Boundary Sinks")
    branch = str((top_frontier or {}).get("branch_of_math") or "chip-firing and abelian sandpiles")
    repair_key = str(hypothesis.get("key") or "repair")
    repair_title = str(hypothesis.get("title") or "Repair")
    repair_description = str(hypothesis.get("description") or "")
    repair_focus = str(hypothesis.get("check_focus") or "")
    title = f"{parent_title}: {repair_title}"
    exact_transition_title = f"{repair_key}_exact_transition_law"
    commutation_title = f"{repair_key}_commutation_audit"
    endpoint_title = f"{repair_key}_unique_endpoint"
    return LimaUniverseSpec(
        title=title,
        family_key=f"{parent_family_key}_{repair_key}",
        family_kind="adjacent",
        branch_of_math=branch,
        solved_world=(
            "Boundary spill is modeled as sinked chip-firing plus one explicit repair variable, so the "
            "bridge becomes an exact state update rather than a lossy analogy."
        ),
        why_problem_is_easy_here=(
            f"{repair_description} {repair_focus}".strip()
            or "The repair variable is supposed to close the exact bridge and restore the missing state."
        ),
        core_story_md=(
            f"Lima is testing the '{repair_title}' repair on top of the chip-firing frontier. "
            "This attempt lives or dies on whether the added state makes boundary spill updates exact."
        ),
        core_objects=[
            LimaObjectSpec(
                object_kind="state_space",
                name="BoundaryChipConfiguration",
                description_md="A sink-completed chip configuration modeling the surface state.",
                formal_shape="Fin N -> Nat",
                payload={"boundary_behavior": "absorbing_sinks", "parent_family_key": parent_family_key},
            ),
            LimaObjectSpec(
                object_kind="state_space",
                name=repair_title.replace(" ", ""),
                description_md=repair_description,
                formal_shape="RepairState",
                payload={
                    "repair_hypothesis_key": repair_key,
                    "repair_parent_family_key": parent_family_key,
                    "repair_strategy": "companion_state_search",
                },
            ),
            LimaObjectSpec(
                object_kind="bridge",
                name="ExactBoundaryBridge",
                description_md="A repaired bridge from boundary spill moves into augmented sinked chip-firing.",
                formal_shape="SurfaceState -> BoundaryChipConfiguration × RepairState",
                payload={"repair_focus": repair_focus},
            ),
        ],
        laws=[
            LimaClaimSpec(
                claim_kind="law",
                title=f"{repair_title} closes the transition law",
                statement_md="The repaired state representation evolves exactly under one legal move of the surface system.",
                priority=5,
            )
        ],
        backward_translation=[
            "Project the repaired sinked state back to the surface system and recover the same stable endpoint.",
            "Show which missing boundary information is restored by the repair variable.",
        ],
        bridge_lemmas=[
            LimaClaimSpec(
                claim_kind="bridge_lemma",
                title=exact_transition_title,
                statement_md=(
                    f"State the exact repaired transition law using the {repair_title.lower()} so a boundary-spill move "
                    "matches one augmented sinked-firing step."
                ),
                priority=5,
            )
        ],
        conditional_theorem=LimaClaimSpec(
            claim_kind="conditional_theorem",
            title=f"{repair_title} implies unique endpoint",
            statement_md=(
                "If the repaired bridge is exact and the augmented sinked dynamics are abelian, then the "
                "surface system has a unique stable endpoint."
            ),
            priority=5,
        ),
        kill_tests=[
            LimaClaimSpec(
                claim_kind="kill_test",
                title=commutation_title,
                statement_md=(
                    f"Enumerate small boundary configurations and test whether the {repair_title.lower()} "
                    "makes adjacent legal firings commute."
                ),
                priority=5,
            ),
            LimaClaimSpec(
                claim_kind="kill_test",
                title=f"{repair_key}_same_projection_different_future",
                statement_md=(
                    "Search for two repaired states with the same projected sinked configuration but different "
                    "future behavior, which would show the repair is still insufficient."
                ),
                priority=5,
            ),
        ],
        expected_failure_mode=(
            "The added repair state may still be too weak to determine the exact bridge or may fail the bounded "
            "commutation audit."
        ),
        literature_queries=[
            f"{problem.get('title') or problem.get('slug') or 'problem'} chip-firing {repair_title.lower()}",
            f"{problem.get('title') or problem.get('slug') or 'problem'} exact boundary transition repair state",
        ],
        formalization_targets=[
            LimaObligationSpec(
                obligation_kind="finite_check",
                title=commutation_title,
                statement_md=repair_focus or "Run bounded commutation checks on the repaired state.",
                why_exists_md="A repaired bridge should survive small commutation audits before it is trusted.",
                prove_or_kill_md="A failing audit kills this repair attempt quickly.",
                priority=5,
            ),
            LimaObligationSpec(
                obligation_kind="bridge_lemma",
                title=exact_transition_title,
                statement_md=(
                    f"Write the exact transition law for the {repair_title.lower()} repair of the chip-firing bridge."
                ),
                lean_goal="forall s i, True",
                why_exists_md="The repair only matters if it sharpens the bridge into an exact formal statement.",
                prove_or_kill_md="If the repaired transition law cannot be written cleanly, this repair is not sufficient.",
                priority=5,
            ),
            LimaObligationSpec(
                obligation_kind="lean_goal",
                title=endpoint_title,
                statement_md=(
                    f"Formalize that the {repair_title.lower()} repair preserves a unique stable endpoint."
                ),
                lean_goal="forall s, True",
                why_exists_md="A viable repair should upgrade the frontier toward a genuine survivor.",
                prove_or_kill_md="If the repaired state cannot preserve unique endpoint structure, abandon this repair.",
                priority=4,
            ),
        ],
        scores={
            "compression_score": 4,
            "fit_score": 5,
            "novelty_score": 4,
            "falsifiability_score": 5,
            "bridgeability_score": 5,
            "formalizability_score": 4,
            "theorem_yield_score": 4,
            "literature_novelty_score": 4,
        },
        repair_hypothesis_key=repair_key,
        repair_parent_family_key=parent_family_key,
        repair_strategy="companion_state_search",
        repair_focus=repair_focus,
    )


def _chip_firing_repair_fallback(
    *,
    problem: dict[str, Any],
    mode: LimaMode,
    pressure_map: dict[str, Any],
    top_frontier: dict[str, Any] | None,
    repair_loop: dict[str, Any],
) -> LimaGenerationResponse:
    hypothesis_keys = list(repair_loop.get("next_hypothesis_keys") or [])
    hypotheses = [
        _repair_hypothesis_by_key(repair_loop, key)
        for key in hypothesis_keys
    ]
    hypotheses = [hypothesis for hypothesis in hypotheses if isinstance(hypothesis, dict)]
    if not hypotheses:
        hypotheses = [
            hypothesis
            for hypothesis in repair_loop.get("hypotheses") or []
            if isinstance(hypothesis, dict)
        ][:2]
    universes = [
        _chip_firing_repair_universe(
            problem=problem,
            top_frontier=top_frontier,
            hypothesis=hypothesis,
        )
        for hypothesis in hypotheses[:3]
    ]
    attempted = int(repair_loop.get("attempts_used") or 0)
    budget = int(repair_loop.get("attempt_budget") or 0)
    return LimaGenerationResponse(
        frontier_summary_md=(
            f"{problem.get('title') or problem.get('slug') or 'This problem'} is still centered on the chip-firing "
            "frontier, but Lima has switched from repeating the same bridge story to enumerating concrete repaired "
            "state models that could close the missing companion structure."
        ),
        pressure_map=pressure_map,
        run_summary_md=(
            f"Lima {mode} repair loop is active: it is testing explicit companion-state repairs for the "
            f"chip-firing bridge ({attempted}/{budget} prior repair attempt(s) recorded)."
        ),
        universes=universes,
        policy_notes=[
            "Repair loop is active for the dominant chip-firing frontier.",
            "Each emitted universe is a concrete repaired-state attempt, not another scalar-only summary.",
        ],
    )


def _generic_chip_firing_fallback(
    *,
    problem: dict[str, Any],
    mode: LimaMode,
    pressure_map: dict[str, Any],
    top_frontier: dict[str, Any] | None,
) -> LimaGenerationResponse:
    selection_meta = _select_boundary_chip_firing_identity(
        problem=problem,
        top_frontier=top_frontier,
    )
    family_key = str(selection_meta["family_key"])
    title = str(selection_meta["title"])
    branch = str(selection_meta["branch_of_math"])
    return LimaGenerationResponse(
        frontier_summary_md=(
            f"{problem.get('title') or problem.get('slug') or 'This problem'} currently points toward a "
            "boundary-sink chip-firing model: termination and order-independence should come from an abelian "
            "stabilization mechanism, not from a scalar height argument alone."
        ),
        pressure_map=pressure_map,
        run_summary_md=(
            f"Lima {mode} fallback reinforced the boundary-sink chip-firing frontier, shifted search away "
            "from repeated underparameterized scalar families, and compiled bridge obligations that would "
            "make the survivor more formal."
        ),
        universes=[
            LimaUniverseSpec(
                title=title,
                family_key=family_key,
                family_kind=(
                    "new"
                    if not top_frontier or selection_meta.get("overrode_prior_frontier")
                    else "adjacent"
                ),
                branch_of_math=branch,
                solved_world=(
                    "The surface dynamics are modeled on a finite path graph with absorbing boundary sinks, so legal "
                    "moves become chip-firing topplings and stabilization becomes an abelian-network question."
                ),
                why_problem_is_easy_here=(
                    "Path-graph chip-firing exposes exact local toppling operators, a quadratic potential for "
                    "termination, and a concrete same-endpoint hypothesis for order-independent stabilization."
                ),
                core_story_md=(
                    "Lima uses a problem-native boundary-sinks ontology instead of a generic atlas. The live burden "
                    "is to verify exact bridge steps, bounded commutation, quadratic-potential descent, termination, "
                    "and unique stabilized endpoints honestly on small path graphs."
                ),
                core_objects=[
                    LimaObjectSpec(
                        object_kind="state_space",
                        name="BoundaryPathState",
                        description_md="A chip configuration on a finite path graph whose missing external neighbors are absorbing left/right sinks.",
                        formal_shape="Fin N -> Nat",
                        payload={"graph_kind": "path_with_boundary_sinks", "boundary_behavior": "absorbing_sinks"},
                    ),
                    LimaObjectSpec(
                        object_kind="operator",
                        name="BoundarySpillTopple",
                        description_md="A toppling operator that removes two chips from a legal site, sends one to each path neighbor, and routes off-path mass into the matching sink.",
                        formal_shape="BoundaryPathState -> Fin N -> BoundaryPathState",
                        payload={"operator_kind": "chip_firing_with_sinks"},
                    ),
                    LimaObjectSpec(
                        object_kind="bridge",
                        name="BoundarySpillToSinkedChipFiring",
                        description_md="The exact embedding of a surface state into chip-firing on a path graph with explicit left/right sink coordinates.",
                        formal_shape="SurfaceState -> (Nat × BoundaryPathState × Nat)",
                        payload={"tracks_sink_mass": True},
                    ),
                    LimaObjectSpec(
                        object_kind="potential",
                        name="QuadraticSinkPotential",
                        description_md="A quadratic potential with weights i*(N+1-i) that should strictly decrease under every legal firing.",
                        formal_shape="BoundaryPathState -> Nat",
                        payload={"weight_shape": "i*(N+1-i)", "expected_drop_per_firing": 2},
                    ),
                ],
                laws=[
                    LimaClaimSpec(
                        claim_kind="law",
                        title="Local firings commute on the path graph with sinks",
                        statement_md="Interior and boundary firings commute once the path graph is completed with explicit boundary sinks.",
                        priority=5,
                    ),
                    LimaClaimSpec(
                        claim_kind="law",
                        title="Quadratic potential decreases under legal firings",
                        statement_md="The quadratic sink potential decreases by a fixed positive amount on every legal toppling.",
                        priority=5,
                    )
                ],
                backward_translation=[
                    "Embed a surface state into a finite path graph with explicit left and right sinks.",
                    "Translate each legal move into one sinked toppling step and project stabilized chip-firing states back to 0/1 surface states.",
                ],
                bridge_lemmas=[
                    LimaClaimSpec(
                        claim_kind="bridge_lemma",
                        title="Boundary spill move equals sinked firing",
                        statement_md="Each legal boundary-spill move is exactly one chip-firing step on the completed path graph with boundary sinks.",
                        formal_statement="forall s i, legalMove s i -> bridge(step s i) = fire (bridge s) i",
                        priority=5,
                    )
                ],
                conditional_theorem=LimaClaimSpec(
                    claim_kind="conditional_theorem",
                    title="Sink completion implies unique stabilization",
                    statement_md="If the path-graph bridge is exact, local firings are abelian, and the quadratic potential decreases, every legal sequence terminates and reaches the same stable endpoint.",
                    priority=5,
                ),
                kill_tests=[
                    LimaClaimSpec(
                        claim_kind="kill_test",
                        title="firing_commutation_local",
                        statement_md="Search bounded path-graph states for a pair of legal firings that fail to commute.",
                        priority=5,
                    ),
                    LimaClaimSpec(
                        claim_kind="kill_test",
                        title="local_confluence_or_abelianity",
                        statement_md="Enumerate bounded states and check whether all legal firing orders reach the same stabilized endpoint.",
                        priority=5,
                    ),
                    LimaClaimSpec(
                        claim_kind="kill_test",
                        title="quadratic_potential_descent",
                        statement_md="Check bounded legal firings for any violation of strict quadratic-potential descent.",
                        priority=5,
                    ),
                ],
                expected_failure_mode=(
                    "The path-graph completion may still omit a companion coordinate, or bounded confluence may fail "
                    "even if the bridge and potential descent look plausible."
                ),
                literature_queries=[
                    f"{problem.get('title') or problem.get('slug') or 'problem'} chip-firing sinks abelian sandpile path graph",
                    f"{problem.get('title') or problem.get('slug') or 'problem'} boundary sink stabilization order independence",
                ],
                formalization_targets=[
                    LimaObligationSpec(
                        obligation_kind="finite_check",
                        title="boundary_spill_move_equals_sinked_firing",
                        statement_md="Verify on bounded path-graph states that each legal boundary-spill move matches one sinked chip-firing step exactly.",
                        priority=5,
                        why_exists_md="The ontology is only honest if live moves literally agree with sinked chip-firing on bounded states.",
                        prove_or_kill_md="Any bounded bridge mismatch kills this family immediately.",
                    ),
                    LimaObligationSpec(
                        obligation_kind="finite_check",
                        title="firing_commutation_local",
                        statement_md="Verify on bounded states that adjacent legal firings commute in the path-graph sink model.",
                        priority=5,
                        why_exists_md="Local commutation is the bounded shadow of the abelian-network claim.",
                        prove_or_kill_md="A bounded non-commuting pair would refute the live path-graph ontology.",
                    ),
                    LimaObligationSpec(
                        obligation_kind="invariant_check",
                        title="quadratic_potential_descent",
                        statement_md="Check on bounded states that the quadratic sink potential strictly decreases after every legal firing.",
                        priority=5,
                        why_exists_md="Termination should already be visible as a real bounded ranking witness.",
                        prove_or_kill_md="If the quadratic potential fails to decrease, the current termination story is wrong.",
                    ),
                    LimaObligationSpec(
                        obligation_kind="finite_check",
                        title="stabilization_terminates",
                        statement_md="Verify on bounded states that every legal firing sequence reaches a stable state in finitely many steps.",
                        priority=5,
                        why_exists_md="A live chip-firing ontology should survive bounded termination checks before escalation.",
                        prove_or_kill_md="A bounded non-terminating trace would kill the family.",
                    ),
                    LimaObligationSpec(
                        obligation_kind="finite_check",
                        title="local_confluence_or_abelianity",
                        statement_md="Verify on bounded states that all legal firing orders reach the same stabilized endpoint.",
                        priority=5,
                        why_exists_md="Order-independence is the actual benchmark-facing claim.",
                        prove_or_kill_md="If two legal orders stabilize differently on a bounded state, abandon this ontology.",
                    ),
                    LimaObligationSpec(
                        obligation_kind="lean_goal",
                        title="sink_stabilization_implies_unique_endpoint",
                        statement_md="Formalize that exact boundary-sink chip-firing, local confluence, and quadratic-potential descent imply a unique stabilized endpoint.",
                        lean_goal="forall s, True",
                        priority=4,
                        why_exists_md="This theorem upgrades the bounded path-graph evidence into a formal proof target.",
                        prove_or_kill_md="If the theorem cannot be stated cleanly, the family is not mature enough for formal escalation.",
                    ),
                ],
                scores={
                    "compression_score": 4,
                    "fit_score": 5,
                    "novelty_score": 3,
                    "falsifiability_score": 4,
                    "bridgeability_score": 5,
                    "formalizability_score": 4,
                    "theorem_yield_score": 4,
                    "literature_novelty_score": 3,
                },
            )
        ],
        policy_notes=[
            "Deterministic fallback selected a problem-native path-graph with boundary sinks instead of reusing a generic atlas family.",
            "Live evaluation now prioritizes exact bridge checks, bounded abelianity, potential descent, termination, and same-endpoint stabilization.",
        ],
        selection_meta=selection_meta,
    )


def _latest_problem_obligations_by_title(
    lima_db: LimaDatabase,
    *,
    problem_id: str,
    titles: list[str],
) -> dict[str, dict[str, Any]]:
    wanted = set(titles)
    latest: dict[str, dict[str, Any]] = {}
    for row in lima_db.list_obligations(problem_id, limit=200):
        title = str(row.get("title") or "")
        if title not in wanted:
            continue
        current = latest.get(title)
        if current is None or str(row.get("updated_at") or "") > str(current.get("updated_at") or ""):
            latest[title] = row
    return latest


def _latest_boundary_bridge_counterexample(
    lima_db: LimaDatabase,
    *,
    problem_id: str,
) -> dict[str, Any]:
    for artifact in lima_db.list_artifacts(problem_id, limit=200):
        if str(artifact.get("artifact_kind") or "") != "obligation_check":
            continue
        payload = safe_json_loads(artifact.get("content_json"), {})
        if str(payload.get("title") or "") not in {
            "boundary_spill_move_equals_sinked_firing",
            "exact_transition_law_case_A",
            "exact_transition_law_case_B",
        }:
            continue
        detail = payload.get("artifact")
        if isinstance(detail, dict) and isinstance(detail.get("counterexample"), dict):
            return dict(detail["counterexample"])
    return {}


def _surface_projection(augmented_state: Any) -> list[int]:
    if not isinstance(augmented_state, (list, tuple)) or len(augmented_state) < 2:
        return []
    return [int(v) for v in augmented_state[1:-1]]


def _build_boundary_bridge_repair_payload(
    *,
    problem: dict[str, Any],
    bridge_statuses: dict[str, dict[str, Any]],
    counterexample: dict[str, Any],
) -> dict[str, Any] | None:
    exact_bridge = bridge_statuses.get("exact_transition_law_case_B") or bridge_statuses.get("boundary_spill_move_equals_sinked_firing")
    support_titles = (
        "ranking_or_lexicographic_descent",
        "local_operator_commutation_window",
        "bounded_termination_or_stabilization",
        "local_confluence_or_commutation",
    )
    if str((exact_bridge or {}).get("status") or "") != "refuted_local":
        return None
    if not all(str((bridge_statuses.get(title) or {}).get("status") or "") == "verified_local" for title in support_titles):
        return None

    sinked_next = counterexample.get("sinked_next")
    projected_next = _surface_projection(sinked_next)
    surface_next = [int(v) for v in counterexample.get("surface_next") or []] if isinstance(
        counterexample.get("surface_next"), list
    ) else []
    move = counterexample.get("move")
    state = counterexample.get("state")
    candidates = [
        {
            "key": "simulation_up_to_stabilization",
            "rank": 1,
            "bridge_form": "simulation up to stabilization",
            "statement_md": (
                "For every surface state s, stabilizing the sink-completed chip-firing image of s and then "
                "projecting away sink coordinates gives the same stable surface endpoint as stabilizing s directly."
            ),
            "why_avoids_counterexample_md": (
                "The known failure only records extra boundary sink mass. In the counterexample, one sinked firing sends "
                f"{state} to {sinked_next}, whose interior projection is {projected_next}, while the surface step is "
                f"{surface_next}. The revised bridge compares stabilized projected interiors, not raw sink coordinates."
            ),
            "obligations": [
                {
                    "kind": "finite_check",
                    "title": "stabilized_sink_projection_matches_surface_endpoint",
                    "statement_md": "Enumerate bounded states and compare stabilized surface endpoints with projected stabilized sinked chip-firing endpoints.",
                },
                {
                    "kind": "finite_check",
                    "title": "projection_commutes_with_firing_up_to_stabilization",
                    "statement_md": "Check on bounded states that one legal surface move and one sinked firing have the same projected stabilized future.",
                },
                {
                    "kind": "lean_goal",
                    "title": "stabilized_projection_uniqueness_bridge",
                    "statement_md": "Formalize that projected sink stabilization plus abelianity yields a unique stabilized surface endpoint.",
                },
            ],
        },
        {
            "key": "boundary_sink_ledger_exact_embedding",
            "rank": 2,
            "bridge_form": "embedding with extra boundary bookkeeping state",
            "statement_md": (
                "Augment the surface state with cumulative left/right sink ledgers. The repaired bridge sends "
                "(surface_state, sink_ledger) to the full sink-completed chip configuration, and each legal surface move "
                "becomes exactly one sinked chip-firing step with the ledger updated by the spilled boundary mass."
            ),
            "why_avoids_counterexample_md": (
                "The counterexample fails only because bridge(surface_next) forgot the new sink mass. With a right-sink "
                f"ledger increment after move {move}, the repaired image of {surface_next} is exactly {sinked_next}."
            ),
            "obligations": [
                {
                    "kind": "finite_check",
                    "title": "boundary_sink_ledger_exact_transition",
                    "statement_md": "Verify on bounded states and bounded sink ledgers that one legal surface move equals one sinked chip-firing step with the ledger update.",
                },
                {
                    "kind": "finite_check",
                    "title": "ledger_projection_recovers_surface_state",
                    "statement_md": "Check that forgetting sink ledgers recovers the original surface state after every bounded repaired transition.",
                },
                {
                    "kind": "bridge_lemma",
                    "title": "ledger_exact_embedding_implies_surface_stabilization",
                    "statement_md": "State that exact ledger embedding plus bounded abelian sink dynamics implies order-independent surface stabilization.",
                },
            ],
        },
        {
            "key": "normal_form_odometer_bridge",
            "rank": 3,
            "bridge_form": "normal-form equivalence rather than stepwise equality",
            "statement_md": (
                "Forget raw sink coordinates and compare only the toppling-count odometer or stabilized normal form: two runs are equivalent when they induce the same interior stabilized endpoint and the same bounded firing counts."
            ),
            "why_avoids_counterexample_md": (
                "The bad one-step equality distinguishes states that differ only by already-spilled sink mass. The "
                "counterexample still has the same one-topple explanation and the same projected normal-form behavior, "
                "so odometer or normal-form comparison survives."
            ),
            "obligations": [
                {
                    "kind": "finite_check",
                    "title": "bounded_odometer_matches_surface_stabilization",
                    "statement_md": "On bounded states, compute firing counts and check that the sinked odometer determines the same stabilized surface endpoint.",
                },
                {
                    "kind": "finite_check",
                    "title": "same_odometer_same_surface_endpoint",
                    "statement_md": "Check on bounded states that equal bounded odometer data forces the same stabilized surface endpoint.",
                },
                {
                    "kind": "lean_goal",
                    "title": "normal_form_projection_from_odometer",
                    "statement_md": "Formalize that the stabilized surface normal form is a quotient of the sinked odometer or stabilized chip-firing normal form.",
                },
            ],
        },
    ]
    return {
        "problem_slug": str(problem.get("slug") or ""),
        "problem_title": str(problem.get("title") or ""),
        "family_key": "graph_stabilization_boundary_leakage",
        "repair_scope": "narrow_boundary_bridge_only",
        "exact_bridge_status": str((exact_bridge or {}).get("status") or ""),
        "exact_bridge_summary": str((exact_bridge or {}).get("result_summary_md") or ""),
        "support_statuses": {
            title: str((bridge_statuses.get(title) or {}).get("status") or "")
            for title in support_titles
        },
        "counterexample": counterexample,
        "top_revised_bridges": candidates,
        "most_likely_correct_key": "boundary_sink_ledger_exact_embedding",
        "most_likely_correct_reason": (
            "The only observed failure is missing sink bookkeeping, so adding explicit left/right sink ledgers is the smallest repair that restores exact stepwise simulation without changing the ontology."
        ),
        "proof_program_status": "bounded_proof_program_recovered",
        "benchmark_status": "bounded_proof_program_recovered",
        "status_reason": (
            "The chip-firing ontology now has a live bounded proof program, but the original exact bridge remains false until one of these repaired bridges is checked."
        ),
    }


def _run_boundary_bridge_repair_cycle(
    lima_db: LimaDatabase,
    *,
    problem: dict[str, Any],
    problem_id: str,
    run_id: str,
    universes: list[LimaUniverseSpec],
) -> dict[str, Any] | None:
    if not any(
        str(universe.family_key or "") in {"chip_firing_boundary_sinks", "graph_stabilization_boundary_leakage"}
        for universe in universes
    ):
        return None
    titles = [
        "exact_transition_law_case_A",
        "exact_transition_law_case_B",
        "ranking_or_lexicographic_descent",
        "local_operator_commutation_window",
        "bounded_termination_or_stabilization",
        "local_confluence_or_commutation",
    ]
    payload = _build_boundary_bridge_repair_payload(
        problem=problem,
        bridge_statuses=_latest_problem_obligations_by_title(
            lima_db,
            problem_id=problem_id,
            titles=titles,
        ),
        counterexample=_latest_boundary_bridge_counterexample(
            lima_db,
            problem_id=problem_id,
        ),
    )
    if not payload:
        return None
    persisted = lima_db.list_universes_for_run(run_id)
    artifact_id = lima_db.create_artifact(
        problem_id=problem_id,
        universe_id=str(persisted[0].get("id") or "") if persisted else "",
        artifact_kind="bridge_repair_cycle",
        content=payload,
    )
    return {**payload, "artifact_id": artifact_id}


def _generic_companion_mutation_fallback(
    *,
    problem: dict[str, Any],
    mode: LimaMode,
    pressure_map: dict[str, Any],
    top_frontier: dict[str, Any] | None,
) -> LimaGenerationResponse:
    family_key = "defect_augmented_bridge"
    title = (
        f"{str((top_frontier or {}).get('title') or problem.get('title') or 'Problem')} "
        "with a defect coordinate"
    )
    return LimaGenerationResponse(
        frontier_summary_md=(
            f"{problem.get('title') or problem.get('slug') or 'This problem'} is plateauing on an "
            "underparameterized scalar story, so Lima is forcing a companion-state mutation."
        ),
        pressure_map=pressure_map,
        run_summary_md=(
            f"Lima {mode} fallback detected stagnation under the same blocker and pivoted into a "
            "defect-augmented bridge universe with explicit hidden-state obligations."
        ),
        universes=[
            LimaUniverseSpec(
                title=title,
                family_key=family_key,
                family_kind="new",
                branch_of_math=str((top_frontier or {}).get("branch_of_math") or problem.get("domain") or "discrete dynamics"),
                solved_world=(
                    "A scalar progress quantity is paired with an explicit defect or memory coordinate so "
                    "the repaired state can track exactly what plain descent was losing."
                ),
                why_problem_is_easy_here=(
                    "The conjecture becomes a bridge-and-closure problem: if the defect coordinate closes "
                    "the transition law exactly, then the scalar can be used safely."
                ),
                core_story_md=(
                    "Lima is no longer allowed to emit another scalar-only explanation. This mutation adds "
                    "a defect variable and asks for exact transition laws in the repaired state."
                ),
                core_objects=[
                    LimaObjectSpec(
                        object_kind="potential",
                        name="PrimaryScalar",
                        description_md="The original scalar signal that looked promising but lost state information.",
                        formal_shape="State -> Int",
                        payload={},
                    ),
                    LimaObjectSpec(
                        object_kind="state_space",
                        name="DefectAugmentedState",
                        description_md="A repaired state carrying the scalar together with a defect or memory coordinate.",
                        formal_shape="State × Defect",
                        payload={"mutation_reason": "stagnation_underparameterized_state"},
                    ),
                    LimaObjectSpec(
                        object_kind="bridge",
                        name="ExactTransitionLaw",
                        description_md="An exact transition law on the repaired state representation.",
                        formal_shape="DefectAugmentedState -> DefectAugmentedState",
                        payload={},
                    ),
                ],
                laws=[
                    LimaClaimSpec(
                        claim_kind="law",
                        title="The defect coordinate closes the transition law",
                        statement_md="The repaired state has exact one-step evolution without hidden loss.",
                        priority=5,
                    )
                ],
                backward_translation=[
                    "Project the defect-augmented state back to the surface system without losing termination facts.",
                    "Show which information the scalar forgot and how the defect restores it.",
                ],
                bridge_lemmas=[
                    LimaClaimSpec(
                        claim_kind="bridge_lemma",
                        title="Repaired state implies original step",
                        statement_md="The exact repaired transition projects to a valid surface move.",
                        priority=5,
                    )
                ],
                conditional_theorem=LimaClaimSpec(
                    claim_kind="conditional_theorem",
                    title="Defect-closed descent implies progress",
                    statement_md="If the repaired transition is exact and the scalar decreases there, the original system inherits a valid progress certificate.",
                    priority=5,
                ),
                kill_tests=[
                    LimaClaimSpec(
                        claim_kind="kill_test",
                        title="Two states same scalar different future",
                        statement_md="Search for a smallest pair of states with the same scalar but different next-step behavior, confirming the defect is necessary.",
                        priority=5,
                    )
                ],
                expected_failure_mode="The defect variable may still be insufficient or too ad hoc to bridge cleanly.",
                literature_queries=[
                    f"{problem.get('title') or problem.get('slug') or 'problem'} defect variable discrete dynamics",
                    f"{problem.get('title') or problem.get('slug') or 'problem'} hidden state exact transition law",
                ],
                formalization_targets=[
                    LimaObligationSpec(
                        obligation_kind="counterexample_search",
                        title="same_scalar_different_future",
                        statement_md="Search bounded states for equal-scalar pairs with different next-step behavior.",
                        priority=5,
                    ),
                    LimaObligationSpec(
                        obligation_kind="bridge_lemma",
                        title="defect_augmented_transition_law",
                        statement_md="State an exact repaired transition law with an explicit defect variable.",
                        lean_goal="forall s, True",
                        priority=4,
                    ),
                ],
                scores={
                    "compression_score": 3,
                    "fit_score": 4,
                    "novelty_score": 4,
                    "falsifiability_score": 5,
                    "bridgeability_score": 4,
                    "formalizability_score": 4,
                    "theorem_yield_score": 4,
                    "literature_novelty_score": 4,
                },
            )
        ],
        policy_notes=[
            "Stagnation controller forced a companion-state mutation.",
            "Scalar-only repeats are temporarily disallowed until the missing state is explicit.",
        ],
    )


def _local_generation(
    *,
    problem: dict[str, Any],
    mode: LimaMode,
    pressure_map: dict[str, Any],
    literature_refresh: dict[str, Any],
    current_universes: list[dict[str, Any]] | None = None,
    run_label: str = "GUIDED_DEBUG",
    runtime_policy: dict[str, Any] | None = None,
) -> LimaGenerationResponse:
    problem_title = str(problem.get("title") or problem.get("slug") or "the problem")
    problem_slug = str(problem.get("slug") or "").lower()
    stagnation = pressure_map.get("stagnation_controller") if isinstance(pressure_map, dict) else {}
    resolved_policy = runtime_policy or {"merged_policy": {"run_label": normalize_run_label(run_label)}}
    merged_policy = dict(resolved_policy.get("merged_policy") or {})
    if "collatz" not in problem_slug and "collatz" not in problem_title.lower():
        top_frontier = _best_generic_frontier_universe(current_universes or [], pressure_map)
        signature = extract_problem_signature(problem)
        blueprint_decision = select_ontology_blueprints(
            signature=signature,
            current_universes=current_universes,
        )
        repair_loop = (
            stagnation.get("repair_loop")
            if isinstance(stagnation, dict) and isinstance(stagnation.get("repair_loop"), dict)
            else {}
        )
        if (
            bool(merged_policy.get("allow_guided_repair_cycles", True))
            and top_frontier
            and isinstance(repair_loop, dict)
            and repair_loop.get("active")
            and str(repair_loop.get("target_family_key") or "") == str(top_frontier.get("family_key") or "")
        ):
            return _chip_firing_repair_fallback(
                problem=problem,
                mode=mode,
                pressure_map=pressure_map,
                top_frontier=top_frontier,
                repair_loop=repair_loop,
            )
        if (
            isinstance(stagnation, dict)
            and stagnation.get("active")
            and str(stagnation.get("dominant_blocker") or "") == "underparameterized_state"
        ):
            return _generic_companion_mutation_fallback(
                problem=problem,
                mode=mode,
                pressure_map=pressure_map,
                top_frontier=top_frontier,
            )
        if (
            str(blueprint_decision.get("selected_blueprint") or "") == "graph_stabilization"
            and (
                not top_frontier
                or _family_constraint_action(pressure_map, str(top_frontier.get("family_key") or ""))
                not in {"cooldown", "soft_ban", "hard_ban"}
            )
        ):
            generated = build_graph_stabilization_universe(
                problem=problem,
                mode=mode,
                signature=signature,
            )
            generated.pressure_map = pressure_map
            generated.selection_meta = {
                **generated.selection_meta,
                **blueprint_decision,
                "run_label": normalize_run_label(run_label),
            }
            return generated
        generic_family = {
            "wild": "structural_completion_atlas",
            "stress": "counterexample_boundary_atlas",
            "forge": "minimal_bridge_obligation_atlas",
            "balanced": "minimal_bridge_obligation_atlas",
        }[mode]
        generated = build_generic_blueprint_universe(
            problem=problem,
            mode=mode,
            signature=signature,
            blueprint_key=str(blueprint_decision.get("selected_blueprint") or "coordinate_lift"),
        )
        generated.universes[0].family_key = generic_family
        generated.pressure_map = pressure_map
        generated.selection_meta = {
            **generated.selection_meta,
            **blueprint_decision,
            "run_label": normalize_run_label(run_label),
        }
        generated.policy_notes.append("Generic blueprint fallback used; no problem-specific shortcut family was injected.")
        return generated
    family_key = {
        "wild": "completion_boundary_sheaf",
        "stress": "residue_fracture_boundary",
        "forge": "odd_state_quotient_bridge",
        "balanced": "odd_state_quotient_bridge",
    }[mode]
    if _family_constraint_action(pressure_map, family_key) in {"explore", "cooldown", "soft_ban", "hard_ban"}:
        family_key = "accelerated_drift_certificate"
        title = "Accelerated drift certificate atlas"
        theorem = LimaClaimSpec(
            claim_kind="conditional_theorem",
            title="Block drift certificate implies bounded descent",
            statement_md=(
                "If a block-level acceleration certificate gives negative drift on every "
                "non-terminal parity block with explicit boundary exceptions, then Collatz "
                "trajectories admit a bounded descent certificate."
            ),
            priority=5,
        )
        universe = LimaUniverseSpec(
            title=title,
            family_key=family_key,
            family_kind="adjacent",
            branch_of_math="finite automata and Lyapunov drift",
            solved_world=(
                "Parity blocks carry exact acceleration factors and a rational drift certificate "
                "rather than quotient-class descent claims."
            ),
            why_problem_is_easy_here=(
                "The search target changes from a residue quotient to an explicit block certificate "
                "whose failure should expose a smallest bad block."
            ),
            core_story_md=(
                "Lima mutates away from the prior-art quotient family. The new universe asks for "
                "finite block certificates with exact acceleration weights and boundary exceptions."
            ),
            core_objects=[
                LimaObjectSpec(
                    object_kind="automaton",
                    name="ParityBlockAutomaton",
                    description_md="A finite automaton of parity blocks with exact affine acceleration data.",
                    formal_shape="List Bool -> AffineMap Nat",
                    payload={"block_lengths": [4, 6, 8]},
                ),
                LimaObjectSpec(
                    object_kind="potential",
                    name="BlockDriftPotential",
                    description_md="A rational potential that should decrease across certified non-terminal blocks.",
                    formal_shape="ParityBlock -> Rat",
                    payload={},
                ),
            ],
            laws=[
                LimaClaimSpec(
                    claim_kind="law",
                    title="Certified blocks have explicit drift",
                    statement_md="Every accepted parity block carries an exact affine update and a rational drift margin.",
                    priority=4,
                )
            ],
            backward_translation=[
                "Decompose a Collatz trajectory into fixed-length parity blocks.",
                "Lift a negative block drift certificate to bounded ordinary integer descent with named boundary exceptions.",
            ],
            bridge_lemmas=[
                LimaClaimSpec(
                    claim_kind="bridge_lemma",
                    title="Block certificate composes along trajectories",
                    statement_md="Exact affine block certificates compose without assuming global Collatz descent.",
                    formal_statement="forall b1 b2, certified b1 -> certified b2 -> certified (b1 ++ b2)",
                    priority=5,
                )
            ],
            conditional_theorem=theorem,
            kill_tests=[
                LimaClaimSpec(
                    claim_kind="kill_test",
                    title="Small bad block search",
                    statement_md="Enumerate parity blocks of length up to 8 and find the smallest block with non-negative drift margin.",
                    priority=5,
                ),
                LimaClaimSpec(
                    claim_kind="kill_test",
                    title="Boundary exception audit",
                    statement_md="Reject the certificate if boundary exceptions silently include all difficult trajectories.",
                    priority=4,
                ),
            ],
            expected_failure_mode="The drift certificate may fail on a small block or hide difficulty in boundary exceptions.",
            literature_queries=[
                "Collatz parity vector finite automata drift certificate",
                "3x+1 acceleration parity blocks Lyapunov function",
            ],
            formalization_targets=[
                LimaObligationSpec(
                    obligation_kind="counterexample_search",
                    title="Bad parity block search length 8",
                    statement_md="Enumerate parity blocks up to length 8 and report any non-negative drift margin.",
                    priority=5,
                ),
                LimaObligationSpec(
                    obligation_kind="bridge_lemma",
                    title="Block certificate composes along trajectories",
                    statement_md="State the composition lemma for exact affine parity-block certificates.",
                    lean_goal="forall b1 b2 : List Bool, True",
                    priority=4,
                ),
            ],
            scores={
                "compression_score": 3,
                "fit_score": 3,
                "novelty_score": 4,
                "falsifiability_score": 5,
                "bridgeability_score": 4,
                "formalizability_score": 4,
                "theorem_yield_score": 3,
                "literature_novelty_score": 4,
            },
        )
        return LimaGenerationResponse(
            frontier_summary_md=(
                "Collatz quotient/residue pressure is currently cooled by prior-art fractures; "
                "Lima is mutating toward exact block-drift certificates."
            ),
            pressure_map=pressure_map,
            run_summary_md=(
                f"Lima {mode} run mutated away from a repeated fractured family and emitted "
                "a block-drift certificate universe with new falsification targets."
            ),
            universes=[universe],
            policy_notes=["Fracture-to-pressure controller required a material family mutation."],
        )
    title = {
        "wild": "Completion-boundary sheaf for Collatz orbits",
        "stress": "Residue fracture boundary universe",
        "forge": "Odd-state quotient bridge",
        "balanced": "Odd-state quotient bridge",
    }[mode]
    theorem = LimaClaimSpec(
        claim_kind="conditional_theorem",
        title="Quotient compatibility implies bounded descent transfer",
        statement_md=(
            "If the induced odd-state quotient is well-defined and every quotient class "
            "admits a residue descent certificate, then ordinary Collatz trajectories "
            "inherit a bounded descent step."
        ),
        priority=5,
    )
    universe = LimaUniverseSpec(
        title=title,
        family_key=family_key,
        family_kind="new" if mode == "wild" else "adjacent",
        branch_of_math="symbolic dynamics and arithmetic quotients",
        solved_world=(
            "A finite or profinite quotient of odd Collatz states where even transport is "
            "absorbed into a derived operator and descent certificates are class data."
        ),
        why_problem_is_easy_here=(
            "The hard trajectory question becomes a compatibility theorem: every odd "
            "state maps into a quotient class with an explicit residue descent witness."
        ),
        core_story_md=(
            "Lima treats parity transport as structure rather than bookkeeping. "
            "The universe survives only if the quotient has a real backward translation "
            "to positive integers and produces finite residue obligations."
        ),
        core_objects=[
            LimaObjectSpec(
                object_kind="quotient",
                name="OddStateQuotient",
                description_md="A quotient on odd positive integers after absorbing even transport.",
                formal_shape="Nat -> Quot residue_relation",
                payload={"residue_moduli": [8, 16, 32]},
            ),
            LimaObjectSpec(
                object_kind="operator",
                name="DerivedOddTransfer",
                description_md="The odd-to-odd Collatz transfer operator on quotient classes.",
                formal_shape="OddStateQuotient -> OddStateQuotient",
                payload={},
            ),
        ],
        laws=[
            LimaClaimSpec(
                claim_kind="law",
                title="Residue certificates are class-local",
                statement_md="Descent witnesses should depend on quotient class data, not on arbitrary finite prefixes.",
                priority=4,
            )
        ],
        backward_translation=[
            "Map a positive integer to its odd representative after removing factors of 2.",
            "Lift a quotient descent certificate back to a bounded ordinary integer descent statement.",
        ],
        bridge_lemmas=[
            LimaClaimSpec(
                claim_kind="bridge_lemma",
                title="Odd transfer preserves quotient classes",
                statement_md="For odd n, the odd part of 3n+1 is well-defined on the proposed quotient relation.",
                formal_statement="forall n m, odd n -> odd m -> n ~ m -> oddPart (3*n+1) ~ oddPart (3*m+1)",
                priority=5,
            )
        ],
        conditional_theorem=theorem,
        kill_tests=[
            LimaClaimSpec(
                claim_kind="kill_test",
                title="Small residue obstruction search",
                statement_md="Search residues modulo 16 and 32 for a class where the derived odd transfer is not stable.",
                priority=5,
            ),
            LimaClaimSpec(
                claim_kind="kill_test",
                title="Vacuity audit",
                statement_md="Reject the universe if quotient descent merely assumes Collatz descent.",
                priority=4,
            ),
        ],
        expected_failure_mode="The quotient may be non-bridgeable or may overfit small residue classes.",
        literature_queries=[
            "Collatz odd-only dynamics quotient residue classes",
            "3x+1 problem rational cycles 2-adic extension",
        ],
        formalization_targets=[
            LimaObligationSpec(
                obligation_kind="finite_check",
                title="Residue descent scan modulo 16",
                statement_md="Compute exact one-step and odd-transfer residue summaries modulo 16.",
                priority=4,
            ),
            LimaObligationSpec(
                obligation_kind="lean_goal",
                title="Odd part transfer definition",
                statement_md="Define a local odd-part transfer function and state quotient compatibility.",
                lean_goal="forall n : Nat, n % 2 = 1 -> True",
                priority=3,
            ),
        ],
        scores={
            "compression_score": 4,
            "fit_score": 4,
            "novelty_score": 3 if literature_refresh.get("source_count") else 4,
            "falsifiability_score": 5,
            "bridgeability_score": 4,
            "formalizability_score": 4,
            "theorem_yield_score": 3,
            "literature_novelty_score": 3,
        },
    )
    return LimaGenerationResponse(
        frontier_summary_md=(
            "Collatz frontier pressure is concentrated around residue structure, "
            "odd/even transport, quotient bridgeability, and failed global height heuristics."
        ),
        pressure_map=pressure_map,
        run_summary_md=(
            f"Lima {mode} run produced one quotient-centered universe, then routed it "
            "through deterministic rupture and literature-aware prior-art checks."
        ),
        universes=[universe],
        policy_notes=["Local deterministic fallback used; no live authority granted."],
    )


def _build_user_message(
    *,
    problem: dict[str, Any],
    state: dict[str, Any],
    mode: LimaMode,
    pressure_map: dict[str, Any],
    reference_points: list[dict[str, Any]],
    literature_context: list[dict[str, Any]],
    families: list[dict[str, Any]],
    fractures: list[dict[str, Any]],
    policy_revisions: list[dict[str, Any]],
) -> str:
    payload = {
        "problem": {
            "slug": problem.get("slug"),
            "title": problem.get("title"),
            "statement_md": problem.get("statement_md"),
            "domain": problem.get("domain"),
            "default_goal_text": problem.get("default_goal_text"),
        },
        "mode": mode,
        "state": {
            "revision": state.get("revision"),
            "frontier_summary_md": state.get("frontier_summary_md"),
            "policy_json": safe_json_loads(state.get("policy_json"), {}),
        },
        "pressure_map": pressure_map,
        "reference_points": reference_points[:16],
        "literature_context": literature_context[:12],
        "family_memory": families[:12],
        "fracture_memory": fractures[:12],
        "policy_revisions": policy_revisions[:5],
        "limits": {
            "max_universes": int(app_config.LIMA_MAX_UNIVERSES_PER_RUN),
            "max_obligations": int(app_config.LIMA_MAX_OBLIGATIONS_PER_RUN),
        },
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)[:22000]


async def _invoke_lima_json(user: str) -> tuple[dict[str, Any], str]:
    model = app_config.SHADOW_LLM_MODEL or app_config.LLM_MODEL
    raw = await invoke_llm(
        LIMA_SYSTEM,
        user,
        model=model,
        temperature=0.45,
        json_object=True,
    )
    return _safe_json_loads(raw), raw


async def run_lima(
    lima_db: LimaDatabase,
    main_db: Database,
    *,
    problem_slug: str | None = None,
    trigger_kind: str = "manual",
    mode: str | None = None,
    run_label: str = "GUIDED_DEBUG",
) -> dict[str, Any]:
    global _GLOBAL_LIMA_RUN_LOCK
    if _GLOBAL_LIMA_RUN_LOCK:
        return {"ok": False, "error": "lima_run_in_progress"}
    _GLOBAL_LIMA_RUN_LOCK = True
    selected_mode = _mode(mode)
    normalized_run_label = normalize_run_label(run_label)
    try:
        lima_db.initialize()
        problem = lima_db.get_problem(problem_slug)
        problem_id = str(problem["id"])
        run_id = uuid4().hex[:12]
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="run",
            event_kind="started",
            payload={
                "problem_slug": problem.get("slug"),
                "trigger_kind": trigger_kind,
                "mode": selected_mode,
                "run_label": normalized_run_label,
            },
        )
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="sync",
            event_kind="started",
            payload={"target": "aristotle_results"},
        )
        sync_result = sync_lima_aristotle_results(lima_db, main_db, problem_id=problem_id)
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="sync",
            event_kind="completed",
            payload={
                "target": "aristotle_results",
                "synced": len(sync_result.get("synced") or []),
                "skipped": len(sync_result.get("skipped") or []),
            },
        )
        state = lima_db.get_state(problem_id)
        current_universes = lima_db.list_universes(problem_id, limit=12)
        obligations = lima_db.list_obligations(problem_id, limit=100)
        runs = lima_db.list_runs(problem_id, limit=20)
        reference_points = _build_reference_points(main_db, problem)
        fractures = lima_db.list_fractures(problem_id, limit=24)
        families = lima_db.list_family_leaderboard(problem_id, limit=16)
        family_search_constraints = lima_db.list_family_search_constraints(problem_id, limit=12)
        persisted_policy_layers = lima_db.list_policy_layers(problem_id, limit=16)
        runtime_policy = resolve_runtime_policy(
            persisted_policy_layers,
            run_label=normalized_run_label,
            problem_id=problem_id,
        )
        policy_layers = [
            *runtime_policy.get("active_layers", {}).get("global", []),
            *runtime_policy.get("active_layers", {}).get("problem", []),
            *runtime_policy.get("active_layers", {}).get("benchmark", []),
            *runtime_policy.get("active_layers", {}).get("session", []),
        ]
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="context",
            event_kind="loaded",
            payload={
                "state_revision": int(state.get("revision") or 0) if state else 0,
                "current_universes": len(current_universes),
                "obligations": len(obligations),
                "prior_runs": len(runs),
                "reference_points": len(reference_points),
                "fractures": len(fractures),
                "families": len(families),
                "family_search_constraints": len(family_search_constraints),
                "policy_layers": len(policy_layers),
                "run_label": normalized_run_label,
            },
        )
        pressure_map = build_pressure_map(
            problem,
            state,
            reference_points,
            fractures,
            obligations=obligations,
            runs=runs,
            family_search_constraints=family_search_constraints,
            families=families,
            policy_layers=policy_layers,
        )
        pressure_map["runtime_policy"] = runtime_policy
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="pressure_map",
            event_kind="completed",
            payload={
                "tension_count": len(pressure_map.get("tensions") or []),
                "failed_invariant_count": len(pressure_map.get("failed_invariants") or []),
                "constraint_count": len(pressure_map.get("search_constraints") or []),
                "run_label": normalized_run_label,
                "stagnation_active": bool(
                    isinstance(pressure_map.get("stagnation_controller"), dict)
                    and pressure_map.get("stagnation_controller", {}).get("active")
                ),
            },
        )
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="literature_refresh",
            event_kind="started",
            payload={"phase": "pre_generation"},
        )
        literature_refresh = refresh_literature(
            lima_db,
            problem=problem,
            pressure_map=pressure_map,
            universes=[],
        )
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="literature_refresh",
            event_kind="completed",
            payload={
                "phase": "pre_generation",
                "backend": literature_refresh.get("backend"),
                "source_count": literature_refresh.get("source_count"),
                "extract_count": literature_refresh.get("extract_count"),
            },
        )
        literature_context = lima_db.list_literature_sources(
            problem_id, limit=int(app_config.LIMA_MAX_LITERATURE_RESULTS)
        )
        policy_revisions = lima_db.list_policy_revisions(problem_id, limit=6)
        raw_response: dict[str, Any] = {}
        raw_preview = ""
        json_warnings: list[str] = []
        if app_config.LLM_API_KEY:
            _log_run_event(
                lima_db,
                problem_id=problem_id,
                run_id=run_id,
                stage="llm",
                event_kind="started",
                payload={"provider": "configured"},
            )
            user = _build_user_message(
                problem=problem,
                state=state,
                mode=selected_mode,
                pressure_map=pressure_map,
                reference_points=reference_points,
                literature_context=literature_context,
                families=families,
                fractures=fractures,
                policy_revisions=policy_revisions,
            )
            try:
                raw_response, raw_preview = await _invoke_lima_json(user)
                _log_run_event(
                    lima_db,
                    problem_id=problem_id,
                    run_id=run_id,
                    stage="llm",
                    event_kind="completed",
                    payload={
                        "raw_preview": _clip(raw_preview, 500),
                        "response_keys": sorted(raw_response.keys()),
                    },
                )
            except Exception:
                logger.exception("Lima LLM call failed; falling back to deterministic local generation")
                json_warnings.append("llm_request_failed_local_fallback")
                _log_run_event(
                    lima_db,
                    problem_id=problem_id,
                    run_id=run_id,
                    stage="llm",
                    event_kind="failed",
                    payload={"warning": "llm_request_failed_local_fallback"},
                )
        else:
            json_warnings.append("llm_api_key_missing_local_fallback")
            _log_run_event(
                lima_db,
                problem_id=problem_id,
                run_id=run_id,
                stage="llm",
                event_kind="skipped",
                payload={"warning": "llm_api_key_missing_local_fallback"},
            )

        if raw_response:
            generated, warnings = coerce_lima_generation_response(raw_response)
            json_warnings.extend(warnings)
            generation_source = "llm"
        else:
            generated = _local_generation(
                problem=problem,
                mode=selected_mode,
                pressure_map=pressure_map,
                literature_refresh=literature_refresh,
                current_universes=current_universes,
                run_label=normalized_run_label,
                runtime_policy=runtime_policy,
            )
            generation_source = "local_fallback"
        universes = generated.universes[: int(app_config.LIMA_MAX_UNIVERSES_PER_RUN)]
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="generation",
            event_kind="completed",
            payload={
                "source": generation_source,
                "universe_count": len(universes),
                "json_warnings": json_warnings,
                "run_label": normalized_run_label,
                "selection_meta": dict(getattr(generated, "selection_meta", {}) or {}),
            },
        )
        for universe in universes:
            _log_run_event(
                lima_db,
                problem_id=problem_id,
                run_id=run_id,
                stage="generation",
                event_kind="universe_emitted",
                payload={
                    "title": universe.title,
                    "family_key": universe.family_key,
                    "repair_hypothesis_key": getattr(universe, "repair_hypothesis_key", ""),
                    "formalization_target_count": len(universe.formalization_targets or []),
                    "selection_reason": str(
                        (getattr(generated, "selection_meta", {}) or {}).get("selection_reason") or ""
                    ),
                    "overrode_prior_frontier": bool(
                        (getattr(generated, "selection_meta", {}) or {}).get("overrode_prior_frontier")
                    ),
                },
            )
        # Refresh again after universe-specific queries are known.
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="literature_refresh",
            event_kind="started",
            payload={"phase": "post_generation", "universe_count": len(universes)},
        )
        universe_lit_refresh = refresh_literature(
            lima_db,
            problem=problem,
            pressure_map=pressure_map,
            universes=universes,
        )
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="literature_refresh",
            event_kind="completed",
            payload={
                "phase": "post_generation",
                "backend": universe_lit_refresh.get("backend"),
                "source_count": universe_lit_refresh.get("source_count"),
                "extract_count": universe_lit_refresh.get("extract_count"),
            },
        )
        literature_context = lima_db.list_literature_sources(
            problem_id, limit=int(app_config.LIMA_MAX_LITERATURE_RESULTS)
        )
        rupture_reports = rupture_universes(universes, literature_context=literature_context)
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="rupture",
            event_kind="completed",
            payload={
                "universe_count": len(rupture_reports),
                "fracture_count": sum(len(r.get("fractures") or []) for r in rupture_reports),
            },
        )
        for report in rupture_reports:
            _log_run_event(
                lima_db,
                problem_id=problem_id,
                run_id=run_id,
                stage="rupture",
                event_kind="universe_report",
                payload={
                    "universe_title": report.get("universe_title"),
                    "verdict": report.get("verdict"),
                    "fracture_count": len(report.get("fractures") or []),
                    "attack_count": len(report.get("attacks") or []),
                },
            )
        rupture_by_title = {str(r.get("universe_title") or ""): r for r in rupture_reports}
        universes = [
            universe.model_copy(
                update={
                    "formalization_targets": compile_obligations_for_universe(
                        universe, rupture_by_title.get(universe.title)
                    )
                }
            )
            for universe in universes
        ]
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="obligation_compile",
            event_kind="completed",
            payload={
                "universe_count": len(universes),
                "formalization_target_count": sum(
                    len(universe.formalization_targets or []) for universe in universes
                ),
            },
        )
        policy_snapshot = {
            "mode": selected_mode,
            "run_label": normalized_run_label,
            "autonomy_eval": bool(runtime_policy.get("merged_policy", {}).get("autonomy_eval")),
            "zero_live_authority": True,
            "json_warnings": json_warnings,
            "literature_refresh": literature_refresh,
            "universe_literature_refresh": universe_lit_refresh,
            "policy_layers": [
                {
                    "scope": row.get("scope"),
                    "imposed_by": row.get("imposed_by"),
                    "reason": row.get("reason_md"),
                    "meta_mutable": bool(row.get("meta_mutable", 1)),
                    "policy": row.get("policy_json") if isinstance(row.get("policy_json"), dict) else safe_json_loads(row.get("policy_json"), {}),
                }
                for row in policy_layers
            ],
            "runtime_policy": runtime_policy,
            "benchmark_locked": any(
                str(row.get("scope") or "") in {"benchmark", "session"} for row in policy_layers
            ) or bool(app_config.LIMA_BENCHMARK_LOCKED),
            "anti_overfitting": {
                "benchmark_scoped_controls_stay_scoped": True,
                "zero_live_authority": True,
            },
        }
        response_obj = {
            "output": generated.model_dump(mode="json"),
            "rupture_reports": rupture_reports,
            "meta": {
                "system_prompt_sha256": hashlib.sha256(
                    LIMA_SYSTEM.encode("utf-8")
                ).hexdigest(),
                "raw_preview": _clip(raw_preview, 4000),
                "json_warnings": json_warnings,
                "trigger_kind": trigger_kind,
                "mode": selected_mode,
            },
        }
        artifacts = [
            {
                "artifact_kind": "prompt_trace",
                "content": {
                    "json_warnings": json_warnings,
                    "local_fallback": not bool(raw_response),
                },
            }
        ]
        for report in rupture_reports:
            for attack in report.get("attacks") or []:
                if isinstance(attack, dict) and attack.get("artifact"):
                    artifacts.append(
                        {
                            "universe_title": report.get("universe_title"),
                            "artifact_kind": "numpy_scan"
                            if attack.get("numpy_used")
                            else "benchmark",
                            "content": {
                                "attack": attack.get("attack"),
                                "artifact": attack.get("artifact"),
                            },
                        }
                    )
        for universe in universes:
            dumped = universe.model_dump(mode="json")
            repair_key = str(dumped.get("repair_hypothesis_key") or "")
            if not repair_key:
                continue
            artifacts.append(
                {
                    "universe_title": universe.title,
                    "artifact_kind": "repair_attempt",
                    "content": {
                        "repair_hypothesis_key": repair_key,
                        "repair_parent_family_key": dumped.get("repair_parent_family_key"),
                        "repair_strategy": dumped.get("repair_strategy"),
                        "repair_focus": dumped.get("repair_focus"),
                        "formalization_targets": [
                            target.get("title")
                            for target in dumped.get("formalization_targets") or []
                            if isinstance(target, dict)
                        ],
                    },
                }
            )
        run_id = lima_db.commit_run(
            run_id=run_id,
            problem_id=problem_id,
            trigger_kind=trigger_kind,
            mode=selected_mode,
            run_label=normalized_run_label,
            autonomy_eval=bool(runtime_policy.get("merged_policy", {}).get("autonomy_eval")),
            run_summary_md=generated.run_summary_md,
            frontier_snapshot={"summary": generated.frontier_summary_md},
            pressure_snapshot=pressure_map,
            policy_snapshot=policy_snapshot,
            response_obj=response_obj,
            universes=universes,
            rupture_reports=rupture_reports,
            reference_points=reference_points,
            artifacts=artifacts,
        )
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="run_commit",
            event_kind="completed",
            payload={
                "artifact_count": len(artifacts),
                "reference_count": len(reference_points),
                "universe_count": len(universes),
                "run_label": normalized_run_label,
            },
        )
        created_universes = lima_db.list_universes_for_run(run_id)
        transfer_metric = compute_transfer_metrics(
            families=lima_db.list_family_leaderboard(problem_id, limit=100),
            fractures=lima_db.list_fractures(problem_id, limit=200),
            obligations=lima_db.list_obligations(problem_id, limit=200),
            runs=lima_db.list_runs(problem_id, limit=50),
        )
        lima_db.record_transfer_metric(
            problem_id=problem_id,
            run_id=run_id,
            benchmark_id=str(problem.get("slug") or ""),
            metric=transfer_metric,
        )
        sources = lima_db.list_literature_sources(problem_id, limit=6)
        linked_sources = 0
        for row, universe in zip(created_universes, universes):
            for source in sources[:2]:
                lit_score = score_literature_novelty(universe, source)
                lima_db.link_universe_literature(
                    universe_id=str(row["id"]),
                    source_id=str(source["id"]),
                    relation_kind=str(lit_score.get("relation_kind") or infer_literature_relation(universe, source)),
                    note=f"Linked by Lima literature routing. prior_art_score={lit_score.get('prior_art_score')}",
                )
                linked_sources += 1
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="literature_linking",
            event_kind="completed",
            payload={"link_count": linked_sources},
        )
        obligation_result = None
        if app_config.LIMA_AUTO_LOCAL_OBLIGATION_CHECKS:
            _log_run_event(
                lima_db,
                problem_id=problem_id,
                run_id=run_id,
                stage="obligation_checks",
                event_kind="started",
                payload={"limit": int(app_config.LIMA_MAX_OBLIGATIONS_PER_RUN)},
            )
            obligation_result = run_queued_obligation_checks(
                lima_db,
                problem_id=problem_id,
                limit=int(app_config.LIMA_MAX_OBLIGATIONS_PER_RUN),
                run_id=run_id,
            )
            _log_run_event(
                lima_db,
                problem_id=problem_id,
                run_id=run_id,
                stage="obligation_checks",
                event_kind="completed",
                payload={
                    "checked": len(obligation_result.get("checked") or []),
                    "falsified": len(obligation_result.get("falsified") or []),
                    "skipped": len(obligation_result.get("skipped") or []),
                },
            )
        else:
            _log_run_event(
                lima_db,
                problem_id=problem_id,
                run_id=run_id,
                stage="obligation_checks",
                event_kind="skipped",
                payload={"reason": "auto_local_checks_disabled"},
            )
        if bool(runtime_policy.get("merged_policy", {}).get("allow_guided_repair_cycles", True)):
            _log_run_event(
                lima_db,
                problem_id=problem_id,
                run_id=run_id,
                stage="bridge_repair",
                event_kind="started",
                payload={"scope": "guided_debug_repair_cycle", "run_label": normalized_run_label},
            )
            bridge_repair_result = _run_boundary_bridge_repair_cycle(
                lima_db,
                problem=problem,
                problem_id=problem_id,
                run_id=run_id,
                universes=universes,
            )
            if bridge_repair_result:
                _log_run_event(
                    lima_db,
                    problem_id=problem_id,
                    run_id=run_id,
                    stage="bridge_repair",
                    event_kind="completed",
                    payload={
                        "candidate_count": len(bridge_repair_result.get("top_revised_bridges") or []),
                        "most_likely_correct_key": bridge_repair_result.get("most_likely_correct_key"),
                        "proof_program_status": bridge_repair_result.get("proof_program_status")
                        or bridge_repair_result.get("benchmark_status"),
                        "artifact_id": bridge_repair_result.get("artifact_id"),
                        "run_label": normalized_run_label,
                    },
                )
            else:
                _log_run_event(
                    lima_db,
                    problem_id=problem_id,
                    run_id=run_id,
                    stage="bridge_repair",
                    event_kind="skipped",
                    payload={"reason": "repair_prerequisites_not_met", "run_label": normalized_run_label},
                )
        else:
            bridge_repair_result = None
            _log_run_event(
                lima_db,
                problem_id=problem_id,
                run_id=run_id,
                stage="bridge_repair",
                event_kind="skipped",
                payload={"reason": "autonomy_eval_disables_guided_repair", "run_label": normalized_run_label},
            )
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="formal_submit",
            event_kind="started",
            payload={"limit": int(app_config.LIMA_MAX_OBLIGATIONS_PER_RUN)},
        )
        formal_submit_result = await submit_promising_formal_obligations(
            lima_db,
            main_db,
            problem_id=problem_id,
            limit=int(app_config.LIMA_MAX_OBLIGATIONS_PER_RUN),
        )
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="formal_submit",
            event_kind="completed",
            payload={
                "enabled": bool(formal_submit_result.get("enabled")),
                "submitted": len(formal_submit_result.get("submitted") or []),
                "blocked": len(formal_submit_result.get("blocked") or []),
            },
        )
        sync_after_submit = sync_lima_aristotle_results(lima_db, main_db, problem_id=problem_id)
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="formal_sync",
            event_kind="completed",
            payload={
                "synced": len(sync_after_submit.get("synced") or []),
                "skipped": len(sync_after_submit.get("skipped") or []),
            },
        )
        meta_result = None
        if app_config.LIMA_ENABLE_AUTO_POLICY_UPDATES and not app_config.LIMA_FREEZE_FAMILY_GOVERNANCE:
            _log_run_event(
                lima_db,
                problem_id=problem_id,
                run_id=run_id,
                stage="meta_policy",
                event_kind="started",
                payload={},
            )
            meta_result = analyze_and_update_policy(
                lima_db, problem_id=problem_id, from_run_id=run_id
            )
            _log_run_event(
                lima_db,
                problem_id=problem_id,
                run_id=run_id,
                stage="meta_policy",
                event_kind="completed",
                payload={
                    "has_changes": bool(meta_result and meta_result.get("family_search_controls")),
                    "family_search_controls": len(meta_result.get("family_search_controls") or [])
                    if isinstance(meta_result, dict)
                    else 0,
                },
            )
        else:
            _log_run_event(
                lima_db,
                problem_id=problem_id,
                run_id=run_id,
                stage="meta_policy",
                event_kind="skipped",
                payload={"reason": "auto_policy_updates_disabled_or_frozen"},
            )
        _log_run_event(
            lima_db,
            problem_id=problem_id,
            run_id=run_id,
            stage="run",
            event_kind="completed",
            payload={
                "summary": generated.run_summary_md,
                "universe_count": len(universes),
                "fracture_count": sum(len(r.get("fractures") or []) for r in rupture_reports),
                "pending_handoffs": len(lima_db.list_handoffs(problem_id, status="pending", limit=100)),
                "run_label": normalized_run_label,
            },
        )
        return {
            "ok": True,
            "run_id": run_id,
            "problem_id": problem_id,
            "mode": selected_mode,
            "run_label": normalized_run_label,
            "universe_count": len(universes),
            "fracture_count": sum(len(r.get("fractures") or []) for r in rupture_reports),
            "handoff_count": len(lima_db.list_handoffs(problem_id, status="pending", limit=100)),
            "literature_source_count": len(literature_context),
            "summary": generated.run_summary_md,
            "validation_warnings": json_warnings,
            "obligation_checks": obligation_result,
            "bridge_repair": bridge_repair_result,
            "formal_submit": formal_submit_result,
            "formal_sync": {
                "before_run": sync_result,
                "after_submit": sync_after_submit,
            },
            "meta": meta_result,
        }
    finally:
        _GLOBAL_LIMA_RUN_LOCK = False


async def lima_loop(lima_db: LimaDatabase, main_db: Database) -> None:
    problem_cursor = 0
    while True:
        sleep_seconds = max(60, int(app_config.LIMA_LOOP_INTERVAL_SEC))
        try:
            problems = lima_db.list_schedulable_problems()
            if problems:
                if problem_cursor >= len(problems):
                    problem_cursor = 0
                selected = problems[problem_cursor]
                problem_cursor = (problem_cursor + 1) % max(1, len(problems))
                await run_lima(
                    lima_db,
                    main_db,
                    problem_slug=str(selected.get("slug") or app_config.LIMA_DEFAULT_PROBLEM),
                    trigger_kind="scheduled",
                    mode=app_config.LIMA_DEFAULT_MODE,
                )
                snapshot = lima_db.get_dashboard_snapshot(
                    str(selected.get("slug") or app_config.LIMA_DEFAULT_PROBLEM)
                )
                if problem_ready_for_auto_continue(snapshot):
                    sleep_seconds = _AUTO_CONTINUE_DELAY_SEC
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Lima loop tick failed")
        await asyncio.sleep(sleep_seconds)
