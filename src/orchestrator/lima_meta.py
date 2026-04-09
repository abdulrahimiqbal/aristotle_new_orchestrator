from __future__ import annotations

from typing import Any

from orchestrator import config as app_config
from orchestrator.lima_db import LimaDatabase
from orchestrator.lima_models import normalize_family_governance_state


REQUIRED_DELTA_BY_FAILURE = {
    "prior_art": [
        "introduce a literature-distinct mathematical object",
        "replace the bridge lemma with a non-prior-art bridge",
        "use a cited tool as support rather than claiming novelty",
        "add a falsifier that distinguishes the family from known work",
    ],
    "vacuity": [
        "state a non-vacuous witness or counterexample boundary",
        "remove assumptions equivalent to the target theorem",
        "add an independent bounded test",
    ],
    "counterexample": [
        "change the boundary assumptions",
        "isolate the smallest counterexample region",
        "weaken the claim into a surviving fragment",
    ],
    "formal_blocker": [
        "split the claim into a smaller bridge lemma",
        "replace informal definitions with typed objects",
        "add a Lean-shaped statement with explicit hypotheses",
    ],
    "weak_explanation": [
        "add a concrete object, invariant, or operator",
        "state what would make the family false",
        "provide a bridge back to the original problem",
    ],
    "underparameterized_state": [
        "identify what information the scalar progress variable loses",
        "add a companion coordinate, context label, cocycle, memory state, or defect variable",
        "derive exact transition laws in the repaired state description",
    ],
}


def _search_action_for_family(
    family: dict[str, Any],
    family_fractures: list[dict[str, Any]],
) -> dict[str, Any]:
    current_governance = normalize_family_governance_state(
        family.get("governance_state") or family.get("search_action") or "exploit"
    )
    if (
        current_governance in {"hard_ban", "soft_ban", "cooldown"}
        and int(family.get("governance_meta_mutable") or 0) == 0
    ):
        return {
            "family_key": str(family.get("family_key") or ""),
            "search_action": current_governance,
            "governance_state": current_governance,
            "governance_scope": str(family.get("governance_scope") or "problem"),
            "repeat_failure_count": int(family.get("repeat_failure_count") or 0),
            "last_failure_type": str(family.get("last_failure_type") or ""),
            "required_delta": [],
            "reason_md": (
                "Preserving non-meta-mutable scoped family governance; "
                "Meta-Lima may not revive it inside this scope."
            ),
            "meta_mutable": False,
        }
    histogram: dict[str, int] = {}
    for fracture in family_fractures:
        failure_type = str(fracture.get("failure_type") or "weak_explanation")
        histogram[failure_type] = histogram.get(failure_type, 0) + 1
    if not histogram:
        return {
            "family_key": str(family.get("family_key") or ""),
            "search_action": "exploit",
            "repeat_failure_count": 0,
            "last_failure_type": "",
            "required_delta": [],
            "reason_md": "No repeated fracture pressure.",
        }

    failure_type, repeat_count = sorted(
        histogram.items(), key=lambda kv: kv[1], reverse=True
    )[0]
    formal_wins = int(family.get("formal_win_count") or 0)
    survival_count = int(family.get("survival_count") or 0)
    required_delta = REQUIRED_DELTA_BY_FAILURE.get(
        failure_type, REQUIRED_DELTA_BY_FAILURE["weak_explanation"]
    )
    if repeat_count >= 8 and formal_wins <= 0:
        action = "hard_ban"
    elif (
        failure_type == "underparameterized_state"
        and repeat_count >= 2
        and formal_wins <= 0
        and survival_count <= 0
    ):
        action = "cooldown"
    elif repeat_count >= 3 and failure_type == "prior_art" and formal_wins <= 0:
        action = "cooldown"
    elif repeat_count >= 3 and formal_wins <= 0:
        action = "explore"
    elif survival_count > 0 or formal_wins > 0:
        action = "exploit"
    else:
        action = "explore" if repeat_count >= 2 else "exploit"
    reason = (
        f"Family '{family.get('family_key')}' has {repeat_count} recent "
        f"{failure_type} fracture(s), formal wins={formal_wins}, survivors={survival_count}."
    )
    return {
        "family_key": str(family.get("family_key") or ""),
        "search_action": action,
        "governance_state": action,
        "repeat_failure_count": repeat_count,
        "last_failure_type": failure_type,
        "required_delta": required_delta if action != "exploit" else [],
        "reason_md": reason,
        "failure_histogram": histogram,
    }


def compute_transfer_metrics(
    *,
    families: list[dict[str, Any]],
    fractures: list[dict[str, Any]],
    obligations: list[dict[str, Any]],
    runs: list[dict[str, Any]],
) -> dict[str, Any]:
    total_families = max(1, len(families))
    duplicate_family_count = sum(
        1 for family in families if int(family.get("survival_count") or 0) + int(family.get("failure_count") or 0) > 1
    )
    ontology_histogram: dict[str, int] = {}
    for family in families:
        cls = str(family.get("ontology_class") or "other")
        ontology_histogram[cls] = ontology_histogram.get(cls, 0) + 1
    useful_obligations = [
        o for o in obligations if str(o.get("status") or "") in {"queued_formal_review", "verified_local", "approved_for_formal", "submitted_formal", "verified_formal"}
    ]
    governance_scoped = [
        f for f in families if str(f.get("governance_scope") or "problem") in {"benchmark", "session"}
    ]
    governance_global = [
        f for f in families if str(f.get("governance_scope") or "problem") == "global"
    ]
    return {
        "run_count": len(runs),
        "duplicate_family_rate": duplicate_family_count / total_families,
        "fracture_reuse_effectiveness": sum(1 for f in families if int(f.get("repeat_failure_count") or 0) > 0) / total_families,
        "ontology_class_distribution": ontology_histogram,
        "useful_obligation_rate": len(useful_obligations) / max(1, len(obligations)),
        "proof_program_recovery_rate": sum(
            1 for o in obligations if str(o.get("obligation_kind") or "") in {"bridge_lemma", "equivalence", "lean_goal"}
        ) / max(1, len(obligations)),
        "need_for_manual_steering": sum(
            1 for f in families if str(f.get("governance_state") or "") in {"hard_ban", "soft_ban", "cooldown"}
        ),
        "benchmark_leakage_risk": len(governance_global) if governance_scoped else 0,
        "local_vs_global_policy_mutation_rate": {
            "scoped": len(governance_scoped),
            "global": len(governance_global),
        },
    }


def analyze_and_update_policy(
    lima_db: LimaDatabase,
    *,
    problem_id: str,
    from_run_id: str | None = None,
) -> dict[str, Any]:
    """Bounded Meta-Lima strategy update.

    This writes reversible policy rows only; it never mutates code or changes
    Lima's mission.
    """

    families = lima_db.list_family_leaderboard(problem_id, limit=50)
    fractures = lima_db.list_fractures(problem_id, limit=50)
    obligations = lima_db.list_obligations(problem_id, limit=50)
    runs = lima_db.list_runs(problem_id, limit=20)

    formalizable_families = [
        f for f in families if int(f.get("formal_win_count") or 0) > 0
    ]
    repeated_failures: dict[str, int] = {}
    for fracture in fractures:
        kind = str(fracture.get("failure_type") or "unknown")
        repeated_failures[kind] = repeated_failures.get(kind, 0) + 1

    queued_obligations = [
        o for o in obligations if str(o.get("status")) in {"queued", "queued_local", "queued_formal_review"}
    ]
    verified_local = [o for o in obligations if str(o.get("status")) == "verified_local"]
    refuted_local = [o for o in obligations if str(o.get("status")) == "refuted_local"]
    formal_ready = [
        o
        for o in obligations
        if str(o.get("status")) in {"approved_for_formal", "submitted_formal", "verified_formal"}
    ]
    prior_art_hits = [f for f in fractures if str(f.get("failure_type")) == "prior_art"]
    fractures_by_family: dict[str, list[dict[str, Any]]] = {}
    for fracture in fractures:
        family_key = str(fracture.get("family_key") or "")
        if family_key:
            fractures_by_family.setdefault(family_key, []).append(fracture)
    search_controls = [
        _search_action_for_family(
            family,
            fractures_by_family.get(str(family.get("family_key") or ""), []),
        )
        for family in families
    ]
    applied_controls = []
    for control in search_controls:
        family_key = str(control.get("family_key") or "")
        if not family_key:
            continue
        ok, msg = lima_db.update_family_search_control(
            problem_id=problem_id,
            family_key=family_key,
            search_action=str(control["search_action"]),
            reason_md=str(control["reason_md"]),
            required_delta=list(control.get("required_delta") or []),
            repeat_failure_count=int(control.get("repeat_failure_count") or 0),
            last_failure_type=str(control.get("last_failure_type") or ""),
            scope=str(control.get("governance_scope") or "problem"),
            imposed_by="meta_lima",
            evidence={"failure_histogram": control.get("failure_histogram") or {}},
            meta_mutable=bool(control.get("meta_mutable", True)),
        )
        if ok:
            applied_controls.append({**control, "update": msg})
    policy_changes = {
        "mission_locked": True,
        "generation": {
            "prefer_family_keys": [
                str(f.get("family_key")) for f in formalizable_families[:5]
            ],
            "avoid_empty_repetitions": True,
            "family_search_controls": [
                c
                for c in search_controls
                if str(c.get("search_action") or "") in {"explore", "cooldown", "soft_ban", "hard_ban", "mutate", "retire"}
            ][:8],
            "max_universes_per_run": int(app_config.LIMA_MAX_UNIVERSES_PER_RUN),
        },
        "rupture": {
            "emphasize_failure_types": [
                kind
                for kind, _count in sorted(
                    repeated_failures.items(), key=lambda kv: kv[1], reverse=True
                )[:5]
            ],
            "require_vacuity_and_bridgeability": True,
        },
        "literature": {
            "prior_art_check_required": True,
            "problem_aware_query_generation": True,
            "prior_art_hit_count": len(prior_art_hits),
        },
        "formal": {
            "escalate_only_after_human_approval": True,
            "queued_obligation_count": len(queued_obligations),
            "verified_local_count": len(verified_local),
            "refuted_local_count": len(refuted_local),
            "formal_ready_count": len(formal_ready),
        },
        "scoring": {
            "falsifiability_weight": 1.25 if repeated_failures else 1.0,
            "bridgeability_weight": 1.2,
            "literature_novelty_weight": 1.1,
            "complexity_penalty": 1.0,
        },
    }
    benchmark = {
        "run_count": len(runs),
        "family_count": len(families),
        "fracture_count": len(fractures),
        "queued_obligation_count": len(queued_obligations),
        "local_verification_yield": len(verified_local),
        "local_refutation_yield": len(refuted_local),
        "formalization_yield": len(formal_ready),
        "prior_art_hit_rate": (len(prior_art_hits) / max(1, len(fractures))),
        "family_survival_rate": (
            sum(int(f.get("survival_count") or 0) for f in families)
            / max(1, sum(int(f.get("survival_count") or 0) + int(f.get("failure_count") or 0) for f in families))
        ),
        "failure_type_histogram": repeated_failures,
        "family_search_controls": applied_controls[:12],
    }
    benchmark["transfer_metrics"] = compute_transfer_metrics(
        families=families,
        fractures=fractures,
        obligations=obligations,
        runs=runs,
    )
    summary = (
        "Meta-Lima updated reversible strategy policy. "
        f"Observed {len(fractures)} fracture(s), {len(families)} family/families, "
        f"and {len(queued_obligations)} queued obligation(s). "
        "Core mission and zero-live-authority constraints remain locked."
    )
    meta_id = lima_db.create_meta_run(
        problem_id=problem_id,
        from_run_id=from_run_id,
        analysis_summary_md=summary,
        policy_changes=policy_changes,
        benchmark=benchmark,
    )
    revision_id = lima_db.create_policy_revision(
        problem_id=problem_id,
        generation_policy=policy_changes["generation"],
        rupture_policy=policy_changes["rupture"],
        literature_policy=policy_changes["literature"],
        formal_policy=policy_changes["formal"],
        scoring_weights=policy_changes["scoring"],
        change_reason_md=summary,
    )
    return {
        "meta_id": meta_id,
        "revision_id": revision_id,
        "analysis_summary_md": summary,
        "policy_changes": policy_changes,
        "benchmark": benchmark,
        "family_search_controls": applied_controls,
    }
