from __future__ import annotations

import json
from typing import Any

from orchestrator import config as app_config
from orchestrator.lima_db import LimaDatabase
from orchestrator.lima_discovery import resolve_runtime_policy
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

_STAGNATION_RUN_WINDOW = 6
_STAGNATION_REPEAT_THRESHOLD = 3
_REPAIR_ATTEMPT_BUDGET = 4


def _load_json(raw: Any, default: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


def _run_frontier_signature(run: dict[str, Any]) -> dict[str, str]:
    response = _load_json(run.get("response_json"), {})
    output = response.get("output") if isinstance(response, dict) else {}
    universes = output.get("universes") if isinstance(output, dict) else []
    top_universe = universes[0] if isinstance(universes, list) and universes else {}
    rupture_reports = response.get("rupture_reports") if isinstance(response, dict) else []

    top_failure = ""
    top_blocker_title = ""
    top_confidence = -1.0
    for report in rupture_reports or []:
        if not isinstance(report, dict):
            continue
        attacks = report.get("attacks") or []
        for attack in attacks:
            if not isinstance(attack, dict):
                continue
            failure_type = str(attack.get("failure_type") or "")
            if not failure_type:
                continue
            try:
                confidence = float(attack.get("confidence") or 0)
            except (TypeError, ValueError):
                confidence = 0.0
            if confidence > top_confidence:
                top_confidence = confidence
                top_failure = failure_type
                top_blocker_title = str(report.get("universe_title") or "")

    return {
        "family_key": str((top_universe or {}).get("family_key") or ""),
        "title": str((top_universe or {}).get("title") or ""),
        "failure_type": top_failure,
        "blocker_title": top_blocker_title,
    }


def _recent_repair_attempt_keys(runs: list[dict[str, Any]]) -> list[str]:
    attempts: list[str] = []
    for run in runs[:_STAGNATION_RUN_WINDOW]:
        response = _load_json(run.get("response_json"), {})
        output = response.get("output") if isinstance(response, dict) else {}
        universes = output.get("universes") if isinstance(output, dict) else []
        if not isinstance(universes, list):
            continue
        for universe in universes:
            if not isinstance(universe, dict):
                continue
            key = str(universe.get("repair_hypothesis_key") or "").strip()
            if key and key not in attempts:
                attempts.append(key)
    return attempts


def _repair_hypotheses_for_frontier(
    *,
    family_key: str,
    dominant_blocker: str,
    attempted_keys: list[str],
) -> dict[str, Any]:
    if dominant_blocker != "underparameterized_state":
        return {
            "active": False,
            "strategy": "",
            "target_family_key": family_key,
            "failure_type": dominant_blocker,
            "attempt_budget": 0,
            "attempts_used": 0,
            "attempts_remaining": 0,
            "hypotheses": [],
            "recent_attempt_keys": attempted_keys,
            "next_hypothesis_keys": [],
            "summary_md": "",
        }

    chip_firing_like = "chip_firing" in family_key or "sandpile" in family_key or "sink" in family_key
    if chip_firing_like:
        hypotheses = [
            {
                "key": "boundary_debt_ledger",
                "title": "Boundary debt ledger",
                "description": "Track the mass lost to sinks as an explicit ledger so a boundary spill move becomes an exact augmented firing step.",
                "why_it_might_work": "The current bridge may be losing endpoint-relevant sink information at the boundary.",
                "check_focus": "Does the ledger make the boundary transition and endpoint projection deterministic?",
            },
            {
                "key": "boundary_context_tag",
                "title": "Boundary context tag",
                "description": "Add a local boundary context label recording which side and local pattern produced the spill before sink completion.",
                "why_it_might_work": "The missing information may be local and combinatorial rather than numeric.",
                "check_focus": "Do adjacent boundary firings commute once the context tag is tracked?",
            },
            {
                "key": "sink_parity_cocycle",
                "title": "Sink parity cocycle",
                "description": "Attach a cocycle term capturing parity or ordering information that the raw sinked configuration forgets.",
                "why_it_might_work": "The bridge may need a lightweight correction term rather than a full state redesign.",
                "check_focus": "Does the cocycle remove same-state/different-future counterexamples?",
            },
            {
                "key": "two_coordinate_defect_state",
                "title": "Two-coordinate defect state",
                "description": "Replace the one-number progress story with a paired state: primary scalar plus explicit boundary defect coordinate.",
                "why_it_might_work": "The frontier may need a minimal repaired state that is still formalizable.",
                "check_focus": "Can the repaired state support an exact transition law and unique endpoint proof?",
            },
        ]
        strategy = "companion_state_search"
    else:
        hypotheses = [
            {
                "key": "latent_coordinate",
                "title": "Latent coordinate repair",
                "description": "Introduce a latent coordinate that restores exact one-step evolution.",
                "why_it_might_work": "The current scalar invariant is likely projecting away essential information.",
                "check_focus": "Does the latent coordinate make the transition law exact?",
            },
            {
                "key": "memory_state",
                "title": "Memory state repair",
                "description": "Attach a bounded memory state that records the missing local context.",
                "why_it_might_work": "The system may depend on short-range history rather than just current scalar value.",
                "check_focus": "Do bounded memory states eliminate same-scalar divergence?",
            },
            {
                "key": "defect_variable",
                "title": "Defect variable repair",
                "description": "Track a defect variable measuring what the scalar forgets.",
                "why_it_might_work": "A small correction term may be enough to close the bridge.",
                "check_focus": "Can the defect variable prove a sharper bridge lemma?",
            },
            {
                "key": "quotient_label",
                "title": "Quotient label repair",
                "description": "Attach a quotient or regime label to distinguish states that collapse under the current representation.",
                "why_it_might_work": "The current ontology may need a finite label rather than a continuous correction.",
                "check_focus": "Does the quotient label make the local laws deterministic?",
            },
        ]
        strategy = "state_repair_search"

    annotated = []
    for idx, hypothesis in enumerate(hypotheses, start=1):
        key = str(hypothesis["key"])
        tried = key in attempted_keys
        annotated.append(
            {
                **hypothesis,
                "ordinal": idx,
                "tried": tried,
                "status": "tried" if tried else "queued",
            }
        )
    attempts_used = sum(1 for hypothesis in annotated if hypothesis["tried"])
    next_keys = [str(hypothesis["key"]) for hypothesis in annotated if not hypothesis["tried"]][:2]
    if not next_keys:
        next_keys = [str(hypothesis["key"]) for hypothesis in annotated[:2]]
    return {
        "active": True,
        "strategy": strategy,
        "target_family_key": family_key,
        "failure_type": dominant_blocker,
        "attempt_budget": _REPAIR_ATTEMPT_BUDGET,
        "attempts_used": attempts_used,
        "attempts_remaining": max(0, _REPAIR_ATTEMPT_BUDGET - attempts_used),
        "hypotheses": annotated,
        "recent_attempt_keys": attempted_keys,
        "next_hypothesis_keys": next_keys,
        "summary_md": (
            f"Repair loop is targeting {family_key or 'the current frontier'} with "
            f"{attempts_used}/{_REPAIR_ATTEMPT_BUDGET} companion-state attempt(s) already emitted."
        ),
    }


def compute_stagnation_controller(
    *,
    runs: list[dict[str, Any]],
    families: list[dict[str, Any]],
    fractures: list[dict[str, Any]],
    obligations: list[dict[str, Any]],
) -> dict[str, Any]:
    recent_runs = runs[:_STAGNATION_RUN_WINDOW]
    signatures = [_run_frontier_signature(run) for run in recent_runs]
    family_counts: dict[str, int] = {}
    blocker_counts: dict[str, int] = {}
    for sig in signatures:
        family_key = str(sig.get("family_key") or "")
        if family_key:
            family_counts[family_key] = family_counts.get(family_key, 0) + 1
        failure_type = str(sig.get("failure_type") or "")
        if failure_type:
            blocker_counts[failure_type] = blocker_counts.get(failure_type, 0) + 1

    top_family_key = max(family_counts, key=family_counts.get, default="")
    top_family_repeats = int(family_counts.get(top_family_key, 0))
    dominant_blocker = max(blocker_counts, key=blocker_counts.get, default="")
    dominant_blocker_repeats = int(blocker_counts.get(dominant_blocker, 0))
    active_family_rows = [
        family
        for family in families
        if str(family.get("family_key") or "") == top_family_key
    ]
    top_family = active_family_rows[0] if active_family_rows else {}
    strong_survivor_present = any(
        int(family.get("formal_win_count") or 0) > 0
        or int(family.get("survival_count") or 0) > 1
        for family in families
    )
    human_queue_waiting = any(
        str(obligation.get("status") or "") in {"queued", "queued_local", "running_local", "queued_formal_review"}
        for obligation in obligations
    )
    repeated_failure_pressure = [
        fracture for fracture in fractures if str(fracture.get("failure_type") or "") == dominant_blocker
    ]
    attempted_repair_keys = _recent_repair_attempt_keys(recent_runs)
    active = (
        len(recent_runs) >= 4
        and not strong_survivor_present
        and not human_queue_waiting
        and (
            top_family_repeats >= _STAGNATION_REPEAT_THRESHOLD
            or dominant_blocker_repeats >= _STAGNATION_REPEAT_THRESHOLD
        )
    )

    mode_shift = ""
    recommended_actions: list[str] = []
    repair_loop = {
        "active": False,
        "strategy": "",
        "target_family_key": top_family_key,
        "failure_type": dominant_blocker,
        "attempt_budget": 0,
        "attempts_used": 0,
        "attempts_remaining": 0,
        "hypotheses": [],
        "recent_attempt_keys": attempted_repair_keys,
        "next_hypothesis_keys": [],
        "summary_md": "",
    }
    if active:
        if dominant_blocker == "underparameterized_state":
            mode_shift = "bridge_first"
            recommended_actions = [
                "force a companion-object or defect-state mutation instead of another scalar potential",
                "favor exact bridge lemmas and commutation checks over new high-level summaries",
                "suppress repeated scalar families until they materially change their state representation",
            ]
            repair_loop = _repair_hypotheses_for_frontier(
                family_key=top_family_key,
                dominant_blocker=dominant_blocker,
                attempted_keys=attempted_repair_keys,
            )
        elif dominant_blocker == "prior_art":
            mode_shift = "literature_distinct_mutation"
            recommended_actions = [
                "force a literature-distinct object or operator before re-emitting the family",
                "treat cited prior-art tools as support rather than novelty claims",
                "pivot to a new ontology class if overlap remains unresolved",
            ]
        else:
            mode_shift = "ontology_rotation"
            recommended_actions = [
                "rotate ontology class away from the dominant stalled family",
                "compile one bridge-first obligation and one falsifier-first obligation",
                "prefer materially different universes over score-near repeats",
            ]

    avoid_family_keys = sorted(
        {
            str(family.get("family_key") or "")
            for family in families
            if str(family.get("family_key") or "")
            and int(family.get("formal_win_count") or 0) <= 0
            and (
                str(family.get("last_failure_type") or "") == dominant_blocker
                or int(family.get("repeat_failure_count") or 0) >= 3
            )
        }
    )
    prefer_family_keys = [
        str(family.get("family_key") or "")
        for family in families
        if str(family.get("family_key") or "")
        and int(family.get("formal_win_count") or 0) > 0
    ]
    if top_family_key and top_family_key not in prefer_family_keys and dominant_blocker != "underparameterized_state":
        prefer_family_keys.append(top_family_key)

    return {
        "active": active,
        "window_runs": len(recent_runs),
        "top_family_key": top_family_key,
        "top_family_repeats": top_family_repeats,
        "top_family_formal_wins": int(top_family.get("formal_win_count") or 0),
        "top_family_survivals": int(top_family.get("survival_count") or 0),
        "dominant_blocker": dominant_blocker,
        "dominant_blocker_repeats": dominant_blocker_repeats,
        "repeated_failure_count": len(repeated_failure_pressure),
        "mode_shift": mode_shift,
        "avoid_family_keys": [key for key in avoid_family_keys if key],
        "prefer_family_keys": [key for key in prefer_family_keys if key],
        "recommended_actions": recommended_actions,
        "repair_loop": repair_loop,
        "summary_md": (
            f"Recent frontier repeated family={top_family_key or 'none'} "
            f"{top_family_repeats} time(s) with blocker={dominant_blocker or 'none'} "
            f"{dominant_blocker_repeats} time(s)."
        ),
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
    policy_layers = lima_db.list_policy_layers(problem_id, limit=20)
    runtime_policy = resolve_runtime_policy(policy_layers, run_label="GUIDED_DEBUG", problem_id=problem_id)

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
    stagnation_controller = compute_stagnation_controller(
        runs=runs,
        families=families,
        fractures=fractures,
        obligations=obligations,
    )
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
            "stagnation_controller": stagnation_controller,
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
        "stagnation_controller": stagnation_controller,
        "family_search_controls": applied_controls[:12],
        "runtime_policy_indicators": runtime_policy.get("indicators", {}),
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
        "runtime_policy": runtime_policy,
        "family_search_controls": applied_controls,
        "stagnation_controller": stagnation_controller,
    }
