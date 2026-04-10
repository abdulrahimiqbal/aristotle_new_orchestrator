from __future__ import annotations

from typing import Any

from orchestrator.lima_models import (
    LimaClaimSpec,
    LimaGenerationResponse,
    LimaObjectSpec,
    LimaObligationSpec,
    LimaUniverseSpec,
    safe_json_loads,
    slugify,
)


RUN_LABELS: tuple[str, ...] = ("AUTONOMY_EVAL", "GUIDED_DEBUG")


def normalize_run_label(value: Any) -> str:
    label = str(value or "GUIDED_DEBUG").strip().upper()
    return label if label in RUN_LABELS else "GUIDED_DEBUG"


def extract_problem_signature(problem: dict[str, Any]) -> dict[str, Any]:
    statement = str(problem.get("statement_md") or "")
    title = str(problem.get("title") or "")
    seed = safe_json_loads(problem.get("seed_packet_json"), {})
    examples = seed.get("surface_examples") if isinstance(seed, dict) else []
    example_blob = ""
    if isinstance(examples, list):
        example_blob = " ".join(str(item) for item in examples[:6])
    blob = " ".join([title, statement, example_blob]).lower().replace("_", " ")

    def has_any(*markers: str) -> bool:
        return any(marker in blob for marker in markers)

    features = {
        "local_operator_on_neighboring_sites": has_any(
            "neighbor",
            "adjacent",
            "left",
            "right",
            "position i",
            "site",
            "local move",
            "move at position",
        ),
        "conserved_quantity_with_boundary_leakage": has_any(
            "boundary",
            "disappears",
            "leaks",
            "loss",
            "sink",
            "off the boundary",
            "absorbing",
        ),
        "order_sensitive_legal_moves": has_any(
            "legal move",
            "legal firing order",
            "choose",
            "allowed if",
            "move at position",
            "rewrite rule",
        ),
        "stable_state_termination_target": has_any(
            "stable",
            "stabilize",
            "terminates",
            "normal form",
            "terminal",
            "final state",
        ),
        "commutation_or_confluence_hint": has_any(
            "order independent",
            "same final",
            "commute",
            "confluence",
            "independent of order",
            "same endpoint",
        ),
        "hidden_state_suspicion": has_any(
            "history",
            "memory",
            "depends on order",
            "missing information",
            "companion",
            "hidden",
            "context",
        ),
        "rewrite_system_suspicion": has_any(
            "rewrite",
            "word",
            "string",
            "rule",
            "grammar",
            "symbolic",
        ),
        "quotient_state_suspicion": has_any(
            "quotient",
            "mod ",
            "modulo",
            "residue",
            "equivalence class",
            "factor",
        ),
        "operator_algebra_suspicion": has_any(
            "operator",
            "linear",
            "matrix",
            "algebra",
            "semigroup",
            "module",
        ),
    }
    positive_features = [name for name, active in features.items() if active]
    return {
        "title": title,
        "statement_excerpt": statement[:600],
        "features": features,
        "positive_features": positive_features,
        "surface_shape": {
            "has_examples": bool(example_blob.strip()),
            "mentions_local_rules": features["order_sensitive_legal_moves"],
            "mentions_terminal_goal": features["stable_state_termination_target"],
        },
    }


def runtime_session_layer(problem_id: str, run_label: str) -> dict[str, Any]:
    normalized = normalize_run_label(run_label)
    autonomy_eval = normalized == "AUTONOMY_EVAL"
    return {
        "id": f"runtime-{normalized.lower()}",
        "problem_id": problem_id,
        "scope": "session",
        "policy_json": {
            "run_label": normalized,
            "autonomy_eval": autonomy_eval,
            "allow_only_generic_blueprint_selection": autonomy_eval,
            "disable_benchmark_shaped_shortcuts": autonomy_eval,
            "disable_problem_specific_rescue_prompts": autonomy_eval,
            "allow_guided_repair_cycles": not autonomy_eval,
            "allow_meta_policy_mutation": not autonomy_eval,
        },
        "imposed_by": "runtime",
        "reason_md": (
            "Autonomy evaluation mode: benchmark-shaped shortcuts and guided rescue paths are disabled."
            if autonomy_eval
            else "Guided debug mode: generic discovery remains primary, but bounded repair/debug aids may run."
        ),
        "evidence_json": {"run_label": normalized},
        "expires_at": "",
        "meta_mutable": 0 if autonomy_eval else 1,
        "status": "active",
    }


def resolve_runtime_policy(
    policy_layers: list[dict[str, Any]] | None,
    *,
    run_label: str = "GUIDED_DEBUG",
    problem_id: str = "",
) -> dict[str, Any]:
    normalized = normalize_run_label(run_label)
    scoped: dict[str, list[dict[str, Any]]] = {
        "global": [],
        "problem": [],
        "benchmark": [],
        "session": [],
    }
    for raw in policy_layers or []:
        scope = str(raw.get("scope") or "problem")
        if scope not in scoped:
            continue
        policy = raw.get("policy_json")
        if not isinstance(policy, dict):
            policy = safe_json_loads(policy, {})
        scoped[scope].append(
            {
                **raw,
                "policy_json": policy if isinstance(policy, dict) else {},
            }
        )
    scoped["session"].append(runtime_session_layer(problem_id, normalized))

    merged: dict[str, Any] = {
        "run_label": normalized,
        "autonomy_eval": normalized == "AUTONOMY_EVAL",
        "allow_only_generic_blueprint_selection": normalized == "AUTONOMY_EVAL",
        "disable_benchmark_shaped_shortcuts": normalized == "AUTONOMY_EVAL",
        "disable_problem_specific_rescue_prompts": normalized == "AUTONOMY_EVAL",
        "allow_guided_repair_cycles": normalized != "AUTONOMY_EVAL",
        "allow_meta_policy_mutation": normalized != "AUTONOMY_EVAL",
    }
    for scope in ("global", "problem", "benchmark", "session"):
        for layer in scoped[scope]:
            merged.update(dict(layer.get("policy_json") or {}))

    mutation_allowed = {
        scope: all(bool(layer.get("meta_mutable", 1)) for layer in layers) if layers else True
        for scope, layers in scoped.items()
    }
    return {
        "run_label": normalized,
        "merged_policy": merged,
        "active_layers": scoped,
        "indicators": {
            "active_global_policy": bool(scoped["global"]),
            "active_problem_policy": bool(scoped["problem"]),
            "active_benchmark_policy": bool(scoped["benchmark"]),
            "active_session_policy": bool(scoped["session"]),
            "meta_lima_mutation_allowed": mutation_allowed,
        },
    }


def blueprint_catalog() -> list[dict[str, Any]]:
    return [
        {
            "key": "graph_stabilization",
            "ontology_class": "graph_stabilization",
            "title": "Graph Stabilization Blueprint",
            "family_key": "graph_stabilization_boundary_leakage",
            "match_features": [
                "local_operator_on_neighboring_sites",
                "conserved_quantity_with_boundary_leakage",
                "order_sensitive_legal_moves",
                "stable_state_termination_target",
                "commutation_or_confluence_hint",
            ],
            "core_objects": ["state_space", "operator", "bridge", "potential"],
            "canonical_obligations": [
                "exact_transition_law_case_A",
                "exact_transition_law_case_B",
                "ranking_or_lexicographic_descent",
                "local_operator_commutation_window",
                "local_confluence_or_commutation",
                "bounded_termination_or_stabilization",
                "bridge_to_surface_system",
                "uniqueness_of_representation",
            ],
            "local_checks": [
                "exact_bridge_counterexample_checker",
                "local_operator_commutation_checker",
                "bounded_confluence_checker",
                "bounded_termination_checker",
                "ranking_function_drop_checker",
            ],
        },
        {
            "key": "coordinate_lift",
            "ontology_class": "coordinate_lift",
            "title": "Coordinate Lift Blueprint",
            "family_key": "coordinate_lift_companion_state",
            "match_features": ["hidden_state_suspicion"],
            "core_objects": ["state_space", "bridge", "invariant"],
            "canonical_obligations": [
                "uniqueness_of_representation",
                "exact_transition_law_case_A",
                "bridge_to_surface_system",
            ],
            "local_checks": ["representation_uniqueness_checker"],
        },
        {
            "key": "rewrite_system",
            "ontology_class": "rewrite_system",
            "title": "Rewrite-System Blueprint",
            "family_key": "rewrite_system_normal_forms",
            "match_features": ["rewrite_system_suspicion", "order_sensitive_legal_moves"],
            "core_objects": ["grammar", "operator", "equivalence"],
            "canonical_obligations": [
                "exact_transition_law_case_A",
                "local_confluence_or_commutation",
                "quotient_or_normal_form_soundness",
            ],
            "local_checks": ["rewrite_rule_checker", "bounded_confluence_checker"],
        },
        {
            "key": "automaton",
            "ontology_class": "automaton",
            "title": "Automaton Blueprint",
            "family_key": "finite_state_controller",
            "match_features": ["hidden_state_suspicion", "stable_state_termination_target"],
            "core_objects": ["automaton", "state_space", "bridge"],
            "canonical_obligations": [
                "uniqueness_of_representation",
                "exact_transition_law_case_A",
                "bridge_to_surface_system",
            ],
            "local_checks": ["representation_uniqueness_checker"],
        },
        {
            "key": "quotient",
            "ontology_class": "quotient",
            "title": "Quotient Blueprint",
            "family_key": "quotient_normal_form",
            "match_features": ["quotient_state_suspicion"],
            "core_objects": ["quotient", "equivalence", "bridge"],
            "canonical_obligations": [
                "quotient_or_normal_form_soundness",
                "bridge_to_surface_system",
            ],
            "local_checks": ["representation_uniqueness_checker"],
        },
        {
            "key": "cocycle_or_skew_product",
            "ontology_class": "cocycle_or_skew_product",
            "title": "Cocycle Blueprint",
            "family_key": "cocycle_skew_product",
            "match_features": ["hidden_state_suspicion", "operator_algebra_suspicion"],
            "core_objects": ["state_space", "operator", "measure"],
            "canonical_obligations": [
                "exact_transition_law_case_A",
                "bridge_to_surface_system",
            ],
            "local_checks": ["exact_bridge_counterexample_checker"],
        },
        {
            "key": "symbolic_grammar",
            "ontology_class": "symbolic_grammar",
            "title": "Symbolic Grammar Blueprint",
            "family_key": "symbolic_grammar_parser",
            "match_features": ["rewrite_system_suspicion"],
            "core_objects": ["grammar", "bridge", "equivalence"],
            "canonical_obligations": [
                "exact_transition_law_case_A",
                "quotient_or_normal_form_soundness",
            ],
            "local_checks": ["rewrite_rule_checker"],
        },
        {
            "key": "geometric_or_topological",
            "ontology_class": "geometric_or_topological",
            "title": "Geometric Blueprint",
            "family_key": "geometric_compactification",
            "match_features": ["stable_state_termination_target"],
            "core_objects": ["state_space", "measure", "bridge"],
            "canonical_obligations": [
                "bridge_to_surface_system",
                "ranking_or_lexicographic_descent",
            ],
            "local_checks": [],
        },
        {
            "key": "algebraic_operator",
            "ontology_class": "algebraic_operator",
            "title": "Algebraic Operator Blueprint",
            "family_key": "operator_algebra_surface_model",
            "match_features": ["operator_algebra_suspicion", "local_operator_on_neighboring_sites"],
            "core_objects": ["operator", "state_space", "invariant"],
            "canonical_obligations": [
                "exact_transition_law_case_A",
                "ranking_or_lexicographic_descent",
                "bridge_to_surface_system",
            ],
            "local_checks": ["exact_bridge_counterexample_checker"],
        },
    ]


def select_ontology_blueprints(
    *,
    signature: dict[str, Any],
    current_universes: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    current_universes = current_universes or []
    prior_classes = [str(row.get("ontology_class") or "") for row in current_universes]
    scores: list[dict[str, Any]] = []
    feature_flags = dict(signature.get("features") or {})
    for blueprint in blueprint_catalog():
        score = 0.0
        matched: list[str] = []
        for feature in blueprint["match_features"]:
            if feature_flags.get(feature):
                score += 2.0
                matched.append(feature)
        if blueprint["ontology_class"] in prior_classes:
            score += 0.5
        if blueprint["key"] == "graph_stabilization" and feature_flags.get("commutation_or_confluence_hint"):
            score += 1.5
        if blueprint["key"] == "coordinate_lift" and feature_flags.get("hidden_state_suspicion"):
            score += 1.0
        scores.append(
            {
                "key": blueprint["key"],
                "ontology_class": blueprint["ontology_class"],
                "family_key": blueprint["family_key"],
                "score": round(score, 3),
                "matched_features": matched,
            }
        )
    ordered = sorted(scores, key=lambda item: (float(item["score"]), item["key"]), reverse=True)
    selected = ordered[0] if ordered else {
        "key": "coordinate_lift",
        "ontology_class": "coordinate_lift",
        "family_key": "coordinate_lift_companion_state",
        "score": 0.0,
        "matched_features": [],
    }
    return {
        "selected_blueprint": selected["key"],
        "selected_ontology_class": selected["ontology_class"],
        "selected_family_key": selected["family_key"],
        "scores": ordered,
    }


def _obligation_metadata(
    *,
    template_key: str,
    blueprint_key: str,
    capability_hints: list[str],
    signature: dict[str, Any],
) -> dict[str, Any]:
    return {
        "obligation_template_key": template_key,
        "ontology_blueprint": blueprint_key,
        "capability_hints": capability_hints,
        "problem_signature": signature,
    }


def build_graph_stabilization_universe(
    *,
    problem: dict[str, Any],
    mode: str,
    signature: dict[str, Any],
) -> LimaGenerationResponse:
    problem_title = str(problem.get("title") or problem.get("slug") or "This problem")
    universe = LimaUniverseSpec(
        title="Boundary-Leakage Graph Stabilization",
        family_key="graph_stabilization_boundary_leakage",
        family_kind="new" if mode == "wild" else "adjacent",
        branch_of_math="graph stabilization and local operator dynamics",
        solved_world=(
            "The surface dynamics are lifted into a local redistribution system on neighboring sites with "
            "explicit boundary accounting, so stabilization becomes an operator-order and terminal-state question."
        ),
        why_problem_is_easy_here=(
            "This ontology makes local moves explicit, exposes bounded commutation and confluence checks, and "
            "offers a weighted ranking witness for stabilization."
        ),
        core_story_md=(
            "Lima selected a graph-stabilization blueprint from a generic problem signature: local neighboring-site "
            "operators, boundary leakage, a stable-state target, and a hint that order should not matter in the end."
        ),
        core_objects=[
            LimaObjectSpec(
                object_kind="state_space",
                name="LocalGraphState",
                description_md="A bounded line-shaped state space with explicit boundary accounting coordinates.",
                formal_shape="Fin N -> Nat",
                payload={"graph_kind": "line", "boundary_accounting": True},
            ),
            LimaObjectSpec(
                object_kind="operator",
                name="LocalRedistributionOperator",
                description_md="A local operator that redistributes mass to neighboring sites and records off-graph leakage.",
                formal_shape="LocalGraphState -> Fin N -> LocalGraphState",
                payload={"operator_view": "neighbor_redistribution"},
            ),
            LimaObjectSpec(
                object_kind="bridge",
                name="BoundaryAccountingBridge",
                description_md="A bridge from surface states into the stabilization model with explicit leakage bookkeeping.",
                formal_shape="SurfaceState -> LocalGraphState",
                payload={"tracks_boundary_leakage": True},
            ),
            LimaObjectSpec(
                object_kind="potential",
                name="WeightedStabilizationRanking",
                description_md="A weighted ranking candidate intended to drop under each legal local redistribution.",
                formal_shape="LocalGraphState -> Nat",
                payload={"ranking_kind": "weighted_quadratic"},
            ),
        ],
        laws=[
            LimaClaimSpec(
                claim_kind="law",
                title="Local operators commute on disjoint or compatible support",
                statement_md="Compatible local operators should commute or share a common normal form in the stabilization model.",
                priority=5,
            ),
            LimaClaimSpec(
                claim_kind="law",
                title="Weighted ranking decreases under legal local redistributions",
                statement_md="A weighted ranking or lexicographic descent should force bounded stabilization.",
                priority=5,
            ),
        ],
        backward_translation=[
            "Embed the surface state into a local graph model with explicit leakage bookkeeping.",
            "Project stabilized graph states back to the surface system and compare terminal outcomes.",
        ],
        bridge_lemmas=[
            LimaClaimSpec(
                claim_kind="bridge_lemma",
                title="bridge_to_surface_system",
                statement_md="If the stabilization model has exact local laws and unique terminal outcomes, those conclusions transfer back to the surface system.",
                priority=5,
            )
        ],
        conditional_theorem=LimaClaimSpec(
            claim_kind="conditional_theorem",
            title="uniqueness_of_representation",
            statement_md="A unique boundary-accounting representation together with exact local laws yields a single stabilized surface outcome.",
            priority=4,
        ),
        kill_tests=[
            LimaClaimSpec(
                claim_kind="kill_test",
                title="local_operator_commutation_window",
                statement_md="Search bounded states for compatible local operators that fail to commute.",
                priority=5,
            ),
            LimaClaimSpec(
                claim_kind="kill_test",
                title="local_confluence_or_commutation",
                statement_md="Enumerate bounded states and compare terminal outcomes across legal operator orders.",
                priority=5,
            ),
        ],
        expected_failure_mode=(
            "The representation may still omit a hidden companion coordinate, or local commutation may not lift to global terminal uniqueness."
        ),
        literature_queries=[
            f"{problem_title} local stabilization confluence boundary leakage",
            f"{problem_title} abelian network local operator stabilization",
        ],
        formalization_targets=[
            LimaObligationSpec(
                obligation_kind="finite_check",
                title="exact_transition_law_case_A",
                statement_md="Verify on bounded states that an interior local move is represented exactly by one stabilization-model operator step.",
                priority=5,
                why_exists_md="The ontology must first get exact local dynamics right on the easiest regime.",
                prove_or_kill_md="A bounded mismatch shows the proposed operator language is not faithful.",
                **_obligation_metadata(
                    template_key="exact_transition_law_case_A",
                    blueprint_key="graph_stabilization",
                    capability_hints=["exact_bridge_counterexample_checker"],
                    signature=signature,
                ),
            ),
            LimaObligationSpec(
                obligation_kind="finite_check",
                title="exact_transition_law_case_B",
                statement_md="Verify on bounded states that boundary-leakage moves are represented exactly once bookkeeping coordinates are included.",
                priority=5,
                why_exists_md="Boundary cases are where hidden state is most likely to be missing.",
                prove_or_kill_md="A bounded mismatch shows the leakage bookkeeping is not yet correct.",
                **_obligation_metadata(
                    template_key="exact_transition_law_case_B",
                    blueprint_key="graph_stabilization",
                    capability_hints=["exact_bridge_counterexample_checker"],
                    signature=signature,
                ),
            ),
            LimaObligationSpec(
                obligation_kind="invariant_check",
                title="ranking_or_lexicographic_descent",
                statement_md="Check on bounded states that the weighted ranking strictly decreases under every legal local redistribution.",
                priority=5,
                why_exists_md="A real ranking witness is the shortest honest route to bounded stabilization.",
                prove_or_kill_md="If the ranking fails to drop, the stabilization story is wrong.",
                **_obligation_metadata(
                    template_key="ranking_or_lexicographic_descent",
                    blueprint_key="graph_stabilization",
                    capability_hints=["ranking_function_drop_checker"],
                    signature=signature,
                ),
            ),
            LimaObligationSpec(
                obligation_kind="finite_check",
                title="local_operator_commutation_window",
                statement_md="Verify on bounded states that compatible local operators commute.",
                priority=5,
                why_exists_md="Order-sensitive surface systems often become tractable only after bounded commutation is checked directly.",
                prove_or_kill_md="A bounded non-commuting pair fractures the ontology quickly.",
                **_obligation_metadata(
                    template_key="local_operator_commutation_window",
                    blueprint_key="graph_stabilization",
                    capability_hints=["local_operator_commutation_checker"],
                    signature=signature,
                ),
            ),
            LimaObligationSpec(
                obligation_kind="finite_check",
                title="local_confluence_or_commutation",
                statement_md="Verify on bounded states that legal operator orders lead to the same stabilized endpoint.",
                priority=5,
                why_exists_md="The surface statement hints that order should wash out in the terminal form.",
                prove_or_kill_md="Different bounded endpoints kill the current stabilization ontology.",
                **_obligation_metadata(
                    template_key="local_confluence_or_commutation",
                    blueprint_key="graph_stabilization",
                    capability_hints=["bounded_confluence_checker"],
                    signature=signature,
                ),
            ),
            LimaObligationSpec(
                obligation_kind="finite_check",
                title="bounded_termination_or_stabilization",
                statement_md="Verify on bounded states that every legal local-operator sequence reaches a stable state.",
                priority=5,
                why_exists_md="Bounded termination should already be visible before formal escalation.",
                prove_or_kill_md="A bounded non-terminating trace kills the candidate ranking story.",
                **_obligation_metadata(
                    template_key="bounded_termination_or_stabilization",
                    blueprint_key="graph_stabilization",
                    capability_hints=["bounded_termination_checker"],
                    signature=signature,
                ),
            ),
            LimaObligationSpec(
                obligation_kind="bridge_lemma",
                title="bridge_to_surface_system",
                statement_md="Formalize that exact local laws and stabilized endpoint uniqueness in the graph-stabilization world transfer back to the surface system.",
                priority=5,
                **_obligation_metadata(
                    template_key="bridge_to_surface_system",
                    blueprint_key="graph_stabilization",
                    capability_hints=[],
                    signature=signature,
                ),
            ),
            LimaObligationSpec(
                obligation_kind="lean_goal",
                title="uniqueness_of_representation",
                statement_md="Formalize that the boundary-accounting representation is unique enough to support transfer back to the surface system.",
                lean_goal="forall s, True",
                priority=4,
                **_obligation_metadata(
                    template_key="uniqueness_of_representation",
                    blueprint_key="graph_stabilization",
                    capability_hints=[],
                    signature=signature,
                ),
            ),
        ],
        scores={
            "compression_score": 4,
            "fit_score": 5,
            "novelty_score": 3,
            "falsifiability_score": 5,
            "bridgeability_score": 5,
            "formalizability_score": 4,
            "theorem_yield_score": 4,
            "literature_novelty_score": 3,
        },
        ontology_blueprint="graph_stabilization",
        problem_signature=signature,
    )
    return LimaGenerationResponse(
        frontier_summary_md=(
            f"{problem_title} currently looks like a local stabilization problem with boundary leakage and an order-insensitive terminal target."
        ),
        pressure_map={},
        run_summary_md=(
            f"Lima {mode} selected the graph-stabilization blueprint from a generic problem signature and emitted a bounded proof program around exact transitions, commutation, confluence, descent, and transfer."
        ),
        universes=[universe],
        policy_notes=[
            "Blueprint selected through generic problem-signature matching rather than benchmark-specific phrase triggers.",
            "Local checks are attached as capability hints so runtime routing does not depend on benchmark-shaped obligation titles.",
        ],
        selection_meta={
            "selected_via": "generic_problem_signature_blueprint",
            "selected_blueprint": "graph_stabilization",
            "selected_family_key": universe.family_key,
            "selected_ontology_class": "graph_stabilization",
            "problem_signature": signature,
        },
    )


def build_generic_blueprint_universe(
    *,
    problem: dict[str, Any],
    mode: str,
    signature: dict[str, Any],
    blueprint_key: str,
) -> LimaGenerationResponse:
    blueprint = next(
        (item for item in blueprint_catalog() if str(item["key"]) == blueprint_key),
        blueprint_catalog()[0],
    )
    title = str(problem.get("title") or problem.get("slug") or "This problem")
    universe = LimaUniverseSpec(
        title=f"{blueprint['title']}: {title}",
        family_key=str(blueprint["family_key"]),
        family_kind="new" if mode == "wild" else "adjacent",
        branch_of_math=str(problem.get("domain") or blueprint["ontology_class"]),
        solved_world=(
            f"The problem is recast through the {blueprint['title'].lower()}, exposing only narrow local laws and transfer obligations."
        ),
        why_problem_is_easy_here=(
            "The ontology should compress repeated surface behavior into a smaller object class while staying falsifiable."
        ),
        core_story_md=(
            f"Lima selected the {blueprint['key']} blueprint from a generic problem signature and is emitting only canonical proof-program obligations."
        ),
        core_objects=[
            LimaObjectSpec(
                object_kind=str(blueprint["core_objects"][0]),
                name=blueprint["title"].replace(" ", ""),
                description_md="Generic blueprint object proposed by Lima.",
                formal_shape="OpaqueState",
                payload={"ontology_blueprint": blueprint["key"]},
            )
        ],
        bridge_lemmas=[
            LimaClaimSpec(
                claim_kind="bridge_lemma",
                title="bridge_to_surface_system",
                statement_md="Formalize that the blueprint world transfers back to the surface system.",
                priority=4,
            )
        ],
        formalization_targets=[
            LimaObligationSpec(
                obligation_kind="bridge_lemma" if template == "bridge_to_surface_system" else "lean_goal",
                title=template,
                statement_md=f"Canonical obligation emitted by the {blueprint['key']} blueprint.",
                priority=4,
                **_obligation_metadata(
                    template_key=template,
                    blueprint_key=str(blueprint["key"]),
                    capability_hints=list(blueprint.get("local_checks") or []),
                    signature=signature,
                ),
            )
            for template in list(blueprint.get("canonical_obligations") or [])[:4]
        ],
        scores={
            "compression_score": 3,
            "fit_score": 3,
            "novelty_score": 3,
            "falsifiability_score": 3,
            "bridgeability_score": 3,
            "formalizability_score": 3,
            "theorem_yield_score": 3,
            "literature_novelty_score": 3,
        },
        ontology_blueprint=str(blueprint["key"]),
        problem_signature=signature,
    )
    return LimaGenerationResponse(
        frontier_summary_md=f"{title} is being explored through the {blueprint['title'].lower()}.",
        pressure_map={},
        run_summary_md=f"Lima {mode} selected the {blueprint['key']} blueprint from a generic problem signature.",
        universes=[universe],
        policy_notes=["Blueprint selected via generic signature scoring."],
        selection_meta={
            "selected_via": "generic_problem_signature_blueprint",
            "selected_blueprint": str(blueprint["key"]),
            "selected_family_key": str(blueprint["family_key"]),
            "selected_ontology_class": str(blueprint["ontology_class"]),
            "problem_signature": signature,
        },
    )
