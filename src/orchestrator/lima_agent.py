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

from orchestrator import config as app_config
from orchestrator.db import Database
from orchestrator.lima_db import LimaDatabase
from orchestrator.lima_literature import (
    infer_literature_relation,
    refresh_literature,
    score_literature_novelty,
)
from orchestrator.lima_meta import analyze_and_update_policy
from orchestrator.lima_models import (
    LimaClaimSpec,
    LimaGenerationResponse,
    LimaMode,
    LimaObjectSpec,
    LimaObligationSpec,
    LimaUniverseSpec,
    coerce_lima_generation_response,
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
from orchestrator.llm import invoke_llm

logger = logging.getLogger("orchestrator.lima")

LIMA_SYSTEM = """You are Lima, a falsification-first conceptual research engine.

Role:
- Lima invents candidate mathematical universes, then breaks them.
- Lima compiles claim graphs and formalizable obligations.
- Lima cites and checks literature to avoid fake novelty.
- Lima remembers fractures and updates strategy policy over time.

Authority boundary:
- Lima has zero direct live execution authority.
- Do not enqueue experiments, targets, Aristotle jobs, or main queue work.
- Emit only bounded outputs: dead universe, weakened universe, interesting informal fragment, formalizable obligation, or handoff-worthy incubation.

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
  If a family is mutate/cooldown/retire, do not emit another member unless a core object,
  invariant, bridge lemma, falsifier, or literature tool changes materially.
- No live execution fields such as campaign_id, target_id, objective, new_experiment, or aristotle_job_id."""

_GLOBAL_LIMA_RUN_LOCK = False
_STRIP_JSON_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)


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
    family_search_constraints: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    seed = safe_json_loads(problem.get("seed_packet_json"), {})
    frontier = safe_json_loads(state.get("frontier_json"), {})
    tensions = [
        "Odd/even induced dynamics has strong local structure but no global descent bridge.",
        "Residue-class regularity can overfit finite data unless a quotient or invariant explains it.",
        "2-adic/completion languages are expressive but can become non-bridgeable to positive integers.",
    ]
    failed = [
        str(f.get("failure_type") or "") for f in fractures[:8] if f.get("failure_type")
    ]
    if failed:
        tensions.append("Recent fracture memory emphasizes: " + ", ".join(failed[:5]))
    search_constraints = []
    for row in family_search_constraints or []:
        required_delta = safe_json_loads(row.get("required_delta_json"), [])
        search_constraints.append(
            {
                "family_key": row.get("family_key"),
                "search_action": row.get("search_action"),
                "status": row.get("status"),
                "last_failure_type": row.get("last_failure_type"),
                "repeat_failure_count": row.get("repeat_failure_count"),
                "reason": row.get("search_reason_md"),
                "required_delta": required_delta if isinstance(required_delta, list) else [],
                "instruction": (
                    "Do not re-emit this family unless the next universe materially changes "
                    "a core object, invariant, bridge lemma, falsifier, or literature tool."
                ),
            }
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
        "failed_invariants": ["naive global height descent", *failed[:4]],
        "known_constraints": [
            "No live Aristotle or main experiment queue mutations without human approval.",
            "Formal obligations must stay narrow enough for Lean/Mathlib review.",
            "Literature prior-art risk must be recorded explicitly.",
        ],
        "frontier_compression_opportunities": [
            "Unify parity-vector, residue, and odd-subsystem facts through a quotient or completion.",
            "Turn failed invariants into boundary theorems rather than hiding the failure.",
            "Compile first bridges as finite/residue or one-step compatibility obligations.",
        ],
    }


def _family_constraint_action(pressure_map: dict[str, Any], family_key: str) -> str:
    for row in pressure_map.get("search_constraints") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("family_key") or "") == family_key:
            return str(row.get("search_action") or "")
    return ""


def _local_generation(
    *,
    problem: dict[str, Any],
    mode: LimaMode,
    pressure_map: dict[str, Any],
    literature_refresh: dict[str, Any],
) -> LimaGenerationResponse:
    problem_title = str(problem.get("title") or problem.get("slug") or "the problem")
    problem_slug = str(problem.get("slug") or "").lower()
    if "collatz" not in problem_slug and "collatz" not in problem_title.lower():
        generic_family = {
            "wild": "structural_completion_atlas",
            "stress": "counterexample_boundary_atlas",
            "forge": "minimal_bridge_obligation_atlas",
            "balanced": "minimal_bridge_obligation_atlas",
        }[mode]
        universe = LimaUniverseSpec(
            title=f"{problem_title} bridge-obligation atlas",
            family_key=generic_family,
            family_kind="new" if mode == "wild" else "adjacent",
            branch_of_math=str(problem.get("domain") or "general mathematics"),
            solved_world=(
                "A decomposition of the problem into local regimes where every "
                "candidate mechanism is paired with a falsifier and a narrow bridge."
            ),
            why_problem_is_easy_here=(
                "The conjecture becomes easier only if each regime has a precise "
                "translation back to the original statement and a small obstruction search."
            ),
            core_story_md=(
                f"Lima starts {problem_title} with a conservative atlas: define regimes, "
                "state bridge obligations, and kill any regime that lacks a counterexample boundary."
            ),
            core_objects=[
                LimaObjectSpec(
                    object_kind="state_space",
                    name="ProblemRegimeAtlas",
                    description_md="A problem-specific partition into regimes with explicit bridge obligations.",
                    formal_shape="Regime -> Proposition",
                    payload={"problem_slug": problem.get("slug")},
                ),
                LimaObjectSpec(
                    object_kind="bridge",
                    name="OriginalStatementBridge",
                    description_md="A translation layer from local regime claims back to the original conjecture.",
                    formal_shape="local_claim -> original_statement",
                    payload={},
                ),
            ],
            laws=[
                LimaClaimSpec(
                    claim_kind="law",
                    title="Every regime needs a falsifier",
                    statement_md="A regime is not useful unless it has a bounded obstruction search or a formal bridge target.",
                    priority=4,
                )
            ],
            backward_translation=[
                "Map each local regime claim back to the original conjecture statement.",
                "Reject regimes whose assumptions cannot be stated independently of the target conclusion.",
            ],
            bridge_lemmas=[
                LimaClaimSpec(
                    claim_kind="bridge_lemma",
                    title="Regime bridge implies original reduction",
                    statement_md="If every regime bridge is verified, the atlas reduces the original problem to local obligations.",
                    priority=5,
                )
            ],
            conditional_theorem=LimaClaimSpec(
                claim_kind="conditional_theorem",
                title="Atlas reduction theorem",
                statement_md="If the regime atlas covers the problem and all bridge obligations survive rupture, the original conjecture is reduced.",
                priority=5,
            ),
            kill_tests=[
                LimaClaimSpec(
                    claim_kind="kill_test",
                    title="Vacuity audit",
                    statement_md="Reject any regime that assumes the original conjecture or has no independent falsifier.",
                    priority=5,
                )
            ],
            expected_failure_mode="The atlas may be vacuous, non-covering, or too broad to formalize.",
            literature_queries=[
                f"{problem_title} survey",
                f"{problem_title} counterexample methods",
            ],
            formalization_targets=[
                LimaObligationSpec(
                    obligation_kind="equivalence",
                    title="State the first regime bridge",
                    statement_md="Write one regime-to-original-statement bridge as a formal obligation.",
                    priority=4,
                )
            ],
            scores={
                "compression_score": 3,
                "fit_score": 3,
                "novelty_score": 3,
                "falsifiability_score": 4,
                "bridgeability_score": 4,
                "formalizability_score": 3,
                "theorem_yield_score": 3,
                "literature_novelty_score": 3,
            },
        )
        return LimaGenerationResponse(
            frontier_summary_md=(
                f"{problem_title} is newly registered in Lima; frontier memory starts "
                "from the user statement, seed packet, literature routing, and fracture constraints."
            ),
            pressure_map=pressure_map,
            run_summary_md=(
                f"Lima {mode} run initialized {problem_title} with a conservative "
                "bridge-obligation atlas and deterministic rupture checks."
            ),
            universes=[universe],
            policy_notes=["Generic problem fallback used; no live authority granted."],
        )
    family_key = {
        "wild": "completion_boundary_sheaf",
        "stress": "residue_fracture_boundary",
        "forge": "odd_state_quotient_bridge",
        "balanced": "odd_state_quotient_bridge",
    }[mode]
    if _family_constraint_action(pressure_map, family_key) in {"mutate", "cooldown", "retire"}:
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
) -> dict[str, Any]:
    global _GLOBAL_LIMA_RUN_LOCK
    if _GLOBAL_LIMA_RUN_LOCK:
        return {"ok": False, "error": "lima_run_in_progress"}
    _GLOBAL_LIMA_RUN_LOCK = True
    selected_mode = _mode(mode)
    try:
        lima_db.initialize()
        problem = lima_db.get_problem(problem_slug)
        problem_id = str(problem["id"])
        sync_result = sync_lima_aristotle_results(lima_db, main_db, problem_id=problem_id)
        state = lima_db.get_state(problem_id)
        reference_points = _build_reference_points(main_db, problem)
        fractures = lima_db.list_fractures(problem_id, limit=24)
        families = lima_db.list_family_leaderboard(problem_id, limit=16)
        family_search_constraints = lima_db.list_family_search_constraints(problem_id, limit=12)
        pressure_map = build_pressure_map(
            problem,
            state,
            reference_points,
            fractures,
            family_search_constraints=family_search_constraints,
        )
        literature_refresh = refresh_literature(
            lima_db,
            problem=problem,
            pressure_map=pressure_map,
            universes=[],
        )
        literature_context = lima_db.list_literature_sources(
            problem_id, limit=int(app_config.LIMA_MAX_LITERATURE_RESULTS)
        )
        policy_revisions = lima_db.list_policy_revisions(problem_id, limit=6)
        raw_response: dict[str, Any] = {}
        raw_preview = ""
        json_warnings: list[str] = []
        if app_config.LLM_API_KEY:
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
            except Exception:
                logger.exception("Lima LLM call failed; falling back to deterministic local generation")
                json_warnings.append("llm_request_failed_local_fallback")
        else:
            json_warnings.append("llm_api_key_missing_local_fallback")

        if raw_response:
            generated, warnings = coerce_lima_generation_response(raw_response)
            json_warnings.extend(warnings)
        else:
            generated = _local_generation(
                problem=problem,
                mode=selected_mode,
                pressure_map=pressure_map,
                literature_refresh=literature_refresh,
            )
        universes = generated.universes[: int(app_config.LIMA_MAX_UNIVERSES_PER_RUN)]
        # Refresh again after universe-specific queries are known.
        universe_lit_refresh = refresh_literature(
            lima_db,
            problem=problem,
            pressure_map=pressure_map,
            universes=universes,
        )
        literature_context = lima_db.list_literature_sources(
            problem_id, limit=int(app_config.LIMA_MAX_LITERATURE_RESULTS)
        )
        rupture_reports = rupture_universes(universes, literature_context=literature_context)
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
        policy_snapshot = {
            "mode": selected_mode,
            "zero_live_authority": True,
            "json_warnings": json_warnings,
            "literature_refresh": literature_refresh,
            "universe_literature_refresh": universe_lit_refresh,
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
        run_id = lima_db.commit_run(
            problem_id=problem_id,
            trigger_kind=trigger_kind,
            mode=selected_mode,
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
        created_universes = lima_db.list_universes_for_run(run_id)
        sources = lima_db.list_literature_sources(problem_id, limit=6)
        for row, universe in zip(created_universes, universes):
            for source in sources[:2]:
                lit_score = score_literature_novelty(universe, source)
                lima_db.link_universe_literature(
                    universe_id=str(row["id"]),
                    source_id=str(source["id"]),
                    relation_kind=str(lit_score.get("relation_kind") or infer_literature_relation(universe, source)),
                    note=f"Linked by Lima literature routing. prior_art_score={lit_score.get('prior_art_score')}",
                )
        obligation_result = None
        if app_config.LIMA_AUTO_LOCAL_OBLIGATION_CHECKS:
            obligation_result = run_queued_obligation_checks(
                lima_db,
                problem_id=problem_id,
                limit=int(app_config.LIMA_MAX_OBLIGATIONS_PER_RUN),
            )
        formal_submit_result = await submit_promising_formal_obligations(
            lima_db,
            main_db,
            problem_id=problem_id,
            limit=int(app_config.LIMA_MAX_OBLIGATIONS_PER_RUN),
        )
        sync_after_submit = sync_lima_aristotle_results(lima_db, main_db, problem_id=problem_id)
        meta_result = None
        if app_config.LIMA_ENABLE_AUTO_POLICY_UPDATES:
            meta_result = analyze_and_update_policy(
                lima_db, problem_id=problem_id, from_run_id=run_id
            )
        return {
            "ok": True,
            "run_id": run_id,
            "problem_id": problem_id,
            "mode": selected_mode,
            "universe_count": len(universes),
            "fracture_count": sum(len(r.get("fractures") or []) for r in rupture_reports),
            "handoff_count": len(lima_db.list_handoffs(problem_id, status="pending", limit=100)),
            "literature_source_count": len(literature_context),
            "summary": generated.run_summary_md,
            "validation_warnings": json_warnings,
            "obligation_checks": obligation_result,
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
    if not app_config.LIMA_ENABLED:
        return
    while True:
        try:
            await run_lima(
                lima_db,
                main_db,
                problem_slug=app_config.LIMA_DEFAULT_PROBLEM,
                trigger_kind="scheduled",
                mode=app_config.LIMA_DEFAULT_MODE,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Lima loop tick failed")
        await asyncio.sleep(max(60, int(app_config.LIMA_LOOP_INTERVAL_SEC)))
