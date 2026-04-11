from __future__ import annotations

from .db import LimaCoreDB
from .control import (
    build_control_snapshot,
    is_actionable_fracture,
    is_duplicate_churn,
    maintenance_churn_penalty,
    materially_changed_required_delta,
    materially_changed_theorem_skeleton,
)
from .frontier import proof_debt
from .models import DeltaProposal, ProblemSpec, ReductionPacket, ScoreDelta


JOB_TO_NODE_KEY = {
    "bridge_lemma": "bridge_claim",
    "local_law": "local_energy_law",
    "theorem_skeleton_probe": "terminal_form_uniqueness",
    "replay_check": "replay_closure",
}


def score_results(
    db: LimaCoreDB,
    problem: ProblemSpec,
    delta: DeltaProposal,
    reduction: ReductionPacket,
    jobs: list[dict],
) -> ScoreDelta:
    snapshot = build_control_snapshot(db, problem.id)
    frontier_before = db.get_frontier_nodes(problem.id)
    proved_before = {
        str(node.get("node_key") or "")
        for node in frontier_before
        if str(node.get("status") or "") == "proved"
    }
    replayable_gain = 0
    credited_nodes: set[str] = set()
    for job in jobs:
        if not job["replayable"]:
            continue
        node_key = JOB_TO_NODE_KEY.get(str(job.get("job_kind") or ""))
        if not node_key:
            replayable_gain += 1
            continue
        if node_key in proved_before or node_key in credited_nodes:
            continue
        credited_nodes.add(node_key)
        replayable_gain += 1
    proof_debt_delta = -min(replayable_gain, max(0, proof_debt(frontier_before)))
    fracture_gain = sum(
        1
        for job in jobs
        if str(job.get("status") or "") == "failed"
        and (
            "counterexample" in str(job.get("result_summary_md") or "").lower()
            or "blocked" in str(job.get("result_summary_md") or "").lower()
        )
    )
    worlds = db.list_world_heads(problem.id)
    duplication_penalty = 0.0
    stale_penalty = 0.0
    if any(str(world["family_key"]) == delta.family_key for world in worlds):
        duplication_penalty += 0.6
    fractures = db.list_fracture_heads(problem.id)
    if any(str(row["family_key"]) == delta.family_key and int(row["repeat_count"]) >= 2 for row in fractures):
        stale_penalty += 0.5
    required_delta_md = str(delta.edits.get("required_delta_md") or "")
    theorem_skeleton_md = str(reduction.theorem_skeleton or "")
    
    # Check for material changes
    required_delta_changed = materially_changed_required_delta(
        snapshot.current_required_delta_md, required_delta_md
    )
    theorem_skeleton_changed = materially_changed_theorem_skeleton(
        snapshot.current_theorem_skeleton_md, theorem_skeleton_md
    )
    
    actionable_fracture = is_actionable_fracture(
        snapshot,
        family_key=delta.family_key,
        blocked_node_key=snapshot.blocked_node_key,
        blocker_kind=snapshot.blocker_kind,
        required_delta_md=required_delta_md,
        theorem_skeleton_md=theorem_skeleton_md,
        next_cohort_plan=str(delta.edits.get("next_cohort_plan") or ""),
    )
    duplicate_churn = is_duplicate_churn(
        snapshot,
        family_key=delta.family_key,
        blocked_node_key=snapshot.blocked_node_key,
        blocker_kind=snapshot.blocker_kind,
        required_delta_md=required_delta_md,
        theorem_skeleton_md=theorem_skeleton_md,
        replayable_gain=replayable_gain,
        proof_debt_delta=proof_debt_delta,
        yielded_lemmas=snapshot.recent_current_family_yielded_lemmas,  # FIXED: Use recent family metrics
    )
    
    # NEW: Calculate maintenance churn penalty
    churn_penalty = maintenance_churn_penalty(snapshot)
    
    # STRONGER rejection: repeated maintenance cohorts with same outcomes
    repeated_maintenance_pattern = (
        snapshot.repeated_cohort_pattern_detected
        and replayable_gain == 0
        and proof_debt_delta == 0
        and not required_delta_changed
        and not theorem_skeleton_changed
    )
    
    narrative_only = (
        replayable_gain == 0
        and proof_debt_delta == 0
        and fracture_gain == 0
        and not required_delta_md.strip()
        and not theorem_skeleton_md.strip()
    )
    
    novelty = max(0.0, (delta.world_packet.confidence_prior if delta.world_packet else 0.4) - duplication_penalty - churn_penalty)
    
    # World rotation only accepted if it materially changes trajectory
    world_rotation = (
        delta.delta_type == "world_delta"
        and delta.world_packet is not None
        and not duplicate_churn
        and (required_delta_changed or theorem_skeleton_changed or not snapshot.same_family_persists)
    )
    
    # STRONGER acceptance criteria:
    # 1. Replayable gain > 0 (real structure)
    # 2. Proof debt reduced (frontier advanced)
    # 3. Material world rotation (different family/trajectory)
    # 4. Actionable fracture with low penalties
    # REJECTED: repeated maintenance churn, narrative-only, duplicate patterns
    accepted = (
        not narrative_only
        and not duplicate_churn
        and not repeated_maintenance_pattern
        and (
            replayable_gain > 0
            or proof_debt_delta < 0
            or world_rotation
            or (fracture_gain > 0 and actionable_fracture and (duplication_penalty + stale_penalty + churn_penalty) < 0.8)
        )
    )
    
    # Build comprehensive summary
    summary_parts = [
        f"accepted={accepted}",
        f"replayable_gain={replayable_gain}",
        f"proof_debt_delta={proof_debt_delta}",
        f"fracture_gain={fracture_gain}",
        f"churn_penalty={churn_penalty:.2f}",
    ]
    if snapshot.repeated_cohort_pattern_detected:
        summary_parts.append(f"pattern={snapshot.repeated_cohort_signature}")
    if repeated_maintenance_pattern:
        summary_parts.append("repeated_maintenance_rejected")
    
    summary = "; ".join(summary_parts)
    
    return ScoreDelta(
        accepted=accepted,
        replayable_gain=replayable_gain,
        proof_debt_delta=proof_debt_delta,
        fracture_gain=fracture_gain,
        novelty_signal=novelty,
        duplication_penalty=duplication_penalty + stale_penalty + churn_penalty,
        summary_md=summary,
    )
