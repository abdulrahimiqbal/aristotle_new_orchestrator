from __future__ import annotations

from .db import LimaCoreDB
from .control import build_control_snapshot, is_actionable_fracture, is_duplicate_churn
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
    narrative_only = (
        replayable_gain == 0
        and proof_debt_delta == 0
        and fracture_gain == 0
        and not required_delta_md.strip()
        and not theorem_skeleton_md.strip()
    )
    novelty = max(0.0, (delta.world_packet.confidence_prior if delta.world_packet else 0.4) - duplication_penalty)
    world_rotation = delta.delta_type == "world_delta" and delta.world_packet is not None and not duplicate_churn
    accepted = not narrative_only and not duplicate_churn and (
        replayable_gain > 0
        or proof_debt_delta < 0
        or world_rotation
        or (fracture_gain > 0 and actionable_fracture and (duplication_penalty + stale_penalty) < 1.2)
    )
    summary = (
        f"accepted={accepted}; replayable_gain={replayable_gain}; "
        f"proof_debt_delta={proof_debt_delta}; fracture_gain={fracture_gain}; "
        f"duplication_penalty={duplication_penalty + stale_penalty:.2f}"
    )
    return ScoreDelta(
        accepted=accepted,
        replayable_gain=replayable_gain,
        proof_debt_delta=proof_debt_delta,
        fracture_gain=fracture_gain,
        novelty_signal=novelty,
        duplication_penalty=duplication_penalty + stale_penalty,
        summary_md=summary,
    )
