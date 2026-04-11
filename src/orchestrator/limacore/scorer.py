from __future__ import annotations

from .db import LimaCoreDB
from .frontier import proof_debt
from .models import DeltaProposal, ProblemSpec, ReductionPacket, ScoreDelta


def score_results(
    db: LimaCoreDB,
    problem: ProblemSpec,
    delta: DeltaProposal,
    reduction: ReductionPacket,
    jobs: list[dict],
) -> ScoreDelta:
    frontier_before = db.get_frontier_nodes(problem.id)
    narrative_claims = {
        reduction.bridge_claim.strip().lower(),
        reduction.local_law.strip().lower(),
        reduction.kill_test.strip().lower(),
        reduction.theorem_skeleton.strip().lower(),
    }
    replayable_gain = sum(1 for job in jobs if job["replayable"])
    proof_debt_delta = -min(replayable_gain, max(0, proof_debt(frontier_before)))
    fracture_gain = sum(1 for job in jobs if "counterexample" in str(job.get("result_summary_md") or "").lower() or "blocked" in str(job.get("result_summary_md") or "").lower())
    worlds = db.list_world_heads(problem.id)
    duplication_penalty = 0.0
    stale_penalty = 0.0
    if any(str(world["family_key"]) == delta.family_key for world in worlds):
        duplication_penalty += 0.6
    fractures = db.list_fracture_heads(problem.id)
    if any(str(row["family_key"]) == delta.family_key and int(row["repeat_count"]) >= 2 for row in fractures):
        stale_penalty += 0.5
    narrative_only = (
        (replayable_gain == 0 and proof_debt_delta == 0 and fracture_gain == 0)
        or (len(narrative_claims) == 1 and "narrative" in next(iter(narrative_claims)))
    )
    novelty = max(0.0, (delta.world_packet.confidence_prior if delta.world_packet else 0.4) - duplication_penalty)
    accepted = not narrative_only and (replayable_gain > 0 or proof_debt_delta < 0 or fracture_gain > 0) and (duplication_penalty + stale_penalty) < 1.2
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
