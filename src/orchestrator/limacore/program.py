from __future__ import annotations

from .artifacts import utc_now
from .db import LimaCoreDB
from .models import ProgramState


def get_program_state(db: LimaCoreDB, problem_id: str) -> ProgramState:
    return db.get_program_state(problem_id)


def write_candidate_program_delta(db: LimaCoreDB, problem_id: str, *, note: str) -> ProgramState:
    current = db.get_program_state(problem_id)
    return ProgramState(
        version=current.version + 1,
        worldsmith_policy_md=current.worldsmith_policy_md,
        retrieval_policy_md=current.retrieval_policy_md,
        compiler_policy_md=current.compiler_policy_md,
        frontier_policy_md=current.frontier_policy_md,
        acceptance_policy_md=current.acceptance_policy_md + f"\n- {note}",
        updated_at=utc_now(),
    )


def evaluate_program_delta(db: LimaCoreDB, problem_id: str, candidate: ProgramState, *, window: int = 6) -> bool:
    events = db.list_events(problem_id, limit=window)
    if not events:
        return False
    verified_yield = sum(1 for event in events if (event.get("score_delta") or {}).get("replayable_gain", 0) > 0)
    debt_reduction = sum(max(0, -int((event.get("score_delta") or {}).get("proof_debt_delta", 0))) for event in events)
    fracture_use = sum(1 for event in events if (event.get("score_delta") or {}).get("fracture_gain", 0) > 0)
    return (verified_yield + debt_reduction + fracture_use) > max(1, len(events) // 3)


def maybe_accept_program_delta(db: LimaCoreDB, problem_id: str, candidate: ProgramState, *, window: int = 6) -> bool:
    if not evaluate_program_delta(db, problem_id, candidate, window=window):
        return False
    db.set_program_state(problem_id, candidate)
    return True
