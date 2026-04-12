from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.control import build_control_snapshot
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.frontier import select_frontier_gap
from orchestrator.limacore.manager_core import ManagerCore, build_manager_input, default_program_payload
from orchestrator.limacore.models import DeltaProposal, ProblemSpec


def _manager_input(db: LimaCoreDB, problem_slug: str, *, mode: str = "explore"):  # type: ignore[no-untyped-def]
    row = db.get_problem(problem_slug)
    assert row is not None
    problem = ProblemSpec(**row)
    gap = select_frontier_gap(db, problem.id)
    snapshot = build_control_snapshot(db, problem.id)
    return build_manager_input(
        problem=problem,
        current_gap=gap,
        control_snapshot=snapshot,
        strongest_worlds=db.list_world_heads(problem.id),
        recent_fractures=db.list_fracture_heads(problem.id),
        recent_events=db.list_events(problem.id, limit=80),
        recent_cohorts=db.list_cohorts(problem.id),
        mode=mode,  # type: ignore[arg-type]
        runtime_status=str(row.get("runtime_status") or "running"),
        current_program=default_program_payload(problem.id, db=db),
    )


def test_manager_core_returns_valid_plan_for_manager_input(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    manager = ManagerCore()
    monkeypatch.setattr(manager, "_plan_with_llm", lambda *_args, **_kwargs: None)
    manager_input = _manager_input(db, "collatz", mode="explore")
    plan = manager.plan(manager_input)
    assert plan.mode == "explore"
    assert plan.candidates
    assert isinstance(plan.chosen_delta, DeltaProposal)
    assert plan.chosen_delta is not None
    assert plan.chosen_delta.target_node_key


def test_manager_core_invalid_llm_response_falls_back_safely(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    manager = ManagerCore()
    manager_input = _manager_input(db, "inward-compression-conjecture", mode="bootstrap")
    monkeypatch.setattr(manager, "_plan_with_llm", lambda *_args, **_kwargs: None)
    plan = manager.plan(manager_input)
    assert plan.provider == "deterministic"
    assert plan.candidates


def test_manager_plan_chosen_delta_maps_cleanly_to_delta_proposal(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    manager = ManagerCore()
    monkeypatch.setattr(manager, "_plan_with_llm", lambda *_args, **_kwargs: None)
    manager_input = _manager_input(db, "collatz", mode="unblock")
    plan = manager.plan(manager_input)
    chosen = plan.chosen_delta
    assert chosen is not None
    assert chosen.delta_type in {"world_delta", "reduction_delta", "lemma_delta", "kill_delta", "program_delta"}
    assert chosen.title
    assert chosen.summary_md
