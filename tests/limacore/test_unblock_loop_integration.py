from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.aristotle import LocalAristotleBackend
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import LimaCoreLoop
from orchestrator.limacore.manager_core import ManagerCandidate, ManagerPlan
from orchestrator.limacore.models import DeltaProposal


def _delta(*, family_key: str, title: str) -> DeltaProposal:
    return DeltaProposal(
        delta_type="lemma_delta",
        title=title,
        summary_md=title,
        family_key=family_key,
        target_node_key="target_theorem",
        edits={
            "bridge_claim": "Bridge claim for unblock delta",
            "local_law": "Local law for unblock delta",
            "kill_test": "Kill test for unblock delta",
            "theorem_skeleton": "Theorem skeleton for unblock delta",
            "required_delta_md": "Materially different required delta",
            "obligations": [
                "formalize bridge",
                "prove local law",
                "run bounded kill test",
            ],
        },
    )


class _StubManager:
    def __init__(self, plan: ManagerPlan | None) -> None:
        self.plan_to_return = plan
        self.calls: list[str] = []

    def plan(self, manager_input):  # type: ignore[no-untyped-def]
        self.calls.append(str(manager_input.mode))
        return self.plan_to_return


def _plan_with_delta(delta: DeltaProposal, *, mode: str = "unblock") -> ManagerPlan:
    return ManagerPlan(
        mode=mode,
        reason_md="manager selected bounded unblock step",
        strategy_kind="neighbor_family",
        current_line={
            "family_key": "quotient",
            "frontier_node_key": "target_theorem",
            "blocker_kind": "missing_bridge_lemma",
            "blocker_summary": "need bridge",
        },
        candidates=(ManagerCandidate("neighbor_family", "materially different", delta),),
        chosen_index=0,
        confidence=0.71,
        expected_frontier_change="switch family and advance frontier",
        provider="deterministic",
    )


def test_loop_uses_manager_plan_when_blocked(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    problem = db.get_problem("collatz")
    assert problem is not None
    db.update_problem_runtime(
        str(problem["id"]),
        runtime_status="blocked",
        status_reason_md="Blocked for test",
        blocked_node_key="target_theorem",
        blocker_kind="missing_bridge_lemma",
    )

    chosen = _delta(family_key="hidden_state", title="manager-chosen")
    stub = _StubManager(_plan_with_delta(chosen))
    loop.manager_core = stub  # type: ignore[assignment]
    monkeypatch.setattr(
        loop.proposer,
        "propose_delta",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("fallback proposer should not run")),
    )

    loop.run_iteration("collatz")
    events = db.list_events(str(problem["id"]), limit=30)
    assert any(event["event_type"] == "manager_tick" for event in events)
    assert any(event["event_type"] == "manager_plan_selected" for event in events)
    assert any(event["event_type"] == "delta_proposed" and "manager-chosen" in event["summary_md"] for event in events)
    assert "unblock" in stub.calls


def test_loop_falls_back_when_manager_has_no_choice(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    problem = db.get_problem("collatz")
    assert problem is not None
    db.update_problem_runtime(
        str(problem["id"]),
        runtime_status="stalled",
        status_reason_md="Stalled for test",
        blocked_node_key="target_theorem",
        blocker_kind="missing_bridge_lemma",
    )

    empty = ManagerPlan(
        mode="unblock",
        reason_md="manager had no valid candidates",
        strategy_kind="frontier_shift",
        current_line={
            "family_key": "quotient",
            "frontier_node_key": "target_theorem",
            "blocker_kind": "missing_bridge_lemma",
            "blocker_summary": "missing bridge",
        },
        candidates=(),
        chosen_index=-1,
        confidence=0.2,
        expected_frontier_change="none",
        provider="deterministic",
    )
    fallback = _delta(family_key="operator_world", title="fallback-proposer")

    loop.manager_core = _StubManager(empty)  # type: ignore[assignment]
    monkeypatch.setattr(loop.proposer, "propose_delta", lambda *_args, **_kwargs: fallback)

    loop.run_iteration("collatz")
    events = db.list_events(str(problem["id"]), limit=30)
    assert any(event["event_type"] == "manager_tick" for event in events)
    assert any(event["event_type"] == "delta_proposed" and "fallback-proposer" in event["summary_md"] for event in events)
