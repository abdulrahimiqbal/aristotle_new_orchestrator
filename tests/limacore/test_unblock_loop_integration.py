from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.aristotle import LocalAristotleBackend
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import LimaCoreLoop
from orchestrator.limacore.models import DeltaProposal
from orchestrator.limacore.unblock_manager import UnblockCandidate, UnblockSuggestion


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


def test_loop_uses_unblock_manager_when_blocked(tmp_path: Path, monkeypatch) -> None:
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

    chosen = _delta(family_key="hidden_state", title="unblock-chosen")
    suggestion = UnblockSuggestion(
        reason_md="Use hidden_state to unblock",
        strategy_kind="neighbor_family",
        current_family="quotient",
        current_frontier_node="target_theorem",
        suggested_family="hidden_state",
        blocked_node_key="target_theorem",
        candidates=(UnblockCandidate("neighbor_family", "family changed", chosen, 1.0),),
        chosen_index=0,
    )

    monkeypatch.setattr(loop.unblock_manager, "should_activate", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(loop.unblock_manager, "suggest", lambda **_kwargs: suggestion)
    monkeypatch.setattr(
        loop.proposer,
        "propose_delta",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("fallback proposer should not run")),
    )

    loop.run_iteration("collatz")
    events = db.list_events(str(problem["id"]), limit=30)
    assert any(event["event_type"] == "unblock_plan_selected" for event in events)
    assert any(event["event_type"] == "delta_proposed" and "unblock-chosen" in event["summary_md"] for event in events)


def test_loop_falls_back_when_unblock_has_no_valid_choice(tmp_path: Path, monkeypatch) -> None:
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

    empty = UnblockSuggestion(
        reason_md="No valid unblock candidate",
        strategy_kind="none",
        current_family="quotient",
        current_frontier_node="target_theorem",
        suggested_family="",
        blocked_node_key="target_theorem",
        candidates=(),
        chosen_index=-1,
    )
    fallback = _delta(family_key="operator_world", title="fallback-proposer")

    monkeypatch.setattr(loop.unblock_manager, "should_activate", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(loop.unblock_manager, "suggest", lambda **_kwargs: empty)
    monkeypatch.setattr(loop.proposer, "propose_delta", lambda *_args, **_kwargs: fallback)

    loop.run_iteration("collatz")
    events = db.list_events(str(problem["id"]), limit=30)
    assert any(event["event_type"] == "delta_proposed" and "fallback-proposer" in event["summary_md"] for event in events)
