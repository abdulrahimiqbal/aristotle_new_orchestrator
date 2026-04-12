from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.aristotle import LocalAristotleBackend
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import LimaCoreLoop
from orchestrator.limacore.manager_core import ManagerCandidate, ManagerPlan, ManagerProgramPatch
from orchestrator.limacore.models import DeltaProposal


def _delta(title: str, family: str = "hidden_state") -> DeltaProposal:
    return DeltaProposal(
        delta_type="lemma_delta",
        title=title,
        summary_md=title,
        family_key=family,
        target_node_key="target_theorem",
        edits={
            "bridge_claim": "bounded bridge",
            "local_law": "bounded local law",
            "kill_test": "bounded kill",
            "theorem_skeleton": "bounded skeleton",
            "obligations": ["o1", "o2"],
        },
    )


class _ModeManager:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def plan(self, manager_input):  # type: ignore[no-untyped-def]
        mode = str(manager_input.mode)
        self.calls.append(mode)
        candidate = ManagerCandidate(
            strategy_kind="neighbor_family",
            material_difference_md="different family",
            delta=_delta(f"{mode}-delta", family="hidden_state"),
        )
        return ManagerPlan(
            mode=mode,
            reason_md=f"mode {mode}",
            strategy_kind="neighbor_family",
            current_line={
                "family_key": manager_input.control_snapshot.current_family_key,
                "frontier_node_key": manager_input.control_snapshot.current_line_node_key,
                "blocker_kind": manager_input.control_snapshot.blocker_kind,
                "blocker_summary": manager_input.control_snapshot.blocker_summary,
            },
            candidates=(candidate,),
            chosen_index=0,
            confidence=0.6,
            expected_frontier_change="move frontier",
            provider="deterministic",
        )


def test_bootstrap_and_iteration_use_manager_and_persist_events(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    manager = _ModeManager()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend(), manager_core=manager)  # type: ignore[arg-type]
    result = loop.create_problem_from_prompt("Test theorem: every object has bounded drift.")
    assert result["problem_slug"]
    created = db.get_problem(result["problem_slug"])
    assert created is not None
    events = db.list_events(str(created["id"]), limit=200)
    assert any(event["event_type"] == "manager_tick" for event in events)
    assert any(event["event_type"] == "manager_plan_selected" for event in events)
    manager_tick = next(event for event in reversed(events) if event["event_type"] == "manager_tick")
    refs = manager_tick.get("artifact_refs") or []
    assert refs
    artifact = db.get_artifact(refs[0])
    assert artifact is not None
    assert artifact["artifact_kind"] == "manager_plan"
    assert "bootstrap" in manager.calls


def test_iteration_mode_selection_invokes_unblock_and_improve_program(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    manager = _ModeManager()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend(), manager_core=manager)  # type: ignore[arg-type]
    problem = db.get_problem("collatz")
    assert problem is not None
    pid = str(problem["id"])
    db.update_problem_runtime(
        pid,
        runtime_status="blocked",
        status_reason_md="Blocked for manager mode test",
        blocked_node_key="target_theorem",
        blocker_kind="missing_bridge_lemma",
    )
    loop.run_iteration(pid)
    assert "unblock" in manager.calls or "repair" in manager.calls

    for _ in range(8):
        db.append_event(pid, "frontier_improved", "accepted", summary_md="synthetic", score_delta={"replayable_gain": 0})
    monkeypatch.setattr("orchestrator.limacore.loop.select_manager_mode", lambda *_args, **_kwargs: "improve_program")
    db.update_problem_runtime(pid, runtime_status="running", status_reason_md="Running for improve test")
    loop.run_iteration(pid)
    assert "improve_program" in manager.calls


def test_manager_failure_emits_failed_tick_and_falls_back_to_proposer(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    problem = db.get_problem("collatz")
    assert problem is not None

    class _BrokenManager:
        def plan(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
            raise RuntimeError("manager boom")

    loop.manager_core = _BrokenManager()  # type: ignore[assignment]
    fallback = _delta("fallback-delta", family="operator_world")
    monkeypatch.setattr(loop.proposer, "propose_delta", lambda *_args, **_kwargs: fallback)
    loop.run_iteration(str(problem["id"]))
    events = db.list_events(str(problem["id"]), limit=40)
    assert any(event["event_type"] == "manager_tick_failed" for event in events)
    assert any(event["event_type"] == "delta_proposed" and "fallback-delta" in event["summary_md"] for event in events)


def test_improve_program_mode_emits_program_patch_decision_events(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    problem = db.get_problem("collatz")
    assert problem is not None
    pid = str(problem["id"])
    for _ in range(8):
        db.append_event(pid, "frontier_improved", "accepted", summary_md="synthetic", score_delta={"replayable_gain": 0})

    class _ProgramPatchManager:
        def plan(self, manager_input):  # type: ignore[no-untyped-def]
            return ManagerPlan(
                mode="improve_program",
                reason_md="policy tweak",
                strategy_kind="program_patch",
                current_line={
                    "family_key": manager_input.control_snapshot.current_family_key,
                    "frontier_node_key": manager_input.control_snapshot.current_line_node_key,
                    "blocker_kind": manager_input.control_snapshot.blocker_kind,
                    "blocker_summary": manager_input.control_snapshot.blocker_summary,
                },
                candidates=(),
                chosen_index=-1,
                confidence=0.4,
                expected_frontier_change="nudge policy",
                program_patch=ManagerProgramPatch(reason_md="policy tweak", patch={"rotation_bias": "increase"}),
                provider="deterministic",
            )

    loop.manager_core = _ProgramPatchManager()  # type: ignore[assignment]
    loop.run_iteration(pid)
    events = db.list_events(pid, limit=60)
    assert any(event["event_type"] == "manager_program_patch_proposed" for event in events)
    assert any(event["event_type"] in {"manager_program_patch_kept", "manager_program_patch_reverted"} for event in events)
