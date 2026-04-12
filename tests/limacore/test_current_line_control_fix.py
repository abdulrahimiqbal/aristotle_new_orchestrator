from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.control import ControlSnapshot, build_control_snapshot
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.models import DeltaProposal, FrontierNode, ProblemSpec, ReductionPacket
from orchestrator.limacore.proposer import Proposer
from orchestrator.limacore.runtime import detect_runtime_status
from orchestrator.limacore.scorer import score_results


def _snapshot(
    *,
    family_key: str,
    node_key: str,
    exhausted: bool = True,
    replayable_gain: int = 0,
    proof_debt_delta: int = 0,
    accepts: int = 0,
    reverts: int = 0,
    yielded_lemmas: int = 0,
    failed_jobs: int = 0,
    failed_cohorts: int = 0,
    repeated_count: int = 3,
    repeated_pattern: bool = True,
    suggested_family_key: str = "hidden_state",
) -> ControlSnapshot:
    return ControlSnapshot(
        problem_id="problem",
        problem_slug="collatz",
        current_family_key=family_key,
        blocked_node_key=node_key,
        blocker_kind="skeleton_incomplete",
        blocker_summary="Need to complete quotient line proof structure.",
        current_required_delta_md="",
        current_theorem_skeleton_md="",
        exhausted_family_key=family_key if exhausted else "",
        exhausted_family_since="2026-04-12T00:00:00+00:00" if exhausted else "",
        exhausted_reason="",
        suggested_family_key=suggested_family_key,
        recent_replayable_gain=replayable_gain,
        recent_proof_debt_delta=proof_debt_delta,
        recent_fracture_gain=0,
        recent_reverts=reverts,
        yielded_lemmas=yielded_lemmas,
        failed_jobs=failed_jobs,
        running_jobs=0,
        queued_jobs=0,
        succeeded_jobs=0,
        total_jobs=failed_jobs,
        failed_cohorts=failed_cohorts,
        current_family_failed_cohorts=failed_cohorts,
        current_family_failed_jobs=failed_jobs,
        current_family_total_jobs=failed_jobs,
        live_family_count=1,
        active_alternative_families=(),
        same_blocker_persists=True,
        same_family_persists=True,
        current_family_exhausted=exhausted,
        recent_current_family_yielded_lemmas=yielded_lemmas,
        recent_current_family_replayable_gain=replayable_gain,
        recent_current_family_failed_jobs=failed_jobs,
        recent_current_family_failed_cohorts=failed_cohorts,
        recent_current_family_total_jobs=failed_jobs,
        recent_current_family_accepts=accepts,
        recent_current_family_reverts=reverts,
        recent_current_family_counterexamples=0,
        recent_current_family_last_gain_at="2026-04-12T00:00:00+00:00" if replayable_gain > 0 else "",
        repeated_cohort_pattern_detected=repeated_pattern,
        repeated_cohort_signature="quotient|quotient_closure|agenda|0|4|0" if repeated_pattern else "",
        recent_accept_count=accepts,
        recent_revert_count=reverts,
        current_line_replayable_gain_rate=float(replayable_gain),
        window_size=10,
        current_line_node_key=node_key,
        current_line_key=f"{family_key}:{node_key}",
        recent_current_family_proof_debt_delta=proof_debt_delta,
        recent_current_family_repeated_signature_count=repeated_count,
        recent_current_line_yielded_lemmas=yielded_lemmas,
        recent_current_line_replayable_gain=replayable_gain,
        recent_current_line_proof_debt_delta=proof_debt_delta,
        recent_current_line_failed_jobs=failed_jobs,
        recent_current_line_failed_cohorts=failed_cohorts,
        recent_current_line_total_jobs=failed_jobs,
        recent_current_line_accepts=accepts,
        recent_current_line_reverts=reverts,
        recent_current_line_counterexamples=0,
        recent_current_line_last_gain_at="2026-04-12T00:00:00+00:00" if replayable_gain > 0 else "",
        recent_current_line_repeated_signature_count=repeated_count,
        current_line_exhausted=exhausted,
    )


def test_historical_success_does_not_prevent_current_line_exhaustion(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    assert problem is not None
    problem_id = str(problem["id"])

    db.replace_world_head(
        problem_id,
        {
            "family_key": "quotient",
            "world_name": "Odd-step quotient probe",
            "status": "surviving",
            "bridge_status": "open",
            "kill_status": "pressured",
            "theorem_status": "open",
            "yield_score": 0.9,
            "updated_at": "2026-04-12T00:00:00+00:00",
        },
    )
    db.replace_world_head(
        problem_id,
        {
            "family_key": "hidden_state",
            "world_name": "Parity carry ledger",
            "status": "surviving",
            "bridge_status": "open",
            "kill_status": "open",
            "theorem_status": "open",
            "yield_score": 0.2,
            "updated_at": "2026-04-11T23:00:00+00:00",
        },
    )
    db.upsert_frontier_node(
        FrontierNode(
            id="frontier-1",
            problem_id=problem_id,
            node_key="quotient_closure",
            node_kind="theorem_skeleton",
            title="Quotient closure",
            status="blocked",
            blocker_kind="skeleton_incomplete",
            blocker_note_md="Need to complete quotient line proof structure.",
            priority=12.0,
            updated_at="2026-04-12T00:00:00+00:00",
        )
    )
    db.update_problem_runtime(
        problem_id,
        runtime_status="blocked",
        status_reason_md="Need to complete quotient line proof structure.",
        blocked_node_key="quotient_closure",
        blocker_kind="skeleton_incomplete",
    )

    quotient_cohort = db.create_cohort(
        problem_id,
        world_id=f"{problem_id}:quotient",
        cohort_kind="agenda_fanout",
        title="quotient maintenance",
        total_jobs=4,
    )
    for idx in range(4):
        job_id = db.create_job(
            problem_id,
            cohort_id=quotient_cohort,
            frontier_node_key="quotient_closure",
            job_kind="local_law",
            input_artifact_ref={"idx": idx},
        )
        db.set_job_status(job_id, status="failed", result_summary_md="blocked")
    db.update_cohort_metrics(quotient_cohort)

    hidden_cohort = db.create_cohort(
        problem_id,
        world_id=f"{problem_id}:hidden_state",
        cohort_kind="agenda_fanout",
        title="hidden_state proof",
        total_jobs=2,
    )
    for idx in range(2):
        job_id = db.create_job(
            problem_id,
            cohort_id=hidden_cohort,
            frontier_node_key="carry_ledger",
            job_kind="bridge_lemma",
            input_artifact_ref={"idx": idx},
        )
        db.set_job_status(job_id, status="succeeded", result_summary_md="lemma", replayable=True)
    db.update_cohort_metrics(hidden_cohort)

    snapshot = build_control_snapshot(db, problem_id)
    assert snapshot.current_family_key == "quotient"
    assert snapshot.current_line_exhausted is True
    assert snapshot.recent_current_line_replayable_gain == 0
    assert snapshot.recent_current_line_accepts == 0


def test_proposer_rotates_away_from_exhausted_quotient_and_preserves_hidden_state(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = Proposer(db)
    problem = db.get_problem("collatz")
    assert problem is not None
    spec = ProblemSpec(**problem)
    gap = {"node_key": "quotient_closure", "title": "Quotient closure"}

    db.replace_world_head(
        str(problem["id"]),
        {
            "family_key": "hidden_state",
            "world_name": "Parity carry ledger",
            "status": "surviving",
            "bridge_status": "open",
            "kill_status": "open",
            "theorem_status": "open",
            "yield_score": 0.8,
            "updated_at": "2026-04-12T00:00:00+00:00",
        },
    )

    monkeypatch.setattr(
        "orchestrator.limacore.proposer.build_control_snapshot",
        lambda _db, _problem_id: _snapshot(family_key="hidden_state", node_key="carry_ledger", exhausted=True, replayable_gain=0, proof_debt_delta=0),
    )
    result = loop.propose_delta(spec, gap)
    assert result.delta_type == "lemma_delta"
    assert result.family_key == "hidden_state"

    monkeypatch.setattr(
        "orchestrator.limacore.proposer.build_control_snapshot",
        lambda _db, _problem_id: _snapshot(family_key="quotient", node_key="quotient_closure", exhausted=True, replayable_gain=0, proof_debt_delta=0),
    )
    result = loop.propose_delta(spec, gap)
    assert result.delta_type != "kill_delta"
    assert result.family_key != "quotient"


def test_repeated_zero_gain_maintenance_is_rejected_and_gain_is_accepted(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    assert problem is not None
    spec = ProblemSpec(**problem)
    reduction = ReductionPacket(
        selected_gap="quotient_closure",
        bridge_claim="bridge",
        local_law="law",
        kill_test="test",
        theorem_skeleton="skeleton",
        obligations=[],
        cohort_plan=[],
        rationale_md="rationale",
    )
    delta = DeltaProposal(
        delta_type="lemma_delta",
        title="maintenance",
        summary_md="maintenance",
        family_key="quotient",
        target_node_key="quotient_closure",
        edits={
            "required_delta_md": "",
            "theorem_skeleton": "",
        },
    )

    monkeypatch.setattr("orchestrator.limacore.scorer.build_control_snapshot", lambda _db, _problem_id: _snapshot(
        family_key="quotient",
        node_key="quotient_closure",
        exhausted=True,
        replayable_gain=0,
        proof_debt_delta=0,
        accepts=0,
        reverts=0,
        failed_jobs=4,
        failed_cohorts=2,
        repeated_count=3,
    ))
    monkeypatch.setattr("orchestrator.limacore.scorer.proof_debt", lambda _frontier: 0)

    rejected = score_results(db, spec, delta, reduction, jobs=[])
    assert rejected.accepted is False

    gain_delta = DeltaProposal(
        delta_type="lemma_delta",
        title="gain",
        summary_md="gain",
        family_key="hidden_state",
        target_node_key="carry_ledger",
        edits={
            "required_delta_md": "materially different bridge",
            "theorem_skeleton": "materially different skeleton",
        },
    )
    monkeypatch.setattr("orchestrator.limacore.scorer.build_control_snapshot", lambda _db, _problem_id: _snapshot(
        family_key="hidden_state",
        node_key="carry_ledger",
        exhausted=False,
        replayable_gain=1,
        proof_debt_delta=-1,
        accepts=1,
        reverts=0,
        failed_jobs=0,
        failed_cohorts=0,
        repeated_pattern=False,
        repeated_count=1,
    ))
    monkeypatch.setattr("orchestrator.limacore.scorer.proof_debt", lambda _frontier: 1)
    accepted = score_results(
        db,
        spec,
        gain_delta,
        reduction,
        jobs=[{"status": "succeeded", "replayable": True, "job_kind": "bridge_lemma", "result_summary_md": "lemma"}],
    )
    assert accepted.accepted is True


def test_runtime_marks_stale_current_line_not_running(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    assert problem is not None
    problem_id = str(problem["id"])
    db.replace_world_head(
        problem_id,
        {
            "family_key": "quotient",
            "world_name": "Odd-step quotient probe",
            "status": "surviving",
            "bridge_status": "open",
            "kill_status": "pressured",
            "theorem_status": "open",
            "yield_score": 0.9,
            "updated_at": "2026-04-12T00:00:00+00:00",
        },
    )
    db.update_problem_runtime(problem_id, runtime_status="running", status_reason_md="Running: autopilot active.")

    monkeypatch.setattr(
        "orchestrator.limacore.runtime.build_control_snapshot",
        lambda _db, _problem_id, window=10: _snapshot(
            family_key="quotient",
            node_key="quotient_closure",
            exhausted=False,
            replayable_gain=0,
            proof_debt_delta=0,
            accepts=0,
            reverts=0,
            failed_jobs=4,
            failed_cohorts=2,
            repeated_count=3,
        ),
    )
    view = detect_runtime_status(db, problem_id)
    assert view.status == "stalled"


def test_runtime_marks_exhausted_current_line_blocked(tmp_path: Path, monkeypatch) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    assert problem is not None
    problem_id = str(problem["id"])
    db.upsert_frontier_node(
        FrontierNode(
            id="node-1",
            problem_id=problem_id,
            node_key="quotient_closure",
            node_kind="theorem_skeleton",
            title="Quotient closure",
            status="blocked",
            blocker_kind="skeleton_incomplete",
            blocker_note_md="Need to complete quotient line proof structure.",
            priority=10.0,
            updated_at="2026-04-12T00:00:00+00:00",
        )
    )
    db.update_problem_runtime(problem_id, runtime_status="running", status_reason_md="Running: autopilot active.", blocked_node_key="quotient_closure", blocker_kind="skeleton_incomplete")

    monkeypatch.setattr(
        "orchestrator.limacore.runtime.build_control_snapshot",
        lambda _db, _problem_id, window=10: _snapshot(
            family_key="quotient",
            node_key="quotient_closure",
            exhausted=True,
            replayable_gain=0,
            proof_debt_delta=0,
            accepts=0,
            reverts=0,
            failed_jobs=4,
            failed_cohorts=2,
            repeated_count=3,
        ),
    )
    view = detect_runtime_status(db, problem_id)
    assert view.status == "blocked"
