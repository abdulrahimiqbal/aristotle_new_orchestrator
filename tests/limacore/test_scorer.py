from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.models import DeltaProposal, ProblemSpec, ReductionPacket
from orchestrator.limacore.scorer import score_results


def test_accepts_delta_when_replayable_gain_exists(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = ProblemSpec(**db.get_problem("collatz"))
    delta = DeltaProposal(delta_type="lemma_delta", title="x", summary_md="x", family_key="other")
    reduction = ReductionPacket("gap", "bridge", "law", "kill", "skel", ["o"], [], "why")
    jobs = [{"replayable": True, "job_kind": "bridge_lemma", "result_summary_md": "proved"}]
    score = score_results(db, problem, delta, reduction, jobs)
    assert score.accepted is True


def test_rejects_narrative_only_output(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = ProblemSpec(**db.get_problem("collatz"))
    delta = DeltaProposal(delta_type="lemma_delta", title="x", summary_md="x", family_key="other")
    reduction = ReductionPacket("gap", "bridge", "law", "kill", "skel", ["o"], [], "why")
    jobs = [{"replayable": False, "job_kind": "bridge_lemma", "result_summary_md": "narrative only"}]
    score = score_results(db, problem, delta, reduction, jobs)
    assert score.accepted is False


def test_stale_family_is_penalized(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    assert problem is not None
    db.replace_fracture_head(
        str(problem["id"]),
        {
            "family_key": "quotient",
            "failure_type": "stale_or_refuted",
            "repeat_count": 3,
        },
    )
    delta = DeltaProposal(delta_type="kill_delta", title="x", summary_md="x", family_key="quotient")
    reduction = ReductionPacket("gap", "bridge", "law", "kill", "skel", ["o"], [], "why")
    jobs = [{"replayable": False, "job_kind": "counterexample_search", "result_summary_md": "blocked"}]
    score = score_results(db, ProblemSpec(**problem), delta, reduction, jobs)
    assert score.duplication_penalty >= 0.5
