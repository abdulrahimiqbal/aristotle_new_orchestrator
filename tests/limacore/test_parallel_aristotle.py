from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.aristotle import LocalAristotleBackend, submit_aristotle_jobs
from orchestrator.limacore.compiler import compile_delta_to_reduction
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.models import GroundingBundle, ProblemSpec
from orchestrator.limacore.worldsmith import Worldsmith


def test_parallel_cohorts_submit_and_aggregate(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem_row = db.get_problem("inward-compression-conjecture")
    assert problem_row is not None
    problem = ProblemSpec(**problem_row)
    gap = db.get_frontier_node(problem.id, "target_theorem")
    assert gap is not None
    proposal = Worldsmith().propose_world(problem, gap)
    _reduction, agenda = compile_delta_to_reduction(gap, proposal, GroundingBundle())
    cohort_ids, job_ids = submit_aristotle_jobs(db, problem, proposal, agenda, LocalAristotleBackend())
    assert cohort_ids
    assert len(job_ids) == 4
    cohort = db.get_cohort(cohort_ids[0])
    assert cohort is not None
    assert int(cohort["total_jobs"]) == 4
    assert int(cohort["yielded_lemmas"]) >= 1
