"""Tests for UI honesty about Aristotle execution (Fix 2)."""

from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.aristotle import LocalAristotleBackend
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import LimaCoreLoop
from orchestrator.limacore.presenter import _compute_cohort_summary, build_workspace_context


class TestCohortSummaryHonesty:
    """Tests that cohort summary honestly reflects inline execution."""

    def test_compute_cohort_summary_empty(self) -> None:
        """Empty cohorts should produce honest 'no activity' summary."""
        summary = _compute_cohort_summary([])

        assert summary["latest_cohort"] is None
        assert summary["has_recent_activity"] is False
        assert summary["latest_cohort_yield_summary"] == "No cohorts yet"
        assert summary.get("total_cohorts") == 0
        assert summary.get("finished_cohorts") == 0

    def test_compute_cohort_summary_with_yield(self) -> None:
        """Cohorts with lemmas should report yield."""
        cohorts = [
            {
                "id": "c1",
                "title": "Test Cohort",
                "status": "finished",
                "total_jobs": 4,
                "succeeded_jobs": 3,
                "failed_jobs": 1,
                "yielded_lemmas": 2,
                "yielded_counterexamples": 1,
                "created_at": "2024-01-01T00:00:00",
                "updated_at": "2024-01-01T00:01:00",
            }
        ]

        summary = _compute_cohort_summary(cohorts)

        assert summary["latest_cohort_title"] == "Test Cohort"
        assert summary["has_recent_activity"] is True
        assert summary["recent_job_yield"] == 2
        assert "2 lemma(s)" in summary["latest_cohort_yield_summary"]
        assert "1 counterexample(s)" in summary["latest_cohort_yield_summary"]

    def test_compute_cohort_summary_no_yield(self) -> None:
        """Cohorts with no yield should report failure honestly."""
        cohorts = [
            {
                "id": "c1",
                "title": "Failed Cohort",
                "status": "finished",
                "total_jobs": 4,
                "succeeded_jobs": 0,
                "failed_jobs": 4,
                "yielded_lemmas": 0,
                "yielded_counterexamples": 0,
                "created_at": "2024-01-01T00:00:00",
                "updated_at": "2024-01-01T00:01:00",
            }
        ]

        summary = _compute_cohort_summary(cohorts)

        assert "no yield" in summary["latest_cohort_yield_summary"].lower() or \
               "failed" in summary["latest_cohort_yield_summary"].lower()

    def test_compute_cohort_summary_recent_calculation(self) -> None:
        """Recent yield should aggregate last 3 cohorts."""
        cohorts = [
            {
                "id": "c1",
                "title": "Cohort 1",
                "status": "finished",
                "total_jobs": 4,
                "succeeded_jobs": 3,
                "failed_jobs": 1,
                "yielded_lemmas": 1,
                "yielded_counterexamples": 0,
                "created_at": "2024-01-01T00:00:00",
                "updated_at": "2024-01-01T00:01:00",
            },
            {
                "id": "c2",
                "title": "Cohort 2",
                "status": "finished",
                "total_jobs": 4,
                "succeeded_jobs": 3,
                "failed_jobs": 1,
                "yielded_lemmas": 2,
                "yielded_counterexamples": 1,
                "created_at": "2024-01-01T00:02:00",
                "updated_at": "2024-01-01T00:03:00",
            },
        ]

        summary = _compute_cohort_summary(cohorts)

        # Recent yield should sum both cohorts' lemmas (1 + 2 = 3)
        assert summary["recent_job_yield"] == 3


class TestWorkspaceContextHonestStats:
    """Tests that workspace context provides honest stats about execution."""

    def test_workspace_shows_honest_inline_execution(self, tmp_path: Path) -> None:
        """After iteration completes, active job counts should be 0 (inline execution)."""
        db = LimaCoreDB(str(tmp_path / "test.db"))
        db.initialize()
        loop = LimaCoreLoop(db, backend=LocalAristotleBackend())

        # Create and run a problem
        result = loop.create_problem_from_prompt("collatz conjecture")

        # Get workspace context
        ctx = build_workspace_context(db, result["problem_slug"])

        # Since jobs execute inline synchronously:
        # - After iteration, running_jobs should be 0
        # - After iteration, queued_jobs should be 0
        # - Historical succeeded_jobs should be > 0
        assert ctx["stats"]["running_jobs"] == 0, "Inline execution leaves no running jobs after completion"
        assert ctx["stats"]["queued_jobs"] == 0, "Inline execution leaves no queued jobs after completion"

        # But we should have cohort summary info
        assert "cohort_summary" in ctx["stats"]
        assert ctx["stats"]["has_recent_cohort_activity"] is True

    def test_workspace_distinguishes_active_vs_historical(self, tmp_path: Path) -> None:
        """Workspace should clearly distinguish active (0) vs historical throughput."""
        db = LimaCoreDB(str(tmp_path / "test.db"))
        db.initialize()
        loop = LimaCoreLoop(db, backend=LocalAristotleBackend())

        result = loop.create_problem_from_prompt("inward compression conjecture")
        ctx = build_workspace_context(db, result["problem_slug"])

        # Active jobs should be 0 for inline execution
        assert ctx["stats"]["active_jobs"] == 0

        # But we should see the cohort that was just executed
        assert ctx["stats"]["latest_cohort_title"] is not None
        assert ctx["stats"]["latest_cohort_yield_summary"] is not None

        # Total cohorts should be tracked
        assert ctx["stats"]["cohort_summary"]["total_cohorts"] > 0
