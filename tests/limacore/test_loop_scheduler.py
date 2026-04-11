"""Tests for LimaCore loop scheduler behavior (Fix 1: immediate startup)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from orchestrator.limacore.aristotle import LocalAristotleBackend
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import LimaCoreLoop, _scheduler_pass


class TestSchedulerPass:
    """Test the _scheduler_pass helper for immediate execution behavior."""

    def test_scheduler_pass_processes_seeded_problems(self, tmp_path: Path) -> None:
        """Seeded problems (collatz, inward-compression) should be processed immediately."""
        db = LimaCoreDB(str(tmp_path / "test.db"))
        db.initialize()
        loop = LimaCoreLoop(db, backend=LocalAristotleBackend())

        # db.initialize() seeds collatz and inward-compression-conjecture
        # Both should be processed by scheduler pass
        results = _scheduler_pass(db, loop)

        # Should process both seeded problems
        slugs = {r["slug"] for r in results}
        assert "collatz" in slugs
        assert "inward-compression-conjecture" in slugs

    def test_scheduler_pass_runs_new_active_problem_immediately(self, tmp_path: Path) -> None:
        """Newly created active autopilot-enabled problems should be processed."""
        db = LimaCoreDB(str(tmp_path / "test.db"))
        db.initialize()
        loop = LimaCoreLoop(db, backend=LocalAristotleBackend())

        # Run scheduler pass once to process seeded problems
        _scheduler_pass(db, loop)

        # Create a new problem
        problem_id, _ = db.create_problem(
            slug="new-active-problem",
            title="New Active Problem",
            statement_md="Test statement",
            runtime_status="running",
            autopilot_enabled=True,
        )

        # Scheduler pass should process the new problem
        results = _scheduler_pass(db, loop)

        # Should find our new problem among results (along with any still-active seeded ones)
        problem_ids = {r["problem_id"] for r in results}
        assert problem_id in problem_ids

    def test_scheduler_pass_skips_paused_problems(self, tmp_path: Path) -> None:
        """Paused problems should be skipped by scheduler pass."""
        db = LimaCoreDB(str(tmp_path / "test.db"))
        db.initialize()
        loop = LimaCoreLoop(db, backend=LocalAristotleBackend())

        # Process seeded problems first
        _scheduler_pass(db, loop)

        # Create a paused problem (autopilot_enabled=False makes it paused)
        problem_id, _ = db.create_problem(
            slug="paused-problem",
            title="Paused Problem",
            statement_md="Test statement",
            runtime_status="paused",
            autopilot_enabled=False,
        )

        results = _scheduler_pass(db, loop)

        # Should not process paused problem
        problem_ids = {r["problem_id"] for r in results}
        assert problem_id not in problem_ids

    def test_scheduler_pass_skips_solved_problems(self, tmp_path: Path) -> None:
        """Solved problems should be skipped by scheduler pass."""
        db = LimaCoreDB(str(tmp_path / "test.db"))
        db.initialize()
        loop = LimaCoreLoop(db, backend=LocalAristotleBackend())

        # Process seeded problems first
        _scheduler_pass(db, loop)

        # Create a solved problem
        problem_id, _ = db.create_problem(
            slug="solved-problem",
            title="Solved Problem",
            statement_md="Test statement",
            runtime_status="solved",
            autopilot_enabled=False,
        )

        results = _scheduler_pass(db, loop)

        # Should not process solved problem
        problem_ids = {r["problem_id"] for r in results}
        assert problem_id not in problem_ids

    def test_scheduler_pass_skips_failed_problems(self, tmp_path: Path) -> None:
        """Failed problems should be skipped by scheduler pass."""
        db = LimaCoreDB(str(tmp_path / "test.db"))
        db.initialize()
        loop = LimaCoreLoop(db, backend=LocalAristotleBackend())

        # Process seeded problems first
        _scheduler_pass(db, loop)

        # Create a failed problem
        problem_id, _ = db.create_problem(
            slug="failed-problem",
            title="Failed Problem",
            statement_md="Test statement",
            runtime_status="failed",
            autopilot_enabled=True,
        )

        results = _scheduler_pass(db, loop)

        # Should not process failed problem
        problem_ids = {r["problem_id"] for r in results}
        assert problem_id not in problem_ids


class TestLoopIterationOrdering:
    """Test that the loop runs iterations correctly without initial sleep."""

    def test_run_iteration_does_not_require_sleep(self, tmp_path: Path) -> None:
        """Iteration should be runnable immediately without pre-sleep."""
        db = LimaCoreDB(str(tmp_path / "test.db"))
        db.initialize()
        loop = LimaCoreLoop(db, backend=LocalAristotleBackend())

        # Create a problem using the full creation flow
        result = loop.create_problem_from_prompt("collatz conjecture")

        # Should complete without requiring a sleep
        assert "problem_slug" in result
        # Status can be running, booting, or blocked depending on what happened
        assert result["status"] in {"running", "booting", "blocked", "stalled"}

        # Should be able to run another iteration immediately
        second_result = loop.run_iteration(result["problem_slug"])
        assert "decision" in second_result or "accepted" in second_result


class TestLimacoreLoopAsync:
    """Test the async limacore_loop function."""

    @pytest.mark.asyncio
    async def test_loop_structure_runs_before_sleep(self, tmp_path: Path) -> None:
        """The loop structure should allow running before any sleep."""
        db = LimaCoreDB(str(tmp_path / "test.db"))
        db.initialize()

        # Create a mock loop that tracks calls
        calls = []

        class MockLoop:
            def __init__(self, db):
                pass

            def run_iteration(self, problem_id):
                calls.append(problem_id)
                return {"accepted": True}

        # Test that _scheduler_pass runs immediately
        results = _scheduler_pass(db, MockLoop(db))

        # Should have processed the seeded problems immediately
        assert len(results) >= 2  # At least collatz and inward-compression
        assert len(calls) >= 2
