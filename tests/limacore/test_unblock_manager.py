from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.control import ControlSnapshot
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.models import ProblemSpec
from orchestrator.limacore.unblock_manager import UnblockManager


def _snapshot(
    *,
    problem_slug: str = "generic-problem",
    family_key: str = "quotient",
    node_key: str = "target_theorem",
    required_delta_md: str = "old required delta",
    theorem_skeleton_md: str = "old theorem skeleton",
    exhausted: bool = True,
) -> ControlSnapshot:
    return ControlSnapshot(
        problem_id="problem",
        problem_slug=problem_slug,
        current_family_key=family_key,
        blocked_node_key=node_key,
        blocker_kind="missing_bridge_lemma",
        blocker_summary="Need a materially different bridge claim.",
        current_required_delta_md=required_delta_md,
        current_theorem_skeleton_md=theorem_skeleton_md,
        exhausted_family_key=family_key if exhausted else "",
        exhausted_family_since="2026-04-12T00:00:00+00:00" if exhausted else "",
        exhausted_reason="",
        suggested_family_key="hidden_state",
        recent_replayable_gain=0,
        recent_proof_debt_delta=0,
        recent_fracture_gain=0,
        recent_reverts=2,
        yielded_lemmas=0,
        failed_jobs=4,
        running_jobs=0,
        queued_jobs=0,
        succeeded_jobs=0,
        total_jobs=4,
        failed_cohorts=2,
        current_family_failed_cohorts=2,
        current_family_failed_jobs=4,
        current_family_total_jobs=4,
        live_family_count=2,
        active_alternative_families=("hidden_state",),
        same_blocker_persists=True,
        same_family_persists=True,
        current_family_exhausted=exhausted,
        recent_current_family_yielded_lemmas=0,
        recent_current_family_replayable_gain=0,
        recent_current_family_failed_jobs=4,
        recent_current_family_failed_cohorts=2,
        recent_current_family_total_jobs=4,
        recent_current_family_accepts=0,
        recent_current_family_reverts=2,
        recent_current_family_counterexamples=0,
        recent_current_family_last_gain_at="",
        repeated_cohort_pattern_detected=True,
        repeated_cohort_signature=f"{family_key}|{node_key}|maintenance|0|4|0",
        recent_accept_count=0,
        recent_revert_count=2,
        current_line_replayable_gain_rate=0.0,
        window_size=10,
        current_line_node_key=node_key,
        current_line_key=f"{family_key}:{node_key}",
        recent_current_family_proof_debt_delta=0,
        recent_current_family_repeated_signature_count=3,
        recent_current_line_yielded_lemmas=0,
        recent_current_line_replayable_gain=0,
        recent_current_line_proof_debt_delta=0,
        recent_current_line_failed_jobs=4,
        recent_current_line_failed_cohorts=2,
        recent_current_line_total_jobs=4,
        recent_current_line_accepts=0,
        recent_current_line_reverts=2,
        recent_current_line_counterexamples=0,
        recent_current_line_last_gain_at="",
        recent_current_line_repeated_signature_count=3,
        current_line_exhausted=exhausted,
    )


def test_unblock_manager_generates_materially_different_successors() -> None:
    manager = UnblockManager()
    problem = ProblemSpec(
        id="problem",
        slug="generic-problem",
        title="Generic problem",
        statement_md="Show bounded descent.",
        runtime_status="blocked",
    )
    snapshot = _snapshot()
    suggestion = manager.suggest(
        problem=problem,
        gap={"node_key": "target_theorem", "title": "Target theorem"},
        control_snapshot=snapshot,
        strongest_worlds=[
            {"family_key": "hidden_state", "status": "surviving", "yield_score": 1.1},
            {"family_key": "operator_world", "status": "surviving", "yield_score": 0.9},
        ],
        recent_fractures=[
            {
                "family_key": "quotient",
                "required_delta_md": "new witness class for repaired quotient bridge",
                "repeat_count": 2,
                "ban_level": "soft",
            }
        ],
        recent_events=[{"family_key": "quotient"}],
    )

    assert suggestion.chosen_delta is not None
    assert len(suggestion.candidates) >= 2
    assert suggestion.strategy_kind in {"repair", "neighbor_family", "orthogonal_family"}
    assert suggestion.current_family == "quotient"
    assert any(c.strategy_kind == "neighbor_family" for c in suggestion.candidates)
    assert any(c.strategy_kind == "orthogonal_family" for c in suggestion.candidates)


def test_repair_candidate_requires_material_change(monkeypatch) -> None:
    manager = UnblockManager()
    problem = ProblemSpec(
        id="problem",
        slug="generic-problem",
        title="Generic problem",
        statement_md="Statement",
        runtime_status="blocked",
    )
    snapshot = _snapshot(
        required_delta_md="same",
        theorem_skeleton_md="same skeleton",
    )

    monkeypatch.setattr(
        "orchestrator.limacore.unblock_manager.materially_changed_required_delta",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        "orchestrator.limacore.unblock_manager.materially_changed_theorem_skeleton",
        lambda *_args, **_kwargs: False,
    )

    suggestion = manager.suggest(
        problem=problem,
        gap={"node_key": "target_theorem", "title": "Target theorem"},
        control_snapshot=snapshot,
        strongest_worlds=[{"family_key": "hidden_state", "status": "surviving"}],
        recent_fractures=[{"family_key": "quotient", "required_delta_md": "same", "repeat_count": 1, "ban_level": "none"}],
        recent_events=[],
    )
    assert all(candidate.strategy_kind != "repair" for candidate in suggestion.candidates)


def test_ranking_avoids_stale_same_family_retries() -> None:
    manager = UnblockManager()
    problem = ProblemSpec(
        id="problem",
        slug="generic-problem",
        title="Generic problem",
        statement_md="Statement",
        runtime_status="stalled",
    )
    snapshot = _snapshot()
    suggestion = manager.suggest(
        problem=problem,
        gap={"node_key": "target_theorem", "title": "Target theorem"},
        control_snapshot=snapshot,
        strongest_worlds=[{"family_key": "hidden_state", "status": "surviving"}],
        recent_fractures=[{"family_key": "quotient", "required_delta_md": "delta", "repeat_count": 3, "ban_level": "soft"}],
        recent_events=[{"family_key": "quotient"}, {"family_key": "quotient"}],
    )
    assert suggestion.chosen_delta is not None
    assert suggestion.chosen_delta.family_key != snapshot.current_family_key


def test_unblock_logic_works_across_seeded_problems(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    manager = UnblockManager()
    for slug in ("collatz", "inward-compression-conjecture"):
        row = db.get_problem(slug)
        assert row is not None
        problem = ProblemSpec(**row)
        problem.runtime_status = "blocked"
        snapshot = _snapshot(problem_slug=slug, family_key="coordinate_lift", node_key="target_theorem")
        suggestion = manager.suggest(
            problem=problem,
            gap={"node_key": "target_theorem", "title": "Target theorem"},
            control_snapshot=snapshot,
            strongest_worlds=[{"family_key": "operator_world", "status": "surviving"}],
            recent_fractures=[],
            recent_events=[],
        )
        assert suggestion.chosen_delta is not None
        assert suggestion.chosen_index == 0
