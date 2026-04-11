from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.aristotle import LocalAristotleBackend
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import LimaCoreLoop
from orchestrator.limacore.models import DeltaProposal
from orchestrator.limacore.program import maybe_accept_program_delta, write_candidate_program_delta


def test_one_loop_iteration_commits_good_delta(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    result = loop.run_iteration("inward-compression-conjecture")
    assert result["accepted"] is True
    worlds = db.list_world_heads(str(db.get_problem("inward-compression-conjecture")["id"]))
    assert worlds


def test_one_loop_iteration_reverts_bad_delta(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    bad = DeltaProposal(
        delta_type="lemma_delta",
        title="narrative-only",
        summary_md="narrative-only",
        family_key="other",
        target_node_key="target_theorem",
        edits={
            "bridge_claim": "narrative",
            "local_law": "narrative",
            "kill_test": "narrative",
            "theorem_skeleton": "narrative",
            "obligations": ["narrative"],
        },
    )
    result = loop.run_iteration("collatz", forced_delta=bad)
    assert result["accepted"] is False


def test_program_delta_only_kept_on_verified_yield(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("inward-compression-conjecture")
    assert problem is not None
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    loop.run_iteration(str(problem["id"]))
    candidate = write_candidate_program_delta(db, str(problem["id"]), note="tighten acceptance wording")
    assert maybe_accept_program_delta(db, str(problem["id"]), candidate) is True


def test_collatz_rotates_off_stale_quotient_loop_and_earns_new_replayable_gain(tmp_path: Path) -> None:
    """Test that Collatz control law rotates off stale quotient to productive families.
    
    With the fixed control-law patch:
    - First iteration creates quotient world
    - Exhaustion detected (no replayable gain + failed jobs)
    - System rotates to hidden_state family
    - hidden_state produces replayable lemmas
    """
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())

    # Run 6 iterations to see rotation behavior
    results = [loop.run_iteration("collatz") for _ in range(6)]

    # First should always create a world
    assert results[0]["accepted"] is True, "First iteration should create a world"
    
    # Check that we eventually rotate to hidden_state and make progress
    problem = db.get_problem("collatz")
    worlds = db.list_world_heads(str(problem["id"]))
    families = {str(world["family_key"]) for world in worlds}
    
    # Should have rotated to hidden_state
    assert "hidden_state" in families, "Should rotate to hidden_state family"
    
    # Check cohorts for lemmas
    cohorts = db.list_cohorts(str(problem["id"]))
    total_lemmas = sum(int(c.get("yielded_lemmas", 0)) for c in cohorts)
    
    # Should have yielded some lemmas from hidden_state
    assert total_lemmas > 0, f"Should have yielded lemmas, got {total_lemmas}"


def test_collatz_produces_native_frontier_nodes_not_benchmark_shaped(tmp_path: Path) -> None:
    """Collatz frontier should be free of legacy benchmark-shaped terminal_form_uniqueness blocker.
    
    After the cleanup patch, Collatz should not have the IC-style terminal_form_uniqueness
    node with "balanced-profile lemma" blocker notes.
    """
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())

    # Run a few iterations
    for _ in range(3):
        loop.run_iteration("collatz")

    # Check frontier
    problem = db.get_problem("collatz")
    assert problem is not None
    frontier = db.get_frontier_nodes(str(problem["id"]))

    # Should NOT have terminal_form_uniqueness as a blocked node with IC-style blocker
    for node in frontier:
        node_key = str(node.get("node_key") or "")
        blocker_note = str(node.get("blocker_note_md") or "").lower()
        if node_key == "terminal_form_uniqueness":
            # If it exists, it should NOT have IC-style balanced-profile language
            assert "balanced-profile" not in blocker_note, \
                f"terminal_form_uniqueness should not have IC-style blocker: {blocker_note}"
            assert "canonical" not in blocker_note, \
                f"terminal_form_uniqueness should not have IC-style blocker: {blocker_note}"

    # The frontier should have the target_theorem (always present)
    node_keys = {str(node["node_key"]) for node in frontier}
    assert "target_theorem" in node_keys, "target_theorem should always be in frontier"


def test_collatz_blocker_note_is_native_not_balanced_profile(tmp_path: Path) -> None:
    """Collatz blocked notes should reference carry-ledger/parity, not balanced-profile."""
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())

    # Run iterations
    loop.run_iteration("collatz")
    loop.run_iteration("collatz")

    problem = db.get_problem("collatz")
    assert problem is not None
    frontier = db.get_frontier_nodes(str(problem["id"]))

    # Check blocked nodes for non-native language
    for node in frontier:
        if str(node["status"]) == "blocked":
            blocker_note = str(node.get("blocker_note_md", "")).lower()
            # Should NOT mention balanced-profile or canonical
            assert "balanced-profile" not in blocker_note, f"Collatz blocker should not mention balanced-profile: {blocker_note}"
            assert "canonical" not in blocker_note or "hidden" in blocker_note, f"Collatz blocker should use native language: {blocker_note}"
