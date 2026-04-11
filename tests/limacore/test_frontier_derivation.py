"""Tests for problem-native frontier derivation (Fix 3)."""

from __future__ import annotations

from orchestrator.limacore.frontier_derivation import (
    _derive_collatz_hidden_state_frontier,
    _derive_generic_frontier,
    _derive_inward_compression_frontier,
    derive_frontier_updates,
    make_bridge_node,
    make_local_law_node,
    make_replay_node,
)
from orchestrator.limacore.models import FrontierNode, ProblemSpec, ReductionPacket


class TestCollatzHiddenStateFrontier:
    """Collatz hidden-state frontier must be problem-native, not benchmark-shaped."""

    def test_collatz_hidden_state_produces_native_downstream_node(self) -> None:
        """Collatz hidden_state should NOT produce 'terminal_form_uniqueness'."""
        problem = ProblemSpec(
            id="test-123",
            slug="collatz",
            title="Collatz Conjecture",
            statement_md="Test",
        )
        reduction = ReductionPacket(
            selected_gap="target_theorem",
            bridge_claim="carry ledger bridge",
            local_law="parity drift bound",
            kill_test="counterexample search",
            theorem_skeleton="carry-ledger closure in parity blocks",
            obligations=["bridge", "law", "skeleton"],
            cohort_plan=[],
            rationale_md="test",
        )

        # No jobs proved skeleton
        jobs = [
            {"job_kind": "bridge_lemma", "replayable": True, "output_artifact_ref": {}},
            {"job_kind": "local_law", "replayable": True, "output_artifact_ref": {}},
            {"job_kind": "theorem_skeleton_probe", "replayable": False, "output_artifact_ref": {}},
        ]

        derived = derive_frontier_updates(problem, "hidden_state", reduction, jobs)

        # Should have a downstream node
        assert derived.downstream_node is not None
        node = derived.downstream_node

        # Should NOT be the hardcoded benchmark name
        assert node.node_key != "terminal_form_uniqueness"
        assert node.title != "Terminal form uniqueness"

        # Should be Collatz-native
        assert "carry" in node.node_key or "parity" in node.node_key or "ledger" in node.node_key or "accelerated" in node.node_key

    def test_collatz_hidden_state_blocked_note_is_native(self) -> None:
        """Blocked notes should reference carry-ledger/parity, not balanced-profile."""
        problem = ProblemSpec(
            id="test-123",
            slug="collatz",
            title="Collatz Conjecture",
            statement_md="Test",
        )
        reduction = ReductionPacket(
            selected_gap="target_theorem",
            bridge_claim="carry ledger bridge",
            local_law="parity drift bound",
            kill_test="counterexample search",
            theorem_skeleton="carry-ledger closure",
            obligations=["bridge", "law", "skeleton"],
            cohort_plan=[],
            rationale_md="test",
        )

        # Skeleton not proved - should be blocked
        jobs = [
            {"job_kind": "bridge_lemma", "replayable": True, "output_artifact_ref": {}},
            {"job_kind": "local_law", "replayable": True, "output_artifact_ref": {}},
            {"job_kind": "theorem_skeleton_probe", "replayable": False, "output_artifact_ref": {}},
        ]

        derived = derive_frontier_updates(problem, "hidden_state", reduction, jobs)

        assert derived.downstream_node is not None
        node = derived.downstream_node

        assert node.status == "blocked"
        # Blocker note should NOT mention balanced-profile
        assert "balanced-profile" not in node.blocker_note_md.lower()
        assert "canonical" not in node.blocker_note_md.lower()
        # Should mention Collatz-native concepts
        assert (
            "carry" in node.blocker_note_md.lower()
            or "parity" in node.blocker_note_md.lower()
            or "hidden-state" in node.blocker_note_md.lower()
            or "return pattern" in node.blocker_note_md.lower()
        )

    def test_collatz_skeleton_proved_marks_node_proved(self) -> None:
        """When skeleton job is replayable, downstream node should be proved."""
        problem = ProblemSpec(
            id="test-123",
            slug="collatz",
            title="Collatz Conjecture",
            statement_md="Test",
        )
        reduction = ReductionPacket(
            selected_gap="target_theorem",
            bridge_claim="carry ledger bridge",
            local_law="parity drift bound",
            kill_test="counterexample search",
            theorem_skeleton="carry-ledger closure in parity blocks",
            obligations=["bridge", "law", "skeleton"],
            cohort_plan=[],
            rationale_md="test",
        )

        # Skeleton proved
        jobs = [
            {"job_kind": "bridge_lemma", "replayable": True, "output_artifact_ref": {}},
            {"job_kind": "local_law", "replayable": True, "output_artifact_ref": {}},
            {"job_kind": "theorem_skeleton_probe", "replayable": True, "output_artifact_ref": {}},
        ]

        derived = derive_frontier_updates(problem, "hidden_state", reduction, jobs)

        assert derived.downstream_node is not None
        assert derived.downstream_node.status == "proved"
        assert derived.downstream_node.blocker_kind == ""
        assert derived.downstream_node.blocker_note_md == ""

    def test_collatz_target_dependencies_include_native_node(self) -> None:
        """Target theorem dependencies should include the native downstream node key."""
        problem = ProblemSpec(
            id="test-123",
            slug="collatz",
            title="Collatz Conjecture",
            statement_md="Test",
        )
        reduction = ReductionPacket(
            selected_gap="target_theorem",
            bridge_claim="carry ledger bridge",
            local_law="parity drift bound",
            kill_test="counterexample search",
            theorem_skeleton="carry-ledger closure in parity blocks",
            obligations=["bridge", "law", "skeleton"],
            cohort_plan=[],
            rationale_md="test",
        )

        jobs = [
            {"job_kind": "bridge_lemma", "replayable": True, "output_artifact_ref": {}},
            {"job_kind": "local_law", "replayable": True, "output_artifact_ref": {}},
        ]

        derived = derive_frontier_updates(problem, "hidden_state", reduction, jobs)

        # Dependencies should include the native node key, not terminal_form_uniqueness
        assert "terminal_form_uniqueness" not in derived.target_dependencies
        # Should have bridge, law, replay, and a native node
        assert "bridge_claim" in derived.target_dependencies
        assert "local_energy_law" in derived.target_dependencies
        assert "replay_closure" in derived.target_dependencies
        # One of the native keys
        native_keys = [
            "carry_ledger_bridge_closure",
            "parity_block_drift_extension",
            "global_return_pattern_closure",
            "hidden_state_equivalence",
            "accelerated_odd_step_control",
        ]
        assert any(key in derived.target_dependencies for key in native_keys)


class TestInwardCompressionFrontier:
    """Inward Compression frontier should still use balanced-profile language."""

    def test_inward_compression_produces_balanced_profile_nodes(self) -> None:
        """Inward Compression can use canonical/balanced-profile style nodes."""
        problem = ProblemSpec(
            id="test-456",
            slug="inward-compression-conjecture",
            title="Inward Compression",
            statement_md="Test",
        )
        reduction = ReductionPacket(
            selected_gap="target_theorem",
            bridge_claim="offset coordinates bridge",
            local_law="convex energy descent",
            kill_test="counterexample search",
            theorem_skeleton="balanced profile uniqueness in inward flow",
            obligations=["bridge", "law", "skeleton"],
            cohort_plan=[],
            rationale_md="test",
        )

        jobs = [
            {"job_kind": "bridge_lemma", "replayable": True, "output_artifact_ref": {}},
            {"job_kind": "local_law", "replayable": True, "output_artifact_ref": {}},
            {"job_kind": "theorem_skeleton_probe", "replayable": False, "output_artifact_ref": {
                "content": {"blocker": "need_canonical_profile_lemma"}
            }},
        ]

        derived = derive_frontier_updates(problem, "balancing_world", reduction, jobs)

        assert derived.downstream_node is not None
        # Inward Compression CAN use terminal_form_uniqueness naming
        assert derived.downstream_node.node_key == "terminal_form_uniqueness"
        assert derived.downstream_node.title == "Terminal form uniqueness"

    def test_inward_compression_blocked_note_mentions_canonical_profile(self) -> None:
        """Inward Compression blocked notes can reference canonical profile lemmas."""
        problem = ProblemSpec(
            id="test-456",
            slug="inward-compression-conjecture",
            title="Inward Compression",
            statement_md="Test",
        )
        reduction = ReductionPacket(
            selected_gap="target_theorem",
            bridge_claim="offset coordinates bridge",
            local_law="convex energy descent",
            kill_test="counterexample search",
            theorem_skeleton="balanced profile uniqueness",
            obligations=["bridge", "law", "skeleton"],
            cohort_plan=[],
            rationale_md="test",
        )

        jobs = [
            {"job_kind": "theorem_skeleton_probe", "replayable": False, "output_artifact_ref": {
                "content": {"blocker": "need_canonical_profile_lemma"}
            }},
        ]

        derived = derive_frontier_updates(problem, "balancing_world", reduction, jobs)

        assert derived.downstream_node is not None
        assert derived.downstream_node.status == "blocked"
        # CAN mention canonical/balanced-profile
        assert "canonical" in derived.downstream_node.blocker_note_md.lower() or \
               "balanced-profile" in derived.downstream_node.blocker_note_md.lower()


class TestGenericFrontier:
    """Generic frontier derivation for unknown problems."""

    def test_generic_derivation_creates_family_based_node(self) -> None:
        """Generic problems get family-based node keys."""
        problem = ProblemSpec(
            id="test-789",
            slug="unknown-problem",
            title="Unknown Problem",
            statement_md="Test",
        )
        reduction = ReductionPacket(
            selected_gap="target_theorem",
            bridge_claim="some bridge",
            local_law="some law",
            kill_test="some kill test",
            theorem_skeleton="some skeleton",
            obligations=["bridge", "law", "skeleton"],
            cohort_plan=[],
            rationale_md="test",
        )

        jobs = [
            {"job_kind": "bridge_lemma", "replayable": True, "output_artifact_ref": {}},
            {"job_kind": "local_law", "replayable": True, "output_artifact_ref": {}},
        ]

        derived = derive_frontier_updates(problem, "my_family", reduction, jobs)

        assert derived.downstream_node is not None
        # Should be family-based naming
        assert "my_family" in derived.downstream_node.node_key


class TestHelperNodeCreation:
    """Test the helper functions for creating standard frontier nodes."""

    def test_make_bridge_node(self) -> None:
        """Bridge node should have correct structure."""
        problem = ProblemSpec(
            id="test",
            slug="test",
            title="Test",
            statement_md="Test",
        )
        reduction = ReductionPacket(
            selected_gap="gap",
            bridge_claim="test bridge claim",
            local_law="test law",
            kill_test="test kill",
            theorem_skeleton="test skeleton",
            obligations=[],
            cohort_plan=[],
            rationale_md="test",
        )

        node = make_bridge_node(problem, reduction, "test_family")

        assert node.node_key == "bridge_claim"
        assert node.node_kind == "bridge_lemma"
        assert node.status == "proved"
        assert node.statement_md == "test bridge claim"
        assert node.best_world_id == "test_family"

    def test_make_local_law_node(self) -> None:
        """Local law node should have correct structure."""
        problem = ProblemSpec(
            id="test",
            slug="test",
            title="Test",
            statement_md="Test",
        )
        reduction = ReductionPacket(
            selected_gap="gap",
            bridge_claim="test bridge",
            local_law="test local law",
            kill_test="test kill",
            theorem_skeleton="test skeleton",
            obligations=[],
            cohort_plan=[],
            rationale_md="test",
        )

        node = make_local_law_node(problem, reduction, "test_family")

        assert node.node_key == "local_energy_law"
        assert node.node_kind == "local_law"
        assert node.status == "proved"
        assert node.statement_md == "test local law"
        assert node.best_world_id == "test_family"

    def test_make_replay_node(self) -> None:
        """Replay node should have correct structure."""
        problem = ProblemSpec(
            id="test",
            slug="test",
            title="Test",
            statement_md="Test",
        )

        node = make_replay_node(problem)

        assert node.node_key == "replay_closure"
        assert node.node_kind == "replay_check"
        assert node.status == "open"
