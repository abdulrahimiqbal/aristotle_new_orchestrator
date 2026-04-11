"""Problem-native frontier node derivation.

Derives downstream frontier nodes from reduction packets, world families,
and problem context rather than using hardcoded benchmark-shaped nodes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .artifacts import utc_now
from .models import FrontierNode, ProblemSpec, ReductionPacket


@dataclass(slots=True)
class FrontierHint:
    """A hint for frontier node generation from the compiler/reduction."""

    node_key: str
    title: str
    node_kind: str
    blocker_kind: str = ""
    blocker_note: str = ""
    priority: float = 7.0


@dataclass
class DerivedFrontier:
    """Result of frontier derivation for a problem/delta/reduction combination."""

    bridge_node: FrontierNode | None = None
    local_law_node: FrontierNode | None = None
    downstream_node: FrontierNode | None = None
    replay_node: FrontierNode | None = None
    target_dependencies: list[str] = field(default_factory=list)
    all_nodes: list[FrontierNode] = field(default_factory=list)


def _derive_collatz_hidden_state_frontier(
    problem: ProblemSpec,
    reduction: ReductionPacket,
    family_key: str,
    jobs_proved: list[dict],
    jobs_all: list[dict],
) -> DerivedFrontier:
    """Derive Collatz-native frontier for hidden_state parity-carry line.

    Instead of generic 'terminal_form_uniqueness', produces nodes like:
    - carry_ledger_bridge_closure
    - parity_block_drift_extension
    - global_return_pattern_closure
    - hidden_state_equivalence
    """
    # Check if we have the jobs that would prove the skeleton
    skeleton_proved = any(
        job["job_kind"] == "theorem_skeleton_probe" and job["replayable"] for job in jobs_all
    )

    # Determine downstream node based on reduction content and jobs
    downstream_status = "proved" if skeleton_proved else "blocked"

    # Look for specific blocker indicators in job results
    blocker_kind = ""
    blocker_note = ""
    if not skeleton_proved:
        # Check jobs for Collatz-native blocker indicators
        for job in jobs_all:
            if job["job_kind"] == "theorem_skeleton_probe":
                artifact = dict(job.get("output_artifact_ref", {})).get("content", {})
                blocker = artifact.get("blocker", "")
                if blocker == "global_return_pattern_closure":
                    blocker_kind = "missing_return_pattern_closure"
                    blocker_note = "need to extend carry-ledger drift to all return patterns"
                elif blocker == "odd_step_expansion":
                    blocker_kind = "odd_step_control_gap"
                    blocker_note = "need rigorous control of odd-step expansion"
                else:
                    blocker_kind = "global_pattern_closure"
                    blocker_note = "need to extend carry-ledger drift bound to all return patterns"
                break
        if not blocker_note:
            blocker_kind = "global_pattern_closure"
            blocker_note = "need hidden-state/original-map equivalence closure"

    # Use reduction's theorem skeleton to derive a problem-native node key
    skeleton_text = reduction.theorem_skeleton.lower()

    # Derive node key from content hints
    if "carry" in skeleton_text or "ledger" in skeleton_text:
        downstream_key = "carry_ledger_bridge_closure"
        downstream_title = "Carry ledger bridge closure"
    elif "parity" in skeleton_text or "block" in skeleton_text:
        downstream_key = "parity_block_drift_extension"
        downstream_title = "Parity block drift extension"
    elif "return" in skeleton_text or "pattern" in skeleton_text:
        downstream_key = "global_return_pattern_closure"
        downstream_title = "Global return pattern closure"
    elif "equivalence" in skeleton_text or "hidden" in skeleton_text:
        downstream_key = "hidden_state_equivalence"
        downstream_title = "Hidden state equivalence"
    else:
        # Default Collatz-native naming
        downstream_key = "accelerated_odd_step_control"
        downstream_title = "Accelerated odd step control"

    downstream = FrontierNode(
        id=f"{problem.id}-downstream",
        problem_id=problem.id,
        node_key=downstream_key,
        node_kind="theorem_skeleton",
        title=downstream_title,
        statement_md=reduction.theorem_skeleton,
        formal_statement=reduction.theorem_skeleton,
        status=downstream_status,
        blocker_kind=blocker_kind if downstream_status == "blocked" else "",
        blocker_note_md=blocker_note if downstream_status == "blocked" else "",
        priority=7.0,
        updated_at=utc_now(),
    )

    # Target theorem dependencies are problem-native now
    target_deps = ["bridge_claim", "local_energy_law", downstream_key, "replay_closure"]

    return DerivedFrontier(
        downstream_node=downstream,
        target_dependencies=target_deps,
        all_nodes=[downstream],
    )


def _derive_inward_compression_frontier(
    problem: ProblemSpec,
    reduction: ReductionPacket,
    family_key: str,
    jobs_proved: list[dict],
    jobs_all: list[dict],
) -> DerivedFrontier:
    """Derive Inward Compression frontier with balanced-profile style nodes."""
    # Check if skeleton is proved
    skeleton_proved = any(
        job["job_kind"] == "theorem_skeleton_probe" and job["replayable"] for job in jobs_all
    )

    downstream_status = "proved" if skeleton_proved else "blocked"

    blocker_kind = ""
    blocker_note = ""
    if not skeleton_proved:
        # Look for IC-native blocker
        for job in jobs_all:
            if job["job_kind"] == "theorem_skeleton_probe":
                artifact = dict(job.get("output_artifact_ref", {})).get("content", {})
                blocker = artifact.get("blocker", "")
                if blocker == "need_canonical_profile_lemma":
                    blocker_kind = "missing_uniqueness_lemma"
                    blocker_note = "Need a full canonical balanced-profile lemma."
                break
        if not blocker_note:
            blocker_kind = "missing_uniqueness_lemma"
            blocker_note = "Need a full canonical balanced-profile lemma."

    # Inward Compression uses balanced/canonical profile language
    downstream = FrontierNode(
        id=f"{problem.id}-skeleton",
        problem_id=problem.id,
        node_key="terminal_form_uniqueness",
        node_kind="theorem_skeleton",
        title="Terminal form uniqueness",
        statement_md=reduction.theorem_skeleton,
        formal_statement=reduction.theorem_skeleton,
        status=downstream_status,
        blocker_kind=blocker_kind if downstream_status == "blocked" else "",
        blocker_note_md=blocker_note if downstream_status == "blocked" else "",
        priority=7.0,
        updated_at=utc_now(),
    )

    target_deps = ["bridge_claim", "local_energy_law", "terminal_form_uniqueness", "replay_closure"]

    return DerivedFrontier(
        downstream_node=downstream,
        target_dependencies=target_deps,
        all_nodes=[downstream],
    )


def _derive_generic_frontier(
    problem: ProblemSpec,
    reduction: ReductionPacket,
    family_key: str,
    jobs_proved: list[dict],
    jobs_all: list[dict],
) -> DerivedFrontier:
    """Generic frontier derivation for problems without specific handlers."""
    skeleton_proved = any(
        job["job_kind"] == "theorem_skeleton_probe" and job["replayable"] for job in jobs_all
    )

    downstream_status = "proved" if skeleton_proved else "blocked"

    # Derive node key from family and problem slug
    if family_key:
        downstream_key = f"{family_key}_closure"
    else:
        downstream_key = "theorem_skeleton_closure"

    downstream_title = "Theorem skeleton closure"

    blocker_kind = ""
    blocker_note = ""
    if not skeleton_proved:
        blocker_kind = "skeleton_incomplete"
        blocker_note = f"Need to complete {family_key or 'current'} line proof structure."

    downstream = FrontierNode(
        id=f"{problem.id}-skeleton",
        problem_id=problem.id,
        node_key=downstream_key,
        node_kind="theorem_skeleton",
        title=downstream_title,
        statement_md=reduction.theorem_skeleton,
        formal_statement=reduction.theorem_skeleton,
        status=downstream_status,
        blocker_kind=blocker_kind if downstream_status == "blocked" else "",
        blocker_note_md=blocker_note if downstream_status == "blocked" else "",
        priority=7.0,
        updated_at=utc_now(),
    )

    target_deps = ["bridge_claim", "local_energy_law", downstream_key, "replay_closure"]

    return DerivedFrontier(
        downstream_node=downstream,
        target_dependencies=target_deps,
        all_nodes=[downstream],
    )


def derive_frontier_updates(
    problem: ProblemSpec,
    delta_family_key: str,
    reduction: ReductionPacket,
    jobs: list[dict],
) -> DerivedFrontier:
    """Derive frontier updates from problem context, delta, reduction, and job results.

    This is the main entry point for problem-native frontier derivation.
    It replaces the hardcoded benchmark-shaped frontier generation.

    Args:
        problem: The problem specification (includes slug, domain, etc.)
        delta_family_key: The family key of the accepted delta/world
        reduction: The reduction packet with bridge, local law, skeleton
        jobs: List of job result dicts from Aristotle execution

    Returns:
        DerivedFrontier with all nodes to create/update and target theorem dependencies
    """
    jobs_proved = [job for job in jobs if job.get("replayable")]
    jobs_all = jobs

    # Route to problem-specific derivation
    if problem.slug == "collatz" or problem.slug == "collatz-conjecture":
        if delta_family_key == "hidden_state":
            return _derive_collatz_hidden_state_frontier(
                problem, reduction, delta_family_key, jobs_proved, jobs_all
            )
        # Collatz with other families uses generic with Collatz-flavored naming
        return _derive_generic_frontier(problem, reduction, delta_family_key, jobs_proved, jobs_all)

    if problem.slug == "inward-compression-conjecture":
        return _derive_inward_compression_frontier(
            problem, reduction, delta_family_key, jobs_proved, jobs_all
        )

    # Default: generic derivation
    return _derive_generic_frontier(problem, reduction, delta_family_key, jobs_proved, jobs_all)


def make_bridge_node(problem: ProblemSpec, reduction: ReductionPacket, family_key: str | None) -> FrontierNode:
    """Create a bridge_claim frontier node."""
    return FrontierNode(
        id=f"{problem.id}-bridge",
        problem_id=problem.id,
        node_key="bridge_claim",
        node_kind="bridge_lemma",
        title="Bridge claim",
        statement_md=reduction.bridge_claim,
        formal_statement=reduction.bridge_claim,
        status="proved",
        best_world_id=family_key,
        replay_ref={"replay_certificate": "bridge_claim"},
        priority=9.0,
        updated_at=utc_now(),
    )


def make_local_law_node(problem: ProblemSpec, reduction: ReductionPacket, family_key: str | None) -> FrontierNode:
    """Create a local_energy_law frontier node."""
    return FrontierNode(
        id=f"{problem.id}-law",
        problem_id=problem.id,
        node_key="local_energy_law",
        node_kind="local_law",
        title="Local energy law",
        statement_md=reduction.local_law,
        formal_statement=reduction.local_law,
        status="proved",
        best_world_id=family_key,
        replay_ref={"replay_certificate": "local_energy_law"},
        priority=8.0,
        updated_at=utc_now(),
    )


def make_replay_node(problem: ProblemSpec) -> FrontierNode:
    """Create a replay_closure frontier node."""
    return FrontierNode(
        id=f"{problem.id}-replay",
        problem_id=problem.id,
        node_key="replay_closure",
        node_kind="replay_check",
        title="Replay closure",
        statement_md="All dependencies replay from clean state.",
        formal_statement="Replay closure check",
        status="open",
        priority=6.0,
        updated_at=utc_now(),
    )
