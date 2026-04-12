"""Legacy frontier data cleanup for Lima-core problems.

Provides surgical cleanup of benchmark-shaped frontier artifacts that
pollute problem-native proof-debt graphs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .artifacts import utc_now
from .db import LimaCoreDB
from .frontier import ensure_target_frontier
from .models import ProblemSpec
from .runtime import detect_runtime_status, persist_runtime_status


# Legacy Collatz frontier signatures from old benchmark-shaped code
LEGACY_COLLATZ_NODE_KEYS = frozenset({
    "terminal_form_uniqueness",
})

LEGACY_COLLATZ_BLOCKER_PHRASES = frozenset({
    "canonical balanced-profile lemma",
    "balanced-profile",
    "terminal form",
})

LEGACY_COLLATZ_BLOCKER_KINDS = frozenset({
    "missing_uniqueness_lemma",
})


@dataclass
class LegacyCleanupResult:
    """Result of legacy frontier cleanup operation."""

    problem_id: str
    removed_node_keys: list[str]
    archived_node_keys: list[str]
    runtime_fields_cleared: list[str]
    cleanup_event_id: str
    rerun_triggered: bool = False


def is_legacy_collatz_frontier_node(node: dict[str, Any]) -> bool:
    """Check if a frontier node is legacy benchmark-shaped data for Collatz.

    Args:
        node: Frontier node dict from database

    Returns:
        True if this node is legacy Collatz data that should be cleaned up
    """
    node_key = str(node.get("node_key") or "").lower()
    title = str(node.get("title") or "").lower()
    blocker_note = str(node.get("blocker_note_md") or "").lower()
    blocker_kind = str(node.get("blocker_kind") or "").lower()
    statement = str(node.get("statement_md") or "").lower()

    # Check node key signatures
    if node_key in {k.lower() for k in LEGACY_COLLATZ_NODE_KEYS}:
        # Verify it's actually problematic (has IC-style language)
        if any(phrase in blocker_note for phrase in {
            "balanced-profile",
            "canonical",
            "terminal form uniqueness",
        }):
            return True
        if any(phrase in statement for phrase in {
            "balanced profile",
            "canonical profile",
        }):
            return True

    # Check for IC-style blocker text in Collatz context
    if any(phrase in blocker_note for phrase in LEGACY_COLLATZ_BLOCKER_PHRASES):
        # Make sure this isn't a valid Inward Compression node
        if "collatz" not in node_key and "parity" not in node_key and "carry" not in node_key:
            return True

    # Check blocker kind signatures
    if blocker_kind in {k.lower() for k in LEGACY_COLLATZ_BLOCKER_KINDS}:
        # Verify it's IC-style, not Collatz-native
        if not any(native in blocker_note for native in {
            "carry-ledger",
            "parity",
            "hidden-state",
            "return pattern",
            "odd step",
        }):
            return True

    return False


def detect_legacy_collatz_frontier_nodes(
    db: LimaCoreDB,
    problem_id: str,
) -> list[dict[str, Any]]:
    """Detect legacy frontier nodes for Collatz problem.

    Args:
        db: Database handle
        problem_id: Problem ID to check

    Returns:
        List of frontier node dicts that are legacy data
    """
    # Only run this for Collatz-like problems
    problem = db.get_problem(problem_id)
    if problem is None:
        return []

    slug = str(problem.get("slug") or "").lower()
    if "collatz" not in slug:
        # For non-Collatz problems, return empty (cleanup is Collatz-specific)
        return []

    frontier = db.get_frontier_nodes(problem_id)
    legacy_nodes = [node for node in frontier if is_legacy_collatz_frontier_node(node)]

    return legacy_nodes


def cleanup_legacy_collatz_frontier(
    db: LimaCoreDB,
    problem_id: str,
) -> LegacyCleanupResult:
    """Clean up legacy Collatz frontier nodes.

    Preserves event history, jobs, cohorts, and other useful state.
    Only removes/archives the problematic frontier nodes.

    Args:
        db: Database handle
        problem_id: Problem ID to clean

    Returns:
        Cleanup result with details of what was done
    """
    problem = db.get_problem(problem_id)
    if problem is None:
        raise KeyError(f"Problem not found: {problem_id}")

    # Detect legacy nodes
    legacy_nodes = detect_legacy_collatz_frontier_nodes(db, problem_id)

    removed_keys: list[str] = []
    archived_keys: list[str] = []

    # Archive/remove each legacy node
    for node in legacy_nodes:
        node_key = str(node.get("node_key") or "")
        node_id = str(node.get("id") or "")

        # Store archive artifact first (preserves the data for audit)
        archive_record = {
            "node_id": node_id,
            "node_key": node_key,
            "title": str(node.get("title") or ""),
            "status": str(node.get("status") or ""),
            "blocker_note_md": str(node.get("blocker_note_md") or ""),
            "blocker_kind": str(node.get("blocker_kind") or ""),
            "cleanup_reason": "legacy_benchmark_shaped_data",
            "cleanup_at": utc_now(),
        }
        db.store_artifact("legacy_frontier_archive", archive_record)

        # Remove the frontier node
        db.delete_frontier_node(problem_id, node_key)
        removed_keys.append(node_key)
        archived_keys.append(f"{node_key}:{node_id}")

    # Clear stale runtime fields if they reference removed nodes
    runtime_fields_cleared: list[str] = []
    current = db.get_problem(problem_id) or {}

    blocked_node_key = str(current.get("blocked_node_key") or "")
    current_runtime_status = str(current.get("runtime_status") or "blocked")
    if blocked_node_key in removed_keys:
        # Clear the blocked fields since they pointed to removed legacy node
        # Reset to running status since the blocker was removed
        db.update_problem_runtime(
            problem_id,
            runtime_status="running" if current_runtime_status == "blocked" else current_runtime_status,
            blocked_node_key="",
            blocker_kind="",
        )
        runtime_fields_cleared.extend(["blocked_node_key", "blocker_kind"])

    # Check if status_reason_md references legacy blockers
    status_reason = str(current.get("status_reason_md") or "").lower()
    if any(phrase in status_reason for phrase in LEGACY_COLLATZ_BLOCKER_PHRASES):
        # Update to a generic running status
        db.update_problem_runtime(
            problem_id,
            runtime_status=current_runtime_status,
            status_reason_md="Running: autopilot active.",
        )
        runtime_fields_cleared.append("status_reason_md")

    # Append cleanup event
    event_summary = f"Cleaned {len(removed_keys)} legacy frontier node(s): {', '.join(removed_keys) if removed_keys else 'none'}"
    event_id = db.append_event(
        problem_id,
        "legacy_frontier_cleanup",
        "accepted",
        summary_md=event_summary,
        artifact_refs=[
            {"kind": "cleanup_result", "removed": removed_keys, "archived": archived_keys}
        ],
    )

    # Recompute and persist runtime status
    persist_runtime_status(db, problem_id)

    return LegacyCleanupResult(
        problem_id=problem_id,
        removed_node_keys=removed_keys,
        archived_node_keys=archived_keys,
        runtime_fields_cleared=runtime_fields_cleared,
        cleanup_event_id=event_id,
        rerun_triggered=False,
    )


def restart_problem_clean(
    db: LimaCoreDB,
    loop: Any,  # LimaCoreLoop - avoid circular import
    problem_id: str,
) -> LegacyCleanupResult:
    """Clean up legacy frontier and restart problem with clean state.

    Args:
        db: Database handle
        loop: LimaCoreLoop instance for running iteration
        problem_id: Problem ID to restart

    Returns:
        Cleanup result with rerun information
    """
    # First cleanup legacy data
    result = cleanup_legacy_collatz_frontier(db, problem_id)

    problem = db.get_problem(problem_id)
    if problem is None:
        raise KeyError(f"Problem not found: {problem_id}")

    spec = ProblemSpec(**problem)

    # Reset runtime to appropriate starting state
    if spec.runtime_status in {"blocked", "stalled", "failed"}:
        # Reset to running for clean restart
        db.update_problem_runtime(
            problem_id,
            runtime_status="running",
            status_reason_md="Running: clean restart after legacy cleanup.",
            stalled_since="",
            last_gain_at=utc_now(),
        )
    elif spec.runtime_status in {"paused"}:
        # Keep paused if explicitly paused, but enable autopilot
        db.update_problem_runtime(
            problem_id,
            autopilot_enabled=True,
        )

    # Ensure target frontier still exists
    ensure_target_frontier(
        db,
        problem_id,
        target_statement=spec.target_theorem or spec.statement_md,
    )

    # Run one bounded iteration
    from .loop import LimaCoreLoop

    if isinstance(loop, LimaCoreLoop):
        loop.run_iteration(problem_id)
        result.rerun_triggered = True

    # Persist final runtime status
    persist_runtime_status(db, problem_id)

    # Update cleanup event with rerun info
    db.append_event(
        problem_id,
        "clean_restart_completed",
        "accepted",
        summary_md=f"Clean restart completed. Legacy nodes removed: {len(result.removed_node_keys)}",
    )

    return result


def has_legacy_frontier_cleanup_available(
    db: LimaCoreDB,
    problem_id: str,
) -> bool:
    """Check if legacy frontier cleanup is available for this problem.

    Args:
        db: Database handle
        problem_id: Problem ID to check

    Returns:
        True if there are legacy nodes to clean up
    """
    try:
        legacy_nodes = detect_legacy_collatz_frontier_nodes(db, problem_id)
        return len(legacy_nodes) > 0
    except Exception:
        return False
