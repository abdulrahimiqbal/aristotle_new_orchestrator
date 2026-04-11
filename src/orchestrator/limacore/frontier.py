from __future__ import annotations

from .artifacts import utc_now
from .db import LimaCoreDB
from .models import FrontierNode


def ensure_target_frontier(db: LimaCoreDB, problem_id: str, *, target_statement: str) -> None:
    """Ensure the target theorem frontier node exists.

    The target theorem starts with a minimal default dependency list.
    These dependencies are updated dynamically as the frontier evolves
    through problem-native derivation in loop._commit_delta().
    """
    existing = db.get_frontier_node(problem_id, "target_theorem")
    if existing is not None:
        return
    # Start with minimal deps - will be updated dynamically based on derived frontier
    db.upsert_frontier_node(
        FrontierNode(
            id=f"{problem_id}-target",
            problem_id=problem_id,
            node_key="target_theorem",
            node_kind="target",
            title="Target theorem",
            statement_md=target_statement,
            formal_statement=target_statement,
            status="open",
            # Default deps - these are updated dynamically by derive_frontier_updates
            dependency_keys=["bridge_claim", "local_energy_law", "replay_closure"],
            priority=10.0,
            updated_at=utc_now(),
        )
    )


def proof_debt(frontier: list[dict]) -> int:
    return sum(1 for node in frontier if str(node.get("status") or "") != "proved")


def select_frontier_gap(db: LimaCoreDB, problem_id: str) -> dict:
    frontier = db.get_frontier_nodes(problem_id)
    open_nodes = [node for node in frontier if str(node.get("status") or "") in {"open", "blocked"}]
    if not open_nodes:
        raise ValueError("no frontier gaps available")
    open_nodes.sort(
        key=lambda node: (
            0 if node["status"] == "open" else 1,
            -float(node.get("priority") or 0.0),
            str(node.get("node_key") or ""),
        )
    )
    return open_nodes[0]


def update_target_dependencies(db: LimaCoreDB, problem_id: str, dependency_keys: list[str]) -> None:
    """Update the target theorem's dependency keys dynamically.

    This is called by the loop after deriving frontier updates to keep
    the target theorem's dependencies in sync with the actual proved nodes.
    """
    target = db.get_frontier_node(problem_id, "target_theorem")
    if target is None:
        return
    updated_node = FrontierNode(
        id=str(target["id"]),
        problem_id=problem_id,
        node_key="target_theorem",
        node_kind=str(target["node_kind"]),
        title=str(target["title"]),
        statement_md=str(target["statement_md"]),
        formal_statement=str(target["formal_statement"]),
        status=str(target["status"]),
        dependency_keys=list(dependency_keys),
        priority=float(target.get("priority") or 10.0),
        replay_ref=dict(target.get("replay_ref") or {}),
        updated_at=utc_now(),
    )
    db.upsert_frontier_node(updated_node)
