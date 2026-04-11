from __future__ import annotations

from .artifacts import utc_now
from .db import LimaCoreDB
from .models import FrontierNode


def ensure_target_frontier(db: LimaCoreDB, problem_id: str, *, target_statement: str) -> None:
    existing = db.get_frontier_node(problem_id, "target_theorem")
    if existing is not None:
        return
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
            dependency_keys=["bridge_claim", "local_energy_law", "terminal_form_uniqueness", "replay_closure"],
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
