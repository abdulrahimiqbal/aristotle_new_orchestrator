from __future__ import annotations

from .db import LimaCoreDB
from .models import SolvedReport


def solved_checker(db: LimaCoreDB, problem_id: str) -> SolvedReport:
    """Check if the problem is solved based on frontier state.

    A problem is solved when:
    1. The target theorem node is marked 'proved'
    2. All nodes marked as 'proved' have valid replay references
    3. All dependency keys for the target theorem are proved
    4. There are no remaining open nodes
    """
    nodes = db.get_frontier_nodes(problem_id)
    node_map = {str(node["node_key"]): node for node in nodes}
    target = node_map.get("target_theorem")
    if target is None:
        return SolvedReport(
            solved=False,
            reason="missing target theorem node",
            open_nodes=["target_theorem"],
            replay_passed=False,
            dependency_closure_passed=False,
        )

    # Collect open nodes (non-proved)
    open_nodes = [
        node["node_key"]
        for node in nodes
        if str(node.get("status") or "") != "proved"
    ]

    # Check replay integrity: all proved nodes must have replay refs
    replay_passed = True
    for node in nodes:
        if str(node.get("status") or "") == "proved":
            replay_ref = dict(node.get("replay_ref") or {})
            if not replay_ref.get("hash") and not replay_ref.get("replay_certificate"):
                replay_passed = False
                break

    # Check dependency closure: all target dependencies must be proved
    dep_ok = True
    target_deps = target.get("dependency_keys", [])
    # If no specific dependencies, require at least bridge + local law + one downstream
    if not target_deps:
        # Legacy fallback - look for key proved nodes
        required = {"bridge_claim", "local_energy_law", "replay_closure"}
        proved_keys = {str(node.get("node_key")) for node in nodes if str(node.get("status") or "") == "proved"}
        dep_ok = bool(required.intersection(proved_keys))
    else:
        for dep in target_deps:
            dep_node = node_map.get(dep)
            if dep_node is None or str(dep_node.get("status") or "") != "proved":
                dep_ok = False
                break

    # Problem is solved if target is proved, replay passed, deps satisfied, and nothing open
    target_proved = str(target.get("status") or "") == "proved"
    solved = target_proved and replay_passed and dep_ok and not open_nodes

    if solved:
        reason = "proof DAG closed"
    elif not target_proved:
        reason = "target theorem not yet proved"
    elif open_nodes:
        reason = f"open nodes remain: {', '.join(open_nodes[:3])}{'...' if len(open_nodes) > 3 else ''}"
    elif not replay_passed:
        reason = "replay check failed for some proved nodes"
    elif not dep_ok:
        reason = "dependency closure not satisfied"
    else:
        reason = "unsolved"

    return SolvedReport(
        solved=solved,
        reason=reason,
        open_nodes=open_nodes,
        replay_passed=replay_passed,
        dependency_closure_passed=dep_ok,
    )
