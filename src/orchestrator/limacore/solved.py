from __future__ import annotations

from .db import LimaCoreDB
from .models import SolvedReport


def solved_checker(db: LimaCoreDB, problem_id: str) -> SolvedReport:
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
    open_nodes = [
        node["node_key"]
        for node in nodes
        if str(node.get("status") or "") != "proved"
    ]
    replay_passed = True
    for node in nodes:
        if str(node.get("status") or "") == "proved":
            replay_ref = dict(node.get("replay_ref") or {})
            if not replay_ref.get("hash") and not replay_ref.get("replay_certificate"):
                replay_passed = False
                break
    dep_ok = True
    for dep in target.get("dependency_keys", []):
        dep_node = node_map.get(dep)
        if dep_node is None or str(dep_node.get("status") or "") != "proved":
            dep_ok = False
            break
    solved = str(target.get("status") or "") == "proved" and replay_passed and dep_ok and not open_nodes
    reason = "proof DAG closed" if solved else "open nodes remain or replay failed"
    return SolvedReport(
        solved=solved,
        reason=reason,
        open_nodes=open_nodes,
        replay_passed=replay_passed,
        dependency_closure_passed=dep_ok,
    )
