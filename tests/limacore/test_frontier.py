from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.artifacts import utc_now
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.frontier import proof_debt
from orchestrator.limacore.models import FrontierNode


def test_frontier_nodes_add_update_block_and_prove(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    assert problem is not None
    node = FrontierNode(
        id="x",
        problem_id=str(problem["id"]),
        node_key="test_node",
        node_kind="lemma",
        title="Test node",
        status="open",
        updated_at=utc_now(),
    )
    db.upsert_frontier_node(node)
    node.status = "blocked"
    node.blocker_note_md = "need a witness"
    db.upsert_frontier_node(node)
    node.status = "proved"
    node.replay_ref = {"replay_certificate": "ok"}
    db.upsert_frontier_node(node)
    row = db.get_frontier_node(str(problem["id"]), "test_node")
    assert row is not None
    assert row["status"] == "proved"


def test_proof_debt_counts_open_and_blocked_nodes(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    assert problem is not None
    frontier = db.get_frontier_nodes(str(problem["id"]))
    assert proof_debt(frontier) >= 1
