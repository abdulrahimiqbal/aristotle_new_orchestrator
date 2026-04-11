from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.artifacts import utc_now
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.models import FrontierNode
from orchestrator.limacore.solved import solved_checker


def test_unsolved_problem_reports_open_nodes(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    assert problem is not None
    report = solved_checker(db, str(problem["id"]))
    assert report.solved is False
    assert "target_theorem" in report.open_nodes


def test_problem_solved_only_when_dag_and_replay_close(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    assert problem is not None
    for key in ("bridge_claim", "local_energy_law", "terminal_form_uniqueness", "replay_closure"):
        db.upsert_frontier_node(
            FrontierNode(
                id=key,
                problem_id=str(problem["id"]),
                node_key=key,
                node_kind="lemma",
                title=key,
                status="proved",
                replay_ref={"replay_certificate": key},
                updated_at=utc_now(),
            )
        )
    target = db.get_frontier_node(str(problem["id"]), "target_theorem")
    assert target is not None
    db.upsert_frontier_node(
        FrontierNode(
            id=str(target["id"]),
            problem_id=str(problem["id"]),
            node_key="target_theorem",
            node_kind="target",
            title="Target theorem",
            statement_md=str(target["statement_md"]),
            formal_statement=str(target["formal_statement"]),
            status="proved",
            dependency_keys=list(target["dependency_keys"]),
            replay_ref={"replay_certificate": "target"},
            updated_at=utc_now(),
        )
    )
    report = solved_checker(db, str(problem["id"]))
    assert report.solved is True
