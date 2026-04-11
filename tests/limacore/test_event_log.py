from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.artifacts import utc_now
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.events import rebuild_materialized_state
from orchestrator.limacore.models import FrontierNode


def test_event_append_and_state_rebuild(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("inward-compression-conjecture")
    assert problem is not None
    node = FrontierNode(
        id="n1",
        problem_id=str(problem["id"]),
        node_key="bridge_claim",
        node_kind="bridge_lemma",
        title="Bridge",
        statement_md="bridge",
        status="proved",
        replay_ref={"replay_certificate": "bridge"},
        updated_at=utc_now(),
    )
    ref = db.store_artifact("frontier_node", node.to_dict())
    db.append_event(str(problem["id"]), "frontier_improved", "accepted", artifact_refs=[ref], summary_md="bridge up")
    rebuild_materialized_state(db, str(problem["id"]))
    rebuilt = db.get_frontier_node(str(problem["id"]), "bridge_claim")
    assert rebuilt is not None
    assert rebuilt["status"] == "proved"


def test_revert_event_leaves_fracture_memory(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    assert problem is not None
    frac = {
        "family_key": "quotient",
        "failure_type": "stale_or_refuted",
        "blocker_note_md": "quotient drift failed",
        "required_delta_md": "change world",
        "repeat_count": 1,
        "updated_at": utc_now(),
    }
    ref = db.store_artifact("fracture_head", frac)
    db.append_event(str(problem["id"]), "delta_reverted", "reverted", artifact_refs=[ref], summary_md="reverted")
    rebuild_materialized_state(db, str(problem["id"]))
    fractures = db.list_fracture_heads(str(problem["id"]))
    assert fractures
    assert fractures[0]["family_key"] == "quotient"
