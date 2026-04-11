from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.artifacts import utc_now
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.models import FrontierNode
from orchestrator.limacore.runtime import detect_runtime_status, persist_runtime_status


def test_status_transitions_running_blocked_stalled_paused_solved(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("inward-compression-conjecture")
    assert problem is not None
    problem_id = str(problem["id"])

    db.update_problem_runtime(problem_id, runtime_status="running", status_reason_md="Running: autopilot active.")
    assert detect_runtime_status(db, problem_id).status == "running"

    db.upsert_frontier_node(
        FrontierNode(
            id="blocked-node",
            problem_id=problem_id,
            node_key="bridge_claim",
            node_kind="bridge_lemma",
            title="Bridge claim",
            status="blocked",
            blocker_kind="missing_bridge",
            blocker_note_md="bridge family is failing",
            priority=11.0,
            updated_at=utc_now(),
        )
    )
    blocked = persist_runtime_status(db, problem_id)
    assert blocked["runtime_status"] == "blocked"

    for idx in range(10):
        db.append_event(
            problem_id,
            "delta_reverted",
            "reverted",
            score_delta={"replayable_gain": 0, "proof_debt_delta": 0, "fracture_gain": 0},
            summary_md=f"reverted {idx}",
        )
    db.upsert_frontier_node(
        FrontierNode(
            id="blocked-node",
            problem_id=problem_id,
            node_key="bridge_claim",
            node_kind="bridge_lemma",
            title="Bridge claim",
            status="proved",
            replay_ref={"replay_certificate": "bridge"},
            priority=11.0,
            updated_at=utc_now(),
        )
    )
    stalled = persist_runtime_status(db, problem_id)
    assert stalled["runtime_status"] == "stalled"

    paused = db.set_autopilot_enabled(problem_id, False)
    assert paused is not None
    assert paused["runtime_status"] == "paused"

    for key in ("bridge_claim", "local_energy_law", "terminal_form_uniqueness", "replay_closure"):
        db.upsert_frontier_node(
            FrontierNode(
                id=key,
                problem_id=problem_id,
                node_key=key,
                node_kind="lemma",
                title=key,
                status="proved",
                replay_ref={"replay_certificate": key},
                updated_at=utc_now(),
            )
        )
    target = db.get_frontier_node(problem_id, "target_theorem")
    assert target is not None
    db.upsert_frontier_node(
        FrontierNode(
            id=str(target["id"]),
            problem_id=problem_id,
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
    solved = persist_runtime_status(db, problem_id)
    assert solved["runtime_status"] == "solved"


def test_detect_runtime_status_prefers_blocked_nodes_over_open_target(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("collatz")
    assert problem is not None
    problem_id = str(problem["id"])

    db.upsert_frontier_node(
        FrontierNode(
            id="blocked-terminal",
            problem_id=problem_id,
            node_key="terminal_form_uniqueness",
            node_kind="theorem_skeleton",
            title="Terminal form uniqueness",
            status="blocked",
            blocker_kind="missing_uniqueness_lemma",
            blocker_note_md="Need a full canonical balanced-profile lemma.",
            priority=7.0,
            updated_at=utc_now(),
        )
    )
    db.replace_world_head(
        problem_id,
        {
            "family_key": "quotient",
            "world_name": "Odd-step quotient probe",
            "status": "surviving",
            "yield_score": 0.32,
            "updated_at": utc_now(),
        },
    )

    status = detect_runtime_status(db, problem_id)
    assert status.status == "blocked"
    assert status.blocked_node_key == "terminal_form_uniqueness"
