from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.aristotle import LocalAristotleBackend
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import LimaCoreLoop
from orchestrator.limacore.models import DeltaProposal
from orchestrator.limacore.program import maybe_accept_program_delta, write_candidate_program_delta


def test_one_loop_iteration_commits_good_delta(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    result = loop.run_iteration("inward-compression-conjecture")
    assert result["accepted"] is True
    worlds = db.list_world_heads(str(db.get_problem("inward-compression-conjecture")["id"]))
    assert worlds


def test_one_loop_iteration_reverts_bad_delta(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    bad = DeltaProposal(
        delta_type="lemma_delta",
        title="narrative-only",
        summary_md="narrative-only",
        family_key="other",
        target_node_key="target_theorem",
        edits={
            "bridge_claim": "narrative",
            "local_law": "narrative",
            "kill_test": "narrative",
            "theorem_skeleton": "narrative",
            "obligations": ["narrative"],
        },
    )
    result = loop.run_iteration("collatz", forced_delta=bad)
    assert result["accepted"] is False


def test_program_delta_only_kept_on_verified_yield(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("inward-compression-conjecture")
    assert problem is not None
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    loop.run_iteration(str(problem["id"]))
    candidate = write_candidate_program_delta(db, str(problem["id"]), note="tighten acceptance wording")
    assert maybe_accept_program_delta(db, str(problem["id"]), candidate) is True


def test_collatz_rotates_off_stale_quotient_loop_and_earns_new_replayable_gain(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())

    first = loop.run_iteration("collatz")
    second = loop.run_iteration("collatz")
    third = loop.run_iteration("collatz")
    fourth = loop.run_iteration("collatz")

    assert first["accepted"] is True
    assert second["accepted"] is True
    assert second["score"]["replayable_gain"] > 0
    assert third["accepted"] is False
    assert fourth["accepted"] is False

    worlds = db.list_world_heads(str(db.get_problem("collatz")["id"]))
    families = {str(world["family_key"]) for world in worlds}
    assert "hidden_state" in families

    frontier = db.get_frontier_nodes(str(db.get_problem("collatz")["id"]))
    proved = {str(node["node_key"]) for node in frontier if str(node["status"]) == "proved"}
    assert {"bridge_claim", "local_energy_law"}.issubset(proved)
