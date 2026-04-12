from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.aristotle import LocalAristotleBackend
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import LimaCoreLoop
from orchestrator.limacore.solved import solved_checker


def test_inward_compression_benchmark_progresses_without_false_solve(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    problem = db.get_problem("inward-compression-conjecture")
    assert problem is not None

    results = [loop.run_iteration("inward-compression-conjecture") for _ in range(3)]
    assert any(result["accepted"] for result in results)

    worlds = db.list_world_heads(str(problem["id"]))
    assert worlds

    frontier = db.get_frontier_nodes(str(problem["id"]))
    keys = {node["node_key"] for node in frontier}
    assert {"bridge_claim", "local_energy_law", "terminal_form_uniqueness"}.issubset(keys)

    fractures = db.list_fracture_heads(str(problem["id"]))
    assert frontier or fractures

    report = solved_checker(db, str(problem["id"]))
    assert report.solved is False
