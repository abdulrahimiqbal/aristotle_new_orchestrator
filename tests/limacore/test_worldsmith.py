from __future__ import annotations

from pathlib import Path

import pytest

from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.models import ProblemSpec, WorldPacket
from orchestrator.limacore.worldsmith import WORLD_FAMILY_LIBRARY, Worldsmith


def test_world_packets_validate_and_emit_structure(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = ProblemSpec(**db.get_problem("inward-compression-conjecture"))
    gap = db.get_frontier_node(problem.id, "target_theorem")
    assert gap is not None
    proposal = Worldsmith().propose_world(problem, gap)
    assert proposal.world_packet is not None
    assert proposal.world_packet.family_key == "balancing_world"
    assert proposal.world_packet.formal_agenda


def test_invalid_world_is_rejected() -> None:
    packet = WorldPacket(
        world_name="bad",
        family_key="other",
        new_objects=[],
        bridge_to_problem="",
        why_easier_here="",
        local_law="law",
        kill_test="",
        theorem_skeleton="skel",
        formal_agenda=[],
        literature_queries=[],
        formal_queries=[],
        confidence_prior=0.1,
        novelty_note="",
    )
    with pytest.raises(ValueError):
        packet.validate()


def test_family_heuristics_library_present() -> None:
    assert "balancing_world" in WORLD_FAMILY_LIBRARY
    assert "coordinate_lift" in WORLD_FAMILY_LIBRARY
