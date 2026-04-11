from __future__ import annotations

from pathlib import Path

import pytest

from orchestrator.limacore.compiler import CompileError, compile_delta_to_reduction
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.models import DeltaProposal, GroundingBundle
from orchestrator.limacore.worldsmith import Worldsmith


def test_delta_compiles_into_tiny_aristotle_agenda(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    problem = db.get_problem("inward-compression-conjecture")
    assert problem is not None
    gap = db.get_frontier_node(str(problem["id"]), "target_theorem")
    assert gap is not None
    proposal = Worldsmith().propose_world(type("P", (), problem), gap)
    reduction, agenda = compile_delta_to_reduction(gap, proposal, GroundingBundle())
    assert reduction.bridge_claim
    assert len(agenda.job_specs) == 4
    assert len(reduction.obligations) <= 3


def test_invalid_delta_fails_early() -> None:
    with pytest.raises(CompileError):
        compile_delta_to_reduction(
            {"node_key": "x"},
            DeltaProposal(delta_type="lemma_delta", title="bad", summary_md="bad", edits={}),
            GroundingBundle(),
        )
