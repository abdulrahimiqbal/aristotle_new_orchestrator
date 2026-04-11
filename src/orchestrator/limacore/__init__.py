"""Lima-core: a compact, event-sourced autonomous research loop."""

from .aristotle import AristotleBackend, LocalAristotleBackend, RealAristotleBackend
from .db import LimaCoreDB
from .frontier_derivation import (
    DerivedFrontier,
    FrontierHint,
    derive_frontier_updates,
    make_bridge_node,
    make_local_law_node,
    make_replay_node,
)
from .loop import LimaCoreLoop, _scheduler_pass, limacore_loop
from .models import (
    AristotleAgenda,
    AristotleJobResult,
    AristotleJobSpec,
    CohortSummary,
    FractureRecord,
    FrontierNode,
    GroundingBundle,
    ProblemSpec,
    ProgramState,
    ReductionPacket,
    ScoreDelta,
    SolvedReport,
    WorldPacket,
)

__all__ = [
    "AristotleAgenda",
    "AristotleBackend",
    "AristotleJobResult",
    "AristotleJobSpec",
    "CohortSummary",
    "DerivedFrontier",
    "FractureRecord",
    "FrontierHint",
    "FrontierNode",
    "GroundingBundle",
    "LimaCoreDB",
    "LimaCoreLoop",
    "LocalAristotleBackend",
    "ProblemSpec",
    "ProgramState",
    "RealAristotleBackend",
    "ReductionPacket",
    "ScoreDelta",
    "SolvedReport",
    "WorldPacket",
    "_scheduler_pass",
    "derive_frontier_updates",
    "limacore_loop",
    "make_bridge_node",
    "make_local_law_node",
    "make_replay_node",
]
