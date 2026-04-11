"""Lima-core: a compact, event-sourced autonomous research loop."""

from .aristotle import AristotleBackend, LocalAristotleBackend, RealAristotleBackend
from .cleanup import (
    LegacyCleanupResult,
    cleanup_legacy_collatz_frontier,
    detect_legacy_collatz_frontier_nodes,
    has_legacy_frontier_cleanup_available,
    is_legacy_collatz_frontier_node,
    restart_problem_clean,
)
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
    "LegacyCleanupResult",
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
    "cleanup_legacy_collatz_frontier",
    "derive_frontier_updates",
    "detect_legacy_collatz_frontier_nodes",
    "has_legacy_frontier_cleanup_available",
    "is_legacy_collatz_frontier_node",
    "limacore_loop",
    "make_bridge_node",
    "make_local_law_node",
    "make_replay_node",
    "restart_problem_clean",
]
