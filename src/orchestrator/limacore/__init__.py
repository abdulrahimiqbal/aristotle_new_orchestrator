"""Lima-core: a compact, event-sourced autonomous research loop."""

from .aristotle import AristotleBackend, LocalAristotleBackend, RealAristotleBackend
from .db import LimaCoreDB
from .loop import LimaCoreLoop
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
    "FractureRecord",
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
]
