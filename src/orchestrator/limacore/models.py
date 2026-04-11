from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


DeltaType = Literal[
    "world_delta",
    "reduction_delta",
    "lemma_delta",
    "kill_delta",
    "program_delta",
]
JobKind = Literal[
    "bridge_lemma",
    "local_law",
    "theorem_skeleton_probe",
    "counterexample_search",
    "equivalence_probe",
    "replay_check",
]
JobVerdict = Literal["proved", "refuted", "blocked", "inconclusive"]


@dataclass(slots=True)
class ProblemSpec:
    id: str
    slug: str
    title: str
    statement_md: str
    domain: str = ""
    status: str = "active"
    target_theorem: str = ""
    original_prompt: str = ""
    normalized_statement_md: str = ""
    runtime_status: str = "booting"
    status_reason_md: str = ""
    blocked_node_key: str = ""
    blocker_kind: str = ""
    stalled_since: str = ""
    last_gain_at: str = ""
    since_timestamp: str = ""
    autopilot_enabled: int = 1
    created_at: str = ""
    updated_at: str = ""


@dataclass(slots=True)
class FrontierNode:
    id: str
    problem_id: str
    node_key: str
    node_kind: str
    title: str
    statement_md: str = ""
    formal_statement: str = ""
    status: str = "open"
    dependency_keys: list[str] = field(default_factory=list)
    blocker_kind: str = ""
    blocker_note_md: str = ""
    best_world_id: str | None = None
    replay_ref: dict[str, Any] = field(default_factory=dict)
    priority: float = 0.0
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class WorldPacket:
    world_name: str
    family_key: str
    new_objects: list[str]
    bridge_to_problem: str
    why_easier_here: str
    local_law: str
    kill_test: str
    theorem_skeleton: str
    formal_agenda: list[str]
    literature_queries: list[str]
    formal_queries: list[str]
    confidence_prior: float
    novelty_note: str

    def validate(self) -> None:
        if not self.new_objects:
            raise ValueError("world packet needs at least one new object")
        if not self.bridge_to_problem.strip():
            raise ValueError("world packet needs a bridge_to_problem")
        if not self.why_easier_here.strip():
            raise ValueError("world packet needs why_easier_here")
        if not self.kill_test.strip():
            raise ValueError("world packet needs kill_test")
        if not any(item.strip() for item in self.formal_agenda):
            raise ValueError("world packet needs at least one formal agenda item")


@dataclass(slots=True)
class GroundingBundle:
    formal_analogs: list[dict[str, Any]] = field(default_factory=list)
    literature_analogs: list[dict[str, Any]] = field(default_factory=list)
    internal_analogs: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class ReductionPacket:
    selected_gap: str
    bridge_claim: str
    local_law: str
    kill_test: str
    theorem_skeleton: str
    obligations: list[str]
    cohort_plan: list[dict[str, Any]]
    rationale_md: str


@dataclass(slots=True)
class AristotleAgenda:
    title: str
    bridge_claim: str
    local_law: str
    kill_test: str
    theorem_skeleton: str
    obligations: list[str]
    job_specs: list["AristotleJobSpec"]


@dataclass(slots=True)
class AristotleJobSpec:
    job_kind: JobKind
    title: str
    frontier_node_key: str
    input_payload: dict[str, Any]


@dataclass(slots=True)
class AristotleJobResult:
    job_kind: JobKind
    verdict: JobVerdict
    replayable: bool
    summary_md: str
    artifact: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CohortSummary:
    id: str
    cohort_kind: str
    title: str
    total_jobs: int
    queued_jobs: int
    running_jobs: int
    succeeded_jobs: int
    failed_jobs: int
    yielded_lemmas: int
    yielded_counterexamples: int
    yielded_blockers: int
    status: str


@dataclass(slots=True)
class FractureRecord:
    family_key: str
    failure_type: str
    smallest_counterexample_ref: dict[str, Any] = field(default_factory=dict)
    blocker_note_md: str = ""
    required_delta_md: str = ""
    ban_level: str = "none"
    repeat_count: int = 0


@dataclass(slots=True)
class ProgramState:
    version: int
    worldsmith_policy_md: str
    retrieval_policy_md: str
    compiler_policy_md: str
    frontier_policy_md: str
    acceptance_policy_md: str
    updated_at: str


@dataclass(slots=True)
class ScoreDelta:
    accepted: bool
    replayable_gain: int
    proof_debt_delta: int
    fracture_gain: int
    novelty_signal: float
    duplication_penalty: float
    summary_md: str


@dataclass(slots=True)
class SolvedReport:
    solved: bool
    reason: str
    open_nodes: list[str]
    replay_passed: bool
    dependency_closure_passed: bool


@dataclass(slots=True)
class DeltaProposal:
    delta_type: DeltaType
    title: str
    summary_md: str
    family_key: str = ""
    world_packet: WorldPacket | None = None
    edits: dict[str, Any] = field(default_factory=dict)
    target_node_key: str = ""
