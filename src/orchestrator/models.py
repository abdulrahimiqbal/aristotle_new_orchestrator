from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class CampaignStatus(str, Enum):
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


class TargetStatus(str, Enum):
    OPEN = "open"
    VERIFIED = "verified"
    REFUTED = "refuted"
    BLOCKED = "blocked"


class ExperimentStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Verdict(str, Enum):
    PROVED = "proved"
    PARTIAL = "partial"
    DISPROVED = "disproved"
    INCONCLUSIVE = "inconclusive"
    INFRA_ERROR = "infra_error"


class LedgerStatus(str, Enum):
    PROVED = "proved"
    ATTEMPTED = "attempted"
    BLOCKED = "blocked"


class Campaign(BaseModel):
    id: str
    prompt: str
    status: CampaignStatus = CampaignStatus.ACTIVE
    workspace_dir: str = ""
    workspace_template: str = "minimal"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    problem_map_json: str = "{}"
    problem_refs_json: str = "{}"
    # Per-campaign: LeanSearch hints for the manager (requires MATHLIB_KNOWLEDGE_MODE=leansearch on server).
    mathlib_knowledge: bool = False


class Target(BaseModel):
    id: str
    campaign_id: str
    description: str
    status: TargetStatus = TargetStatus.OPEN
    evidence: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class Experiment(BaseModel):
    id: str
    campaign_id: str
    target_id: str
    objective: str
    move_kind: str = "prove"
    move_note: str = ""
    status: ExperimentStatus = ExperimentStatus.PENDING
    aristotle_job_id: Optional[str] = None
    result_raw: Optional[str] = None
    result_summary: Optional[str] = None
    verdict: Optional[Verdict] = None
    submitted_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    parsed_proved_lemmas: list[str] = Field(default_factory=list)
    parsed_generated_lemmas: list[str] = Field(default_factory=list)
    parsed_unsolved_goals: list[str] = Field(default_factory=list)
    parsed_blockers: list[str] = Field(default_factory=list)
    parsed_counterexamples: list[str] = Field(default_factory=list)
    parsed_error_message: str = ""
    result_structured_json: str = ""
    parse_schema_version: int = 0
    parse_source: str = ""
    parse_warnings: list[str] = Field(default_factory=list)


class Tick(BaseModel):
    id: Optional[int] = None
    campaign_id: str
    tick_number: int
    reasoning: str
    actions: dict = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class TargetUpdate(BaseModel):
    target_id: str
    new_status: TargetStatus
    evidence: str = ""


class NewExperiment(BaseModel):
    target_id: str
    objective: str
    move_kind: str = "prove"
    move_note: str = ""


class ManagerDecision(BaseModel):
    """What the LLM decides to do each tick."""

    reasoning: str
    target_updates: list[TargetUpdate] = Field(default_factory=list)
    new_experiments: list[NewExperiment] = Field(default_factory=list)
    campaign_complete: bool = False
    campaign_complete_reason: str = ""


class AristotleParsedResult(BaseModel):
    """Structured result parsed from Aristotle output."""

    verdict: Verdict = Verdict.INCONCLUSIVE
    proved_lemmas: list[str] = Field(default_factory=list)
    generated_lemmas: list[str] = Field(default_factory=list)
    unsolved_goals: list[str] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    counterexamples: list[str] = Field(default_factory=list)
    error_message: str = ""
    summary_text: str = ""
    parse_source: str = "markdown"
    parse_schema_version: int | None = None
    parse_warnings: list[str] = Field(default_factory=list)


class LedgerEntry(BaseModel):
    id: str
    campaign_id: str
    target_id: str
    experiment_id: str
    label: str
    status: LedgerStatus
    detail: str = ""
    created_at: datetime = Field(default_factory=datetime.utcnow)


class CampaignState(BaseModel):
    """Full state passed to LLM for reasoning."""

    campaign: Campaign
    targets: list[Target]
    experiments: list[Experiment]
    recent_ticks: list[Tick] = Field(default_factory=list)
    manager_context_experiments: list[dict[str, Any]] = Field(default_factory=list)
    manager_context_experiments_by_target: dict[str, list[dict[str, Any]]] = Field(
        default_factory=dict
    )
    manager_context_ledger: list[dict[str, Any]] = Field(default_factory=list)
    # Filled by manager when MATHLIB_KNOWLEDGE_MODE=leansearch (LeanSearch-backed hints).
    mathlib_broad_markdown: str = ""
    mathlib_narrow_markdown: str = ""
