from __future__ import annotations

import json
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


LimaMode = Literal["wild", "stress", "forge", "balanced"]
LimaObligationStatus = Literal[
    "queued",
    "checked",
    "falsified",
    "queued_local",
    "running_local",
    "verified_local",
    "refuted_local",
    "queued_formal_review",
    "approved_for_formal",
    "submitted_formal",
    "verified_formal",
    "refuted_formal",
    "inconclusive",
]


_SLUG_RE = re.compile(r"[^a-z0-9_]+")


def slugify(value: Any, *, fallback: str = "universe") -> str:
    raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    slug = _SLUG_RE.sub("_", raw).strip("_")
    return slug[:96] or fallback


def safe_json_loads(raw: Any, default: Any) -> Any:
    if raw is None:
        return default
    if isinstance(raw, (dict, list)):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


class LimaObjectSpec(BaseModel):
    model_config = ConfigDict(extra="allow")

    object_kind: str = Field(default="state_space")
    name: str = Field(default="")
    description_md: str = Field(default="")
    formal_shape: str = Field(default="")
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("object_kind")
    @classmethod
    def normalize_object_kind(cls, value: str) -> str:
        allowed = {
            "state_space",
            "operator",
            "invariant",
            "quotient",
            "completion",
            "grammar",
            "automaton",
            "measure",
            "potential",
            "equivalence",
            "bridge",
        }
        v = slugify(value, fallback="state_space")
        return v if v in allowed else "state_space"


class LimaClaimSpec(BaseModel):
    model_config = ConfigDict(extra="allow")

    claim_kind: str = Field(default="law")
    title: str = Field(default="")
    statement_md: str = Field(default="")
    formal_statement: str = Field(default="")
    status: str = Field(default="open")
    priority: int = Field(default=3, ge=0, le=5)
    depends_on: list[str] = Field(default_factory=list)
    conflicts_with: list[str] = Field(default_factory=list)

    @field_validator("claim_kind")
    @classmethod
    def normalize_claim_kind(cls, value: str) -> str:
        allowed = {
            "law",
            "bridge_lemma",
            "conditional_theorem",
            "finite_prediction",
            "kill_test",
            "negative_constraint",
            "analogy",
            "imported_literature_claim",
        }
        v = slugify(value, fallback="law")
        return v if v in allowed else "law"

    @field_validator("status")
    @classmethod
    def normalize_status(cls, value: str) -> str:
        allowed = {"open", "weakened", "refuted", "survived", "verified"}
        v = slugify(value, fallback="open")
        return v if v in allowed else "open"


class LimaObligationSpec(BaseModel):
    model_config = ConfigDict(extra="allow")

    obligation_kind: str = Field(default="bridge_lemma")
    title: str = Field(default="")
    statement_md: str = Field(default="")
    lean_goal: str = Field(default="")
    status: str = Field(default="queued_local")
    priority: int = Field(default=3, ge=0, le=5)
    why_exists_md: str = Field(default="")
    prove_or_kill_md: str = Field(default="")
    canonical_key: str = Field(default="")
    review_status: str = Field(default="not_reviewed")
    formal_backend: str = Field(default="")

    @field_validator("obligation_kind")
    @classmethod
    def normalize_kind(cls, value: str) -> str:
        allowed = {
            "invariant_check",
            "bridge_lemma",
            "finite_check",
            "equivalence",
            "consistency",
            "counterexample_search",
            "lean_goal",
            "literature_crosscheck",
        }
        v = slugify(value, fallback="bridge_lemma")
        return v if v in allowed else "bridge_lemma"

    @field_validator("status")
    @classmethod
    def normalize_status(cls, value: str) -> str:
        allowed = {
            "queued",
            "checked",
            "falsified",
            "queued_local",
            "running_local",
            "verified_local",
            "refuted_local",
            "queued_formal_review",
            "approved_for_formal",
            "submitted_formal",
            "verified_formal",
            "refuted_formal",
            "inconclusive",
        }
        v = slugify(value, fallback="queued_local")
        if v == "queued":
            return "queued_local"
        if v == "checked":
            return "verified_local"
        if v == "falsified":
            return "refuted_local"
        return v if v in allowed else "queued_local"


class LimaUniverseSpec(BaseModel):
    model_config = ConfigDict(extra="allow")

    title: str
    family_key: str = Field(default="")
    family_kind: str = Field(default="new")
    branch_of_math: str = Field(default="")
    solved_world: str = Field(default="")
    why_problem_is_easy_here: str = Field(default="")
    core_story_md: str = Field(default="")
    core_objects: list[LimaObjectSpec] = Field(default_factory=list)
    laws: list[LimaClaimSpec] = Field(default_factory=list)
    backward_translation: list[str] = Field(default_factory=list)
    bridge_lemmas: list[LimaClaimSpec] = Field(default_factory=list)
    conditional_theorem: LimaClaimSpec | None = None
    kill_tests: list[LimaClaimSpec] = Field(default_factory=list)
    expected_failure_mode: str = Field(default="")
    literature_queries: list[str] = Field(default_factory=list)
    formalization_targets: list[LimaObligationSpec] = Field(default_factory=list)
    scores: dict[str, int | float] = Field(default_factory=dict)

    @field_validator("family_key", mode="after")
    @classmethod
    def default_family_key(cls, value: str, info) -> str:
        if value:
            return slugify(value, fallback="universe")
        title = ""
        if info.data:
            title = str(info.data.get("title") or "")
        return slugify(title, fallback="universe")

    @field_validator("family_kind")
    @classmethod
    def normalize_family_kind(cls, value: str) -> str:
        v = slugify(value, fallback="new")
        return v if v in {"established", "adjacent", "new"} else "new"

    def score(self, key: str, default: int = 3) -> float:
        raw = self.scores.get(key, default)
        try:
            score = float(raw)
        except (TypeError, ValueError):
            score = float(default)
        return max(0.0, min(5.0, score))

    def all_claim_specs(self) -> list[LimaClaimSpec]:
        claims: list[LimaClaimSpec] = []
        claims.extend(self.laws)
        claims.extend(self.bridge_lemmas)
        if self.conditional_theorem:
            claims.append(self.conditional_theorem)
        claims.extend(self.kill_tests)
        return claims


class LimaGenerationResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    frontier_summary_md: str = Field(default="")
    pressure_map: dict[str, Any] = Field(default_factory=dict)
    run_summary_md: str = Field(default="")
    universes: list[LimaUniverseSpec] = Field(default_factory=list)
    policy_notes: list[str] = Field(default_factory=list)


def coerce_lima_generation_response(raw: Any) -> tuple[LimaGenerationResponse, list[str]]:
    warnings: list[str] = []
    if not isinstance(raw, dict):
        warnings.append("response_not_object")
        raw = {}
    if "universes" not in raw and "concepts" in raw:
        raw = dict(raw)
        raw["universes"] = raw.get("concepts")
        warnings.append("concepts_alias_used")
    universes = raw.get("universes")
    if not isinstance(universes, list):
        raw = dict(raw)
        raw["universes"] = []
        warnings.append("universes_not_list")
    parsed = LimaGenerationResponse.model_validate(raw)
    if not parsed.universes:
        warnings.append("no_universes")
    return parsed, warnings
