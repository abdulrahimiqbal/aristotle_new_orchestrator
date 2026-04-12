from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass
from typing import Any

import httpx

from orchestrator import config as app_config
from orchestrator.llm import LLMDisabledError, invoke_llm

from .control import ControlSnapshot
from .models import DeltaProposal, ManagerMode, ProblemSpec
from .program import get_program_state
from .unblock_manager import UnblockManager, UnblockSuggestion
from .worldsmith import WORLD_FAMILY_LIBRARY, Worldsmith


MANAGER_SYSTEM_PROMPT = """You are the Lima-core research manager.

Your job is to choose the next bounded research move that maximizes the chance of replayable formal progress.

You are not proving the theorem directly.
You are not mutating the database.
You are not deciding whether progress is real.
You are directing the next move for the existing Lima-core execution spine.

You must reason from:
- problem statement
- current frontier gap
- current line
- strongest surviving worlds
- recent fractures
- recent accepted/reverted attempts
- repeated cohort patterns
- runtime/control snapshot
- backend capability hints
- mode

Your output must be a bounded, structured plan.
You must not output narrative-only ideas.
You must not retry a stale line unless there is a material change.

Modes:
- bootstrap: choose the first serious line and backup candidates
- explore: choose the next bounded move on a healthy search
- unblock: recover from blocked/stalled/exhausted state
- repair: stay in the same family only if the move is materially different
- improve_program: propose a tiny patch to manager policy that would have improved verified yield

Rules:
1. Prefer moves that can produce replayable formal structure.
2. A same-family repair is allowed only if it materially changes the required delta, theorem skeleton, bridge claim, local law, or kill test.
3. When blocked or stalled, generate successor moves that materially change trajectory.
4. Avoid exhausted families unless the frontier context changed materially.
5. Prefer capability-aware moves: do not rotate into families that recently only produced generic blockers unless there is a strong reason.
6. Keep every chosen move small enough to compile into a compact Aristotle agenda.
7. When suggesting a program patch, change policy only; do not change proof truth criteria.

Return JSON only.

Schema:

{
  "mode": "bootstrap|explore|unblock|repair|improve_program",
  "reason_md": "short explanation of why this is the right next move",
  "strategy_kind": "bootstrap|repair|neighbor_family|orthogonal_family|frontier_shift|program_patch",
  "current_line": {
    "family_key": "...",
    "frontier_node_key": "...",
    "blocker_kind": "...",
    "blocker_summary": "..."
  },
  "candidates": [
    {
      "strategy_kind": "repair|neighbor_family|orthogonal_family|frontier_shift",
      "material_difference_md": "...",
      "delta": {
        "delta_type": "world_delta|reduction_delta|lemma_delta|kill_delta|program_delta",
        "title": "...",
        "summary_md": "...",
        "family_key": "...",
        "target_node_key": "...",
        "edits": {
          "bridge_claim": "...",
          "local_law": "...",
          "kill_test": "...",
          "theorem_skeleton": "...",
          "required_delta_md": "...",
          "obligations": ["...", "..."]
        }
      }
    }
  ],
  "chosen_index": 0,
  "confidence": 0.0,
  "expected_frontier_change": "...",
  "program_patch": {
    "reason_md": "...",
    "patch": {}
  }
}

Constraints:
- Return at most 3 candidates.
- Return exactly one chosen_index when candidates are present.
- Every candidate must be materially different from the stale current line.
- Do not use problem-specific hardcoded shortcuts.
- Choose the move most likely to change frontier trajectory, not the move that sounds most impressive."""


@dataclass(slots=True)
class ManagerInput:
    problem: ProblemSpec
    current_gap: dict[str, Any]
    control_snapshot: ControlSnapshot
    strongest_worlds: list[dict[str, Any]]
    recent_fractures: list[dict[str, Any]]
    recent_events: list[dict[str, Any]]
    recent_cohort_signatures: list[str]
    backend_capability_hints: dict[str, Any]
    mode: ManagerMode
    family_library: dict[str, str]
    runtime_status: str
    current_line_key: str
    repeated_current_line_pattern: bool
    current_program: dict[str, Any]

    def to_prompt_payload(self) -> dict[str, Any]:
        snapshot = self.control_snapshot
        return {
            "mode": self.mode,
            "problem": {
                "id": self.problem.id,
                "slug": self.problem.slug,
                "title": self.problem.title,
                "statement_md": self.problem.statement_md,
                "runtime_status": self.runtime_status,
            },
            "current_gap": self.current_gap,
            "current_line_key": self.current_line_key,
            "current_line": {
                "family_key": snapshot.current_family_key,
                "frontier_node_key": snapshot.current_line_node_key or snapshot.blocked_node_key,
                "blocker_kind": snapshot.blocker_kind,
                "blocker_summary": snapshot.blocker_summary,
            },
            "control_snapshot": {
                "blocked_node_key": snapshot.blocked_node_key,
                "current_family_key": snapshot.current_family_key,
                "current_line_exhausted": snapshot.current_line_exhausted,
                "recent_current_line_replayable_gain": snapshot.recent_current_line_replayable_gain,
                "recent_current_line_accepts": snapshot.recent_current_line_accepts,
                "recent_current_line_reverts": snapshot.recent_current_line_reverts,
                "recent_current_line_failed_jobs": snapshot.recent_current_line_failed_jobs,
                "recent_current_line_failed_cohorts": snapshot.recent_current_line_failed_cohorts,
                "recent_current_line_repeated_signature_count": snapshot.recent_current_line_repeated_signature_count,
                "current_required_delta_md": snapshot.current_required_delta_md,
                "current_theorem_skeleton_md": snapshot.current_theorem_skeleton_md,
            },
            "strongest_worlds": self.strongest_worlds[:5],
            "recent_fractures": self.recent_fractures[:5],
            "recent_events": [
                {
                    "event_type": str(event.get("event_type") or ""),
                    "decision": str(event.get("decision") or ""),
                    "family_key": str(event.get("family_key") or ""),
                    "score_delta": event.get("score_delta") or {},
                    "summary_md": str(event.get("summary_md") or "")[:200],
                }
                for event in self.recent_events[-12:]
            ],
            "recent_cohort_signatures": self.recent_cohort_signatures[-8:],
            "backend_capability_hints": self.backend_capability_hints,
            "family_library": self.family_library,
            "repeated_current_line_pattern": self.repeated_current_line_pattern,
            "current_program": self.current_program,
        }


@dataclass(slots=True)
class ManagerCandidate:
    strategy_kind: str
    material_difference_md: str
    delta: DeltaProposal


@dataclass(slots=True)
class ManagerProgramPatch:
    reason_md: str
    patch: dict[str, Any]


@dataclass(slots=True)
class ManagerPlan:
    mode: str
    reason_md: str
    strategy_kind: str
    current_line: dict[str, str]
    candidates: tuple[ManagerCandidate, ...]
    chosen_index: int
    confidence: float
    expected_frontier_change: str
    program_patch: ManagerProgramPatch | None = None
    provider: str = "deterministic"

    @property
    def chosen_delta(self) -> DeltaProposal | None:
        if self.chosen_index < 0 or self.chosen_index >= len(self.candidates):
            return None
        return self.candidates[self.chosen_index].delta

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "reason_md": self.reason_md,
            "strategy_kind": self.strategy_kind,
            "current_line": self.current_line,
            "candidates": [
                {
                    "strategy_kind": candidate.strategy_kind,
                    "material_difference_md": candidate.material_difference_md,
                    "delta": asdict(candidate.delta),
                }
                for candidate in self.candidates
            ],
            "chosen_index": self.chosen_index,
            "confidence": self.confidence,
            "expected_frontier_change": self.expected_frontier_change,
            "program_patch": (
                {"reason_md": self.program_patch.reason_md, "patch": self.program_patch.patch}
                if self.program_patch
                else None
            ),
            "provider": self.provider,
        }


class ManagerCore:
    def __init__(
        self,
        *,
        worldsmith: Worldsmith | None = None,
        unblock_manager: UnblockManager | None = None,
    ) -> None:
        self.worldsmith = worldsmith or Worldsmith()
        self.unblock_manager = unblock_manager or UnblockManager(worldsmith=self.worldsmith)

    def plan(self, manager_input: ManagerInput) -> ManagerPlan:
        llm_plan = self._plan_with_llm(manager_input)
        if llm_plan is not None:
            return llm_plan
        return self._plan_deterministic(manager_input)

    def _plan_with_llm(self, manager_input: ManagerInput) -> ManagerPlan | None:
        if not app_config.LLM_API_KEY or app_config.LLM_DISABLED:
            return None
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                return None
        except RuntimeError:
            pass
        prompt_payload = manager_input.to_prompt_payload()
        try:
            raw = asyncio.run(
                invoke_llm(
                    MANAGER_SYSTEM_PROMPT,
                    json.dumps(prompt_payload, ensure_ascii=False),
                    json_object=True,
                    temperature=0.2,
                )
            )
        except (LLMDisabledError, httpx.HTTPError, RuntimeError, ValueError):
            return None

        if not isinstance(raw, str) or not raw.strip():
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return self._parse_manager_plan(payload, manager_input, provider="llm")

    def _plan_deterministic(self, manager_input: ManagerInput) -> ManagerPlan:
        mode = manager_input.mode
        snapshot = manager_input.control_snapshot
        problem = manager_input.problem
        gap = manager_input.current_gap

        if mode in {"unblock", "repair"}:
            unblock = self.unblock_manager.suggest(
                problem=problem,
                gap=gap,
                control_snapshot=snapshot,
                strongest_worlds=manager_input.strongest_worlds,
                recent_fractures=manager_input.recent_fractures,
                recent_events=manager_input.recent_events,
            )
            candidates = tuple(
                ManagerCandidate(
                    strategy_kind=item.strategy_kind,
                    material_difference_md=item.material_difference_md,
                    delta=item.delta,
                )
                for item in unblock.candidates
            )
            return ManagerPlan(
                mode=mode,
                reason_md=unblock.reason_md,
                strategy_kind=unblock.strategy_kind,
                current_line={
                    "family_key": unblock.current_family,
                    "frontier_node_key": unblock.current_frontier_node,
                    "blocker_kind": snapshot.blocker_kind,
                    "blocker_summary": snapshot.blocker_summary,
                },
                candidates=candidates[:3],
                chosen_index=unblock.chosen_index if candidates else -1,
                confidence=0.64 if candidates else 0.2,
                expected_frontier_change="Rotate away from blocked line and probe alternative replayable structure.",
                provider="deterministic",
            )

        if mode == "improve_program":
            note = (
                f"Manager patch: increase rotation pressure when repeated signatures >= "
                f"{max(2, snapshot.recent_current_line_repeated_signature_count)} and replayable_gain<=0."
            )
            delta = DeltaProposal(
                delta_type="program_delta",
                title="Manager policy patch candidate",
                summary_md=note,
                family_key=snapshot.current_family_key,
                target_node_key=snapshot.current_line_node_key or str(gap.get("node_key") or ""),
                edits={"manager_note": note},
            )
            candidate = ManagerCandidate(
                strategy_kind="frontier_shift",
                material_difference_md="Policy-only patch to improve family rotation and stale-pattern handling.",
                delta=delta,
            )
            return ManagerPlan(
                mode=mode,
                reason_md="Recent line has low replayable gain; propose a bounded policy patch to improve yield.",
                strategy_kind="program_patch",
                current_line={
                    "family_key": snapshot.current_family_key,
                    "frontier_node_key": snapshot.current_line_node_key,
                    "blocker_kind": snapshot.blocker_kind,
                    "blocker_summary": snapshot.blocker_summary,
                },
                candidates=(candidate,),
                chosen_index=0,
                confidence=0.55,
                expected_frontier_change="Program policy changes may reduce stale retries over the next rolling window.",
                program_patch=ManagerProgramPatch(
                    reason_md="Observed repeated low-gain patterns; prefer earlier rotation when signatures repeat.",
                    patch={
                        "rotation_bias": "increase",
                        "repeated_signature_threshold": max(2, snapshot.recent_current_line_repeated_signature_count),
                        "mode": "improve_program",
                    },
                ),
                provider="deterministic",
            )

        # bootstrap/explore deterministic plan
        preferred_family = self._best_family_from_hints(manager_input)
        chosen_world = self.worldsmith.propose_world(
            problem,
            gap,
            preferred_family_key=preferred_family,
            avoid_family_keys={snapshot.exhausted_family_key} if snapshot.exhausted_family_key else set(),
        )
        candidates: list[ManagerCandidate] = [
            ManagerCandidate(
                strategy_kind="bootstrap" if mode == "bootstrap" else "frontier_shift",
                material_difference_md=f"Start bounded search in `{chosen_world.family_key}` with compile-ready agenda.",
                delta=chosen_world,
            )
        ]
        # backup candidates
        alt_families = [
            family for family in WORLD_FAMILY_LIBRARY if family and family != chosen_world.family_key
        ][:2]
        for family in alt_families:
            proposal = self.worldsmith.propose_world(
                problem,
                gap,
                preferred_family_key=family,
                avoid_family_keys={chosen_world.family_key},
            )
            candidates.append(
                ManagerCandidate(
                    strategy_kind="neighbor_family" if len(candidates) == 1 else "orthogonal_family",
                    material_difference_md=f"Alternative bounded line in `{proposal.family_key}`.",
                    delta=proposal,
                )
            )

        return ManagerPlan(
            mode=mode,
            reason_md=(
                "Bootstrap a first serious line with bounded agenda and backup families."
                if mode == "bootstrap"
                else "Explore with bounded candidate lines prioritizing recent capability signal."
            ),
            strategy_kind="bootstrap" if mode == "bootstrap" else "frontier_shift",
            current_line={
                "family_key": snapshot.current_family_key,
                "frontier_node_key": snapshot.current_line_node_key or str(gap.get("node_key") or ""),
                "blocker_kind": snapshot.blocker_kind,
                "blocker_summary": snapshot.blocker_summary,
            },
            candidates=tuple(candidates[:3]),
            chosen_index=0,
            confidence=0.58,
            expected_frontier_change="Introduce a fresh bounded agenda with backup world options.",
            provider="deterministic",
        )

    def _best_family_from_hints(self, manager_input: ManagerInput) -> str:
        hints = manager_input.backend_capability_hints.get("family_metrics", {})
        best_family = ""
        best_score = float("-inf")
        for family, metrics in hints.items():
            replay = int(metrics.get("recent_replayable_gain", 0))
            failed = int(metrics.get("recent_failed_cohorts", 0))
            yielded = int(metrics.get("recent_yielded_lemmas", 0))
            score = replay + yielded - failed
            if score > best_score:
                best_score = score
                best_family = family
        return best_family or manager_input.control_snapshot.suggested_family_key or "coordinate_lift"

    def _parse_manager_plan(
        self,
        payload: dict[str, Any],
        manager_input: ManagerInput,
        *,
        provider: str,
    ) -> ManagerPlan | None:
        if not isinstance(payload, dict):
            return None
        raw_candidates = payload.get("candidates")
        if not isinstance(raw_candidates, list):
            return None
        candidates: list[ManagerCandidate] = []
        for item in raw_candidates[:3]:
            if not isinstance(item, dict):
                continue
            delta_payload = item.get("delta")
            if not isinstance(delta_payload, dict):
                continue
            try:
                delta = DeltaProposal(
                    delta_type=str(delta_payload.get("delta_type") or "lemma_delta"),  # type: ignore[arg-type]
                    title=str(delta_payload.get("title") or "Manager candidate"),
                    summary_md=str(delta_payload.get("summary_md") or ""),
                    family_key=str(delta_payload.get("family_key") or ""),
                    target_node_key=str(
                        delta_payload.get("target_node_key")
                        or manager_input.current_gap.get("node_key")
                        or ""
                    ),
                    edits=dict(delta_payload.get("edits") or {}),
                )
            except Exception:
                continue
            candidates.append(
                ManagerCandidate(
                    strategy_kind=str(item.get("strategy_kind") or "frontier_shift"),
                    material_difference_md=str(item.get("material_difference_md") or ""),
                    delta=delta,
                )
            )
        if not candidates:
            return None
        chosen_index = int(payload.get("chosen_index", 0))
        if chosen_index < 0 or chosen_index >= len(candidates):
            chosen_index = 0
        mode = str(payload.get("mode") or manager_input.mode)
        reason_md = str(payload.get("reason_md") or "Manager selected bounded next move.")
        strategy_kind = str(payload.get("strategy_kind") or candidates[chosen_index].strategy_kind)
        current_line = payload.get("current_line")
        if not isinstance(current_line, dict):
            current_line = {
                "family_key": manager_input.control_snapshot.current_family_key,
                "frontier_node_key": manager_input.control_snapshot.current_line_node_key,
                "blocker_kind": manager_input.control_snapshot.blocker_kind,
                "blocker_summary": manager_input.control_snapshot.blocker_summary,
            }
        confidence = float(payload.get("confidence", 0.5) or 0.5)
        expected_frontier_change = str(payload.get("expected_frontier_change") or "")
        program_patch_payload = payload.get("program_patch")
        program_patch = None
        if isinstance(program_patch_payload, dict):
            reason = str(program_patch_payload.get("reason_md") or "").strip()
            patch = program_patch_payload.get("patch") or {}
            if isinstance(patch, dict):
                program_patch = ManagerProgramPatch(reason_md=reason, patch=patch)
        return ManagerPlan(
            mode=mode,
            reason_md=reason_md,
            strategy_kind=strategy_kind,
            current_line={
                "family_key": str(current_line.get("family_key") or ""),
                "frontier_node_key": str(current_line.get("frontier_node_key") or ""),
                "blocker_kind": str(current_line.get("blocker_kind") or ""),
                "blocker_summary": str(current_line.get("blocker_summary") or ""),
            },
            candidates=tuple(candidates),
            chosen_index=chosen_index,
            confidence=max(0.0, min(1.0, confidence)),
            expected_frontier_change=expected_frontier_change,
            program_patch=program_patch,
            provider=provider,
        )


def build_backend_capability_hints(
    *,
    strongest_worlds: list[dict[str, Any]],
    recent_fractures: list[dict[str, Any]],
    recent_events: list[dict[str, Any]],
    recent_cohorts: list[dict[str, Any]],
) -> dict[str, Any]:
    by_family: dict[str, dict[str, int]] = {}

    def row(family: str) -> dict[str, int]:
        if family not in by_family:
            by_family[family] = {
                "recent_replayable_gain": 0,
                "recent_failed_cohorts": 0,
                "recent_yielded_lemmas": 0,
                "recent_reverts": 0,
            }
        return by_family[family]

    for event in recent_events[-20:]:
        family = str(event.get("family_key") or "")
        if not family:
            continue
        slot = row(family)
        score = event.get("score_delta") or {}
        slot["recent_replayable_gain"] += int(score.get("replayable_gain", 0) or 0)
        if str(event.get("decision") or "") == "reverted":
            slot["recent_reverts"] += 1

    for cohort in recent_cohorts[-20:]:
        world_id = str(cohort.get("world_id") or "")
        family = world_id.split(":")[-1] if ":" in world_id else ""
        if not family:
            continue
        slot = row(family)
        slot["recent_failed_cohorts"] += 1 if int(cohort.get("failed_jobs") or 0) > 0 else 0
        slot["recent_yielded_lemmas"] += int(cohort.get("yielded_lemmas") or 0)

    fracture_bans = {
        str(row_item.get("family_key") or ""): str(row_item.get("ban_level") or "")
        for row_item in recent_fractures
        if str(row_item.get("family_key") or "")
    }
    strong_families = [
        str(world.get("family_key") or "")
        for world in strongest_worlds
        if str(world.get("family_key") or "")
    ]
    return {
        "family_metrics": by_family,
        "fracture_bans": fracture_bans,
        "strong_families": strong_families,
    }


def collect_recent_cohort_signatures(
    *,
    problem_id: str,
    cohorts: list[dict[str, Any]],
) -> list[str]:
    signatures: list[str] = []
    for cohort in cohorts[-12:]:
        world_id = str(cohort.get("world_id") or "")
        family = world_id.split(":")[-1] if ":" in world_id else ""
        signatures.append(
            "|".join(
                [
                    family,
                    str(cohort.get("title") or ""),
                    str(int(cohort.get("yielded_lemmas") or 0)),
                    str(int(cohort.get("failed_jobs") or 0)),
                    str(int(cohort.get("succeeded_jobs") or 0)),
                ]
            )
        )
    return signatures


def build_manager_input(
    *,
    problem: ProblemSpec,
    current_gap: dict[str, Any],
    control_snapshot: ControlSnapshot,
    strongest_worlds: list[dict[str, Any]],
    recent_fractures: list[dict[str, Any]],
    recent_events: list[dict[str, Any]],
    recent_cohorts: list[dict[str, Any]],
    mode: ManagerMode,
    runtime_status: str,
    current_program: dict[str, Any],
) -> ManagerInput:
    capability_hints = build_backend_capability_hints(
        strongest_worlds=strongest_worlds,
        recent_fractures=recent_fractures,
        recent_events=recent_events,
        recent_cohorts=recent_cohorts,
    )
    return ManagerInput(
        problem=problem,
        current_gap=current_gap,
        control_snapshot=control_snapshot,
        strongest_worlds=strongest_worlds,
        recent_fractures=recent_fractures,
        recent_events=recent_events,
        recent_cohort_signatures=collect_recent_cohort_signatures(
            problem_id=problem.id,
            cohorts=recent_cohorts,
        ),
        backend_capability_hints=capability_hints,
        mode=mode,
        family_library=dict(WORLD_FAMILY_LIBRARY),
        runtime_status=runtime_status,
        current_line_key=control_snapshot.current_line_key,
        repeated_current_line_pattern=control_snapshot.repeated_cohort_pattern_detected,
        current_program=current_program,
    )


def default_program_payload(problem_id: str, *, db: Any) -> dict[str, Any]:
    try:
        program = get_program_state(db, problem_id)
        return asdict(program)
    except Exception:
        return {}
