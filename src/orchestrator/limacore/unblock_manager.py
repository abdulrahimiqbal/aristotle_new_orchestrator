from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .control import (
    ControlSnapshot,
    materially_changed_required_delta,
    materially_changed_theorem_skeleton,
)
from .models import DeltaProposal, ProblemSpec
from .worldsmith import WORLD_FAMILY_LIBRARY, Worldsmith


UNBLOCK_TRIGGER_STATUSES = {"blocked", "stalled"}

FAMILY_NEIGHBORS: dict[str, tuple[str, ...]] = {
    "hidden_state": ("cocycle", "operator_world", "coordinate_lift"),
    "cocycle": ("hidden_state", "operator_world", "symbolic_dynamics"),
    "operator_world": ("hidden_state", "order_or_convexity", "coordinate_lift"),
    "coordinate_lift": ("operator_world", "balancing_world", "order_or_convexity"),
    "balancing_world": ("order_or_convexity", "coordinate_lift", "graph_or_rewrite"),
    "order_or_convexity": ("balancing_world", "operator_world", "graph_or_rewrite"),
    "graph_or_rewrite": ("symbolic_dynamics", "order_or_convexity", "coordinate_lift"),
    "symbolic_dynamics": ("graph_or_rewrite", "cocycle", "other"),
    "quotient": ("hidden_state", "cocycle", "operator_world"),
    "other": ("coordinate_lift", "operator_world", "hidden_state"),
}


@dataclass(slots=True)
class UnblockCandidate:
    strategy_kind: str
    material_difference_md: str
    delta: DeltaProposal
    rank_score: float = 0.0


@dataclass(slots=True)
class UnblockSuggestion:
    reason_md: str
    strategy_kind: str
    current_family: str
    current_frontier_node: str
    suggested_family: str
    blocked_node_key: str
    candidates: tuple[UnblockCandidate, ...]
    chosen_index: int = -1

    @property
    def chosen_delta(self) -> DeltaProposal | None:
        if self.chosen_index < 0 or self.chosen_index >= len(self.candidates):
            return None
        return self.candidates[self.chosen_index].delta

    def to_dict(self) -> dict[str, Any]:
        return {
            "reason_md": self.reason_md,
            "strategy_kind": self.strategy_kind,
            "current_family": self.current_family,
            "current_frontier_node": self.current_frontier_node,
            "suggested_family": self.suggested_family,
            "blocked_node_key": self.blocked_node_key,
            "chosen_index": self.chosen_index,
            "candidates": [
                {
                    "strategy_kind": candidate.strategy_kind,
                    "material_difference_md": candidate.material_difference_md,
                    "rank_score": candidate.rank_score,
                    "delta": asdict(candidate.delta),
                }
                for candidate in self.candidates
            ],
        }


class UnblockManager:
    """Small, deterministic unblock planner for blocked/stalled/exhausted lines."""

    def __init__(self, *, worldsmith: Worldsmith | None = None) -> None:
        self.worldsmith = worldsmith or Worldsmith()

    def should_activate(self, problem: ProblemSpec, snapshot: ControlSnapshot) -> bool:
        return bool(
            problem.runtime_status in UNBLOCK_TRIGGER_STATUSES
            or snapshot.current_line_exhausted
            or snapshot.current_family_exhausted
        )

    def suggest(
        self,
        *,
        problem: ProblemSpec,
        gap: dict[str, Any],
        control_snapshot: ControlSnapshot,
        strongest_worlds: list[dict[str, Any]],
        recent_fractures: list[dict[str, Any]],
        recent_events: list[dict[str, Any]] | None = None,
    ) -> UnblockSuggestion:
        current_family = control_snapshot.current_family_key
        blocked_node_key = control_snapshot.blocked_node_key or str(gap.get("node_key") or "")
        exhausted_families = self._exhausted_families(control_snapshot, recent_fractures)
        strong_families = tuple(
            str(world.get("family_key") or "")
            for world in strongest_worlds
            if str(world.get("status") or "") in {"surviving", "boot_candidate", "proposed"}
            and str(world.get("family_key") or "")
        )
        repeated_signature = str(control_snapshot.repeated_cohort_signature or "").strip()
        event_families = tuple(
            str(event.get("family_key") or "")
            for event in (recent_events or [])
            if str(event.get("family_key") or "")
        )

        candidates: list[UnblockCandidate] = []

        repair = self._build_repair_candidate(
            problem=problem,
            gap=gap,
            snapshot=control_snapshot,
            blocked_node_key=blocked_node_key,
            recent_fractures=recent_fractures,
        )
        if repair is not None:
            candidates.append(repair)

        neighbor = self._build_neighbor_family_candidate(
            problem=problem,
            gap=gap,
            snapshot=control_snapshot,
            strong_families=strong_families,
            exhausted_families=exhausted_families,
        )
        if neighbor is not None:
            candidates.append(neighbor)

        orthogonal = self._build_orthogonal_family_candidate(
            problem=problem,
            gap=gap,
            snapshot=control_snapshot,
            strong_families=strong_families,
            exhausted_families=exhausted_families,
            excluded_families={
                current_family,
                neighbor.delta.family_key if neighbor is not None else "",
            },
            event_families=event_families,
        )
        if orthogonal is not None:
            candidates.append(orthogonal)

        for candidate in candidates:
            candidate.rank_score = self._rank_candidate(
                candidate,
                snapshot=control_snapshot,
                exhausted_families=exhausted_families,
                strong_families=strong_families,
                repeated_signature=repeated_signature,
            )

        ranked = sorted(candidates, key=lambda c: c.rank_score, reverse=True)[:3]
        chosen_index = 0 if ranked else -1
        chosen = ranked[chosen_index] if chosen_index >= 0 else None
        strategy_kind = chosen.strategy_kind if chosen is not None else "none"
        suggested_family = chosen.delta.family_key if chosen is not None else control_snapshot.suggested_family_key

        reason_md = (
            f"Current line `{control_snapshot.current_line_key or current_family}` is "
            f"{'exhausted' if control_snapshot.current_line_exhausted else 'stalled/blocked'}: "
            f"recent replayable gain={control_snapshot.recent_current_line_replayable_gain}, "
            f"accepts={control_snapshot.recent_current_line_accepts}, "
            f"reverts={control_snapshot.recent_current_line_reverts}, "
            f"failed cohorts={control_snapshot.recent_current_line_failed_cohorts}. "
            f"Selected `{strategy_kind}` to materially change trajectory."
        )

        return UnblockSuggestion(
            reason_md=reason_md,
            strategy_kind=strategy_kind,
            current_family=current_family,
            current_frontier_node=control_snapshot.current_line_node_key or blocked_node_key,
            suggested_family=suggested_family,
            blocked_node_key=blocked_node_key,
            candidates=tuple(ranked),
            chosen_index=chosen_index,
        )

    def _build_repair_candidate(
        self,
        *,
        problem: ProblemSpec,
        gap: dict[str, Any],
        snapshot: ControlSnapshot,
        blocked_node_key: str,
        recent_fractures: list[dict[str, Any]],
    ) -> UnblockCandidate | None:
        current_family = snapshot.current_family_key
        if not current_family:
            return None

        current_fracture = next(
            (row for row in recent_fractures if str(row.get("family_key") or "") == current_family),
            None,
        )
        fracture_required = str((current_fracture or {}).get("required_delta_md") or "").strip()
        blocker_kind = snapshot.blocker_kind or "frontier_blocker"
        proposed_required = fracture_required or (
            f"Repair `{blocked_node_key}` with a bounded invariant shift addressing `{blocker_kind}` "
            "and a different witness class than recent failed cohorts."
        )
        proposed_skeleton = (
            f"Under `{current_family}`, prove a revised skeleton for `{blocked_node_key}` that resolves "
            f"`{blocker_kind}` via a bounded repair obligation."
        )

        required_changed = materially_changed_required_delta(
            snapshot.current_required_delta_md,
            proposed_required,
        )
        skeleton_changed = materially_changed_theorem_skeleton(
            snapshot.current_theorem_skeleton_md,
            proposed_skeleton,
        )
        if not (required_changed or skeleton_changed):
            return None

        blocker_summary = snapshot.blocker_summary or "current blocked frontier"
        edits = {
            "bridge_claim": (
                f"Repair the `{current_family}` bridge on `{blocked_node_key}` by making "
                f"`{blocker_kind}` explicit and bounded."
            ),
            "local_law": (
                f"Introduce a local repair invariant that blocks the current failure mode: {blocker_summary}"
            ),
            "kill_test": (
                f"Search for a minimal witness showing the revised `{current_family}` repair law still fails."
            ),
            "theorem_skeleton": proposed_skeleton,
            "required_delta_md": proposed_required,
            "obligations": [
                f"formalize repaired bridge for `{blocked_node_key}`",
                f"prove revised local law for `{blocker_kind}`",
                "run bounded counterexample probe against revised repair",
            ],
        }
        delta = DeltaProposal(
            delta_type="reduction_delta",
            title=f"Repair {current_family} line at {blocked_node_key}",
            summary_md="Materially different same-family repair against the current blocker.",
            family_key=current_family,
            target_node_key=str(gap.get("node_key") or blocked_node_key),
            edits=edits,
        )
        return UnblockCandidate(
            strategy_kind="repair",
            material_difference_md="Changed required delta and theorem skeleton for the same family.",
            delta=delta,
        )

    def _build_neighbor_family_candidate(
        self,
        *,
        problem: ProblemSpec,
        gap: dict[str, Any],
        snapshot: ControlSnapshot,
        strong_families: tuple[str, ...],
        exhausted_families: set[str],
    ) -> UnblockCandidate | None:
        current_family = snapshot.current_family_key
        candidates: list[str] = []
        if current_family:
            candidates.extend(FAMILY_NEIGHBORS.get(current_family, ()))
        candidates.extend([family for family in strong_families if family and family != current_family])
        for family in WORLD_FAMILY_LIBRARY:
            if family != current_family:
                candidates.append(family)
        neighbor_family = self._first_valid_family(candidates, exhausted_families, {current_family})
        if not neighbor_family:
            return None
        proposal = self.worldsmith.propose_world(
            problem,
            gap,
            preferred_family_key=neighbor_family,
            avoid_family_keys={current_family, *exhausted_families} if current_family else exhausted_families,
        )
        if proposal.world_packet is None:
            return None
        if proposal.world_packet.family_key == current_family:
            return None
        return UnblockCandidate(
            strategy_kind="neighbor_family",
            material_difference_md=(
                f"Switch from `{current_family or 'unknown'}` to neighboring family `{proposal.world_packet.family_key}`."
            ),
            delta=proposal,
        )

    def _build_orthogonal_family_candidate(
        self,
        *,
        problem: ProblemSpec,
        gap: dict[str, Any],
        snapshot: ControlSnapshot,
        strong_families: tuple[str, ...],
        exhausted_families: set[str],
        excluded_families: set[str],
        event_families: tuple[str, ...],
    ) -> UnblockCandidate | None:
        current_family = snapshot.current_family_key
        neighbor_set = set(FAMILY_NEIGHBORS.get(current_family, ()))
        excluded = {family for family in excluded_families if family}
        excluded.update(neighbor_set)
        orthogonal_pool = [
            family
            for family in WORLD_FAMILY_LIBRARY
            if family and family not in exhausted_families and family not in excluded
        ]
        # Prefer strong surviving alternatives that are not immediate neighbors.
        strong_orth = [family for family in strong_families if family in orthogonal_pool]
        ranked_pool = strong_orth + [family for family in orthogonal_pool if family not in strong_orth]
        # Avoid immediate return to recently overused families.
        recent_bias = {family for family in event_families[-4:] if family}
        orthogonal_family = next((family for family in ranked_pool if family not in recent_bias), "")
        if not orthogonal_family:
            orthogonal_family = next(iter(ranked_pool), "")
        if not orthogonal_family:
            return None
        proposal = self.worldsmith.propose_world(
            problem,
            gap,
            preferred_family_key=orthogonal_family,
            avoid_family_keys={snapshot.current_family_key, *exhausted_families} if snapshot.current_family_key else exhausted_families,
        )
        if proposal.world_packet is None:
            return None
        if proposal.world_packet.family_key == current_family:
            return None
        return UnblockCandidate(
            strategy_kind="orthogonal_family",
            material_difference_md=(
                f"Rotate to orthogonal family `{proposal.world_packet.family_key}` to escape repeated line signatures."
            ),
            delta=proposal,
        )

    def _rank_candidate(
        self,
        candidate: UnblockCandidate,
        *,
        snapshot: ControlSnapshot,
        exhausted_families: set[str],
        strong_families: tuple[str, ...],
        repeated_signature: str,
    ) -> float:
        score = 0.0
        family = candidate.delta.family_key
        same_family = family == snapshot.current_family_key

        if candidate.strategy_kind == "repair":
            score += 2.0
        elif candidate.strategy_kind == "neighbor_family":
            score += 2.5
        elif candidate.strategy_kind == "orthogonal_family":
            score += 3.0

        if family and family in strong_families:
            score += 1.2
        if not same_family:
            score += 2.4
        if family in exhausted_families:
            score -= 3.0
        if same_family and snapshot.current_line_exhausted:
            score -= 2.2
        if same_family and repeated_signature and snapshot.recent_current_line_repeated_signature_count >= 2:
            score -= 2.0

        obligations = candidate.delta.edits.get("obligations", []) if candidate.delta.edits else []
        if not obligations and candidate.delta.world_packet is None:
            score -= 1.0
        if snapshot.blocker_kind and snapshot.blocker_kind in (
            str(candidate.delta.summary_md) + " " + str(candidate.material_difference_md)
        ):
            score += 0.8
        if candidate.delta.target_node_key and candidate.delta.target_node_key == snapshot.current_line_node_key:
            score += 0.5
        return score

    def _first_valid_family(
        self,
        families: list[str],
        exhausted_families: set[str],
        banned_families: set[str],
    ) -> str:
        for family in families:
            if not family:
                continue
            if family in banned_families:
                continue
            if family in exhausted_families:
                continue
            return family
        return ""

    def _exhausted_families(
        self,
        snapshot: ControlSnapshot,
        recent_fractures: list[dict[str, Any]],
    ) -> set[str]:
        exhausted = set()
        if snapshot.exhausted_family_key:
            exhausted.add(snapshot.exhausted_family_key)
        if snapshot.current_line_exhausted and snapshot.current_family_key:
            exhausted.add(snapshot.current_family_key)
        for fracture in recent_fractures:
            family = str(fracture.get("family_key") or "")
            if not family:
                continue
            if int(fracture.get("repeat_count") or 0) >= 2:
                exhausted.add(family)
            if str(fracture.get("ban_level") or "") == "hard":
                exhausted.add(family)
        return exhausted
