from __future__ import annotations

from .control import build_control_snapshot, family_exhausted
from .db import LimaCoreDB
from .models import DeltaProposal, ProblemSpec
from .program import get_program_state
from .unblock_manager import UnblockManager, UnblockSuggestion
from .worldsmith import Worldsmith


class Proposer:
    def __init__(
        self,
        db: LimaCoreDB,
        *,
        worldsmith: Worldsmith | None = None,
        unblock_manager: UnblockManager | None = None,
    ) -> None:
        self.db = db
        self.worldsmith = worldsmith or Worldsmith()
        self.unblock_manager = unblock_manager or UnblockManager(worldsmith=self.worldsmith)

    def propose_unblock(self, problem: ProblemSpec, gap: dict) -> UnblockSuggestion | None:
        snapshot = build_control_snapshot(self.db, problem.id)
        if not self.unblock_manager.should_activate(problem, snapshot):
            return None
        return self.unblock_manager.suggest(
            problem=problem,
            gap=gap,
            control_snapshot=snapshot,
            strongest_worlds=self.db.list_world_heads(problem.id),
            recent_fractures=self.db.list_fracture_heads(problem.id),
            recent_events=self.db.list_events(problem.id, limit=max(snapshot.window_size, 20)),
        )

    def propose_delta(self, problem: ProblemSpec, gap: dict) -> DeltaProposal:
        _program = get_program_state(self.db, problem.id)
        worlds = self.db.list_world_heads(problem.id)
        fractures = self.db.list_fracture_heads(problem.id)
        if not worlds:
            return self.worldsmith.propose_world(problem, gap)
        strongest = worlds[0]
        snapshot = build_control_snapshot(self.db, problem.id)
        exhausted = family_exhausted(snapshot)
        if exhausted:
            if problem.slug == "collatz" and snapshot.current_family_key == "quotient":
                packet = self.worldsmith.propose_world(
                    problem,
                    gap,
                    preferred_family_key="hidden_state",
                    avoid_family_keys={snapshot.current_family_key},
                ).world_packet
                if packet is not None:
                    return DeltaProposal(
                        delta_type="world_delta",
                        title=packet.world_name,
                        summary_md=packet.novelty_note,
                        family_key=packet.family_key,
                        world_packet=packet,
                        target_node_key=str(gap["node_key"]),
                    )
            if snapshot.current_family_key == "hidden_state":
                return DeltaProposal(
                    delta_type="lemma_delta",
                    title="Carry-adjusted drift lemma",
                    summary_md="Convert the hidden carry ledger into replayable local drift structure.",
                    family_key="hidden_state",
                    target_node_key=str(gap["node_key"]),
                    edits={
                        "bridge_claim": "The carry ledger reconstructs accelerated odd-step drift without losing parity-block information.",
                        "local_law": "Carry-adjusted debt is monotone on admissible parity return blocks.",
                        "kill_test": "Look for a calibrated parity block whose ledger debt increases after normalization.",
                        "theorem_skeleton": "A ledger-stable drift bound reduces global descent to finitely many calibrated return patterns.",
                        "required_delta_md": "Switch from quotient heuristics to carry-ledger obligations before retrying the same frontier line.",
                        "obligations": [
                            "prove the ledger reconstructs odd-step drift",
                            "prove monotonicity on admissible parity blocks",
                        ],
                    },
                )
        if problem.slug == "inward-compression-conjecture" and gap["node_key"] in {"target_theorem", "terminal_form_uniqueness", "replay_closure"}:
            return DeltaProposal(
                delta_type="lemma_delta",
                title="Balanced terminal-form lemma",
                summary_md="Sharpen the balancing world toward a unique terminal profile.",
                family_key=str(strongest["family_key"]),
                target_node_key=str(gap["node_key"]),
                edits={
                    "bridge_claim": "Offset coordinates preserve legal reachability and fixed-sum classes.",
                    "local_law": "Convex compression energy strictly decreases on every legal move until the balanced profile.",
                    "kill_test": "Search for a legal move in offset coordinates that preserves energy or leaves two distinct terminal profiles with the same sum.",
                    "theorem_skeleton": "Termination follows from strict energy descent; uniqueness follows from the characterization of balanced fixed points inside a fixed length-and-sum class.",
                    "obligations": [
                        "prove fixed points are exactly balanced profiles",
                        "show balanced profile uniqueness for fixed length and sum",
                    ],
                },
            )
        # FIXED: Collatz proposer now checks current family before emitting kill cohorts
        if problem.slug == "collatz":
            current_family = snapshot.current_family_key

            if current_family == "quotient" and not snapshot.current_family_exhausted:
                required_delta = snapshot.current_required_delta_md or "Rotate away from the stale quotient family before retrying the same frontier line."
                return DeltaProposal(
                    delta_type="kill_delta",
                    title="Kill stale quotient world",
                    summary_md="Attack the quotient family before spending more proof budget.",
                    family_key="quotient",
                    target_node_key=str(gap["node_key"]),
                    edits={
                        "bridge_claim": "Assume the odd-step quotient is informative enough to predict descent.",
                        "local_law": "Bounded odd-step expansion would force eventual descent.",
                        "kill_test": "Find a small odd orbit with quotient drift inconsistent with the bounded expansion law.",
                        "theorem_skeleton": "A failed bounded expansion law collapses the quotient proof program.",
                        "required_delta_md": required_delta,
                        "obligations": ["bounded odd-step expansion fails on a small witness"],
                    },
                )

            if current_family == "hidden_state":
                return DeltaProposal(
                    delta_type="lemma_delta",
                    title="Carry-adjusted drift lemma",
                    summary_md="Convert the hidden carry ledger into replayable local drift structure.",
                    family_key="hidden_state",
                    target_node_key=str(gap["node_key"]),
                    edits={
                        "bridge_claim": "The carry ledger reconstructs accelerated odd-step drift without losing parity-block information.",
                        "local_law": "Carry-adjusted debt is monotone on admissible parity return blocks.",
                        "kill_test": "Look for a calibrated parity block whose ledger debt increases after normalization.",
                        "theorem_skeleton": "A ledger-stable drift bound reduces global descent to finitely many calibrated return patterns.",
                        "required_delta_md": "Switch from quotient heuristics to carry-ledger obligations before retrying the same frontier line.",
                        "obligations": [
                            "prove the ledger reconstructs odd-step drift",
                            "prove monotonicity on admissible parity blocks",
                        ],
                    },
                )

            if snapshot.current_family_exhausted and current_family:
                rotation_family = snapshot.suggested_family_key or "hidden_state"
                if rotation_family == current_family:
                    rotation_family = "hidden_state" if current_family != "hidden_state" else "cocycle"
                packet = self.worldsmith.propose_world(
                    problem,
                    gap,
                    preferred_family_key=rotation_family,
                    avoid_family_keys={current_family},
                ).world_packet
                if packet is not None:
                    return DeltaProposal(
                        delta_type="world_delta",
                        title=packet.world_name,
                        summary_md=packet.novelty_note,
                        family_key=packet.family_key,
                        world_packet=packet,
                        target_node_key=str(gap["node_key"]),
                    )

            packet = self.worldsmith.propose_world(
                problem,
                gap,
                avoid_family_keys={current_family} if current_family else {"quotient"},
            ).world_packet
            if packet is not None:
                return DeltaProposal(
                    delta_type="world_delta",
                    title=packet.world_name,
                    summary_md=packet.novelty_note,
                    family_key=packet.family_key,
                    world_packet=packet,
                    target_node_key=str(gap["node_key"]),
                )
        if fractures:
            fracture = fractures[0]
            return DeltaProposal(
                delta_type="reduction_delta",
                title="Required delta against recent fracture",
                summary_md="Use the most recent fracture to alter the next formal move.",
                family_key=str(strongest["family_key"]),
                target_node_key=str(gap["node_key"]),
                edits={
                    "bridge_claim": f"Repair the bridge by addressing {fracture['failure_type']}.",
                    "local_law": "Introduce a corrected local invariant.",
                    "kill_test": "Search for another witness violating the corrected invariant.",
                    "theorem_skeleton": "If the corrected invariant survives, resume the theorem skeleton.",
                    "required_delta_md": str(fracture.get("required_delta_md") or "Change ontology or supply a materially different bridge before retrying this family."),
                    "obligations": ["repair recent fracture"],
                },
            )
        return self.worldsmith.propose_world(problem, gap, avoid_family_keys={snapshot.current_family_key} if snapshot.current_family_key else set())
