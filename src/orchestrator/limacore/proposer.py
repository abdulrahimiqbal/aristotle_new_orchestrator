from __future__ import annotations

from .db import LimaCoreDB
from .models import DeltaProposal, ProblemSpec
from .program import get_program_state
from .worldsmith import Worldsmith


class Proposer:
    def __init__(self, db: LimaCoreDB, *, worldsmith: Worldsmith | None = None) -> None:
        self.db = db
        self.worldsmith = worldsmith or Worldsmith()

    def propose_delta(self, problem: ProblemSpec, gap: dict) -> DeltaProposal:
        _program = get_program_state(self.db, problem.id)
        worlds = self.db.list_world_heads(problem.id)
        fractures = self.db.list_fracture_heads(problem.id)
        if not worlds:
            return self.worldsmith.propose_world(problem, gap)
        strongest = worlds[0]
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
        if problem.slug == "collatz":
            return DeltaProposal(
                delta_type="kill_delta",
                title="Kill stale quotient world",
                summary_md="Attack the quotient family before spending more proof budget.",
                family_key=str(strongest["family_key"]),
                target_node_key=str(gap["node_key"]),
                edits={
                    "bridge_claim": "Assume the odd-step quotient is informative enough to predict descent.",
                    "local_law": "Bounded odd-step expansion would force eventual descent.",
                    "kill_test": "Find a small odd orbit with quotient drift inconsistent with the bounded expansion law.",
                    "theorem_skeleton": "A failed bounded expansion law collapses the quotient proof program.",
                    "obligations": ["bounded odd-step expansion fails on a small witness"],
                },
            )
        if fractures:
            return DeltaProposal(
                delta_type="reduction_delta",
                title="Required delta against recent fracture",
                summary_md="Use the most recent fracture to alter the next formal move.",
                family_key=str(strongest["family_key"]),
                target_node_key=str(gap["node_key"]),
                edits={
                    "bridge_claim": f"Repair the bridge by addressing {fractures[0]['failure_type']}.",
                    "local_law": "Introduce a corrected local invariant.",
                    "kill_test": "Search for another witness violating the corrected invariant.",
                    "theorem_skeleton": "If the corrected invariant survives, resume the theorem skeleton.",
                    "obligations": ["repair recent fracture"],
                },
            )
        return self.worldsmith.propose_world(problem, gap)
