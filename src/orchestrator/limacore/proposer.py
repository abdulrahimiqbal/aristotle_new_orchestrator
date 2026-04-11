from __future__ import annotations

from .db import LimaCoreDB
from .models import DeltaProposal, ProblemSpec, WorldPacket
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
        if problem.slug == "collatz":
            stale_quotient_runs = sum(
                1
                for cohort in self.db.list_cohorts(problem.id)
                if str(cohort.get("title") or "") == "Kill stale quotient world"
                and str(cohort.get("status") or "") == "finished"
                and int(cohort.get("yielded_lemmas") or 0) == 0
            )
            if str(strongest["family_key"]) == "quotient" and stale_quotient_runs >= 2:
                packet = WorldPacket(
                    world_name="Parity carry ledger",
                    family_key="hidden_state",
                    new_objects=["carry ledger", "odd-step debt", "parity block signature"],
                    bridge_to_problem="Refine the accelerated odd-step map by attaching a hidden carry ledger that records where parity compression loses information.",
                    why_easier_here="The hidden ledger separates true growth from bookkeeping noise, so local drift bounds can be stated without pretending the raw quotient is already closed.",
                    local_law="Carry-adjusted odd-step debt does not increase across admissible parity blocks and strictly decreases on calibrated return patterns.",
                    kill_test="Search for a short parity block whose carry-adjusted debt increases after ledger normalization.",
                    theorem_skeleton="If the ledger closes the information leak and debt decreases on every calibrated return, Collatz descent can be reduced to a finite family of parity block lemmas.",
                    formal_agenda=[
                        "define the carry ledger and show it reconstructs odd-step drift",
                        "prove non-increase of carry-adjusted debt on admissible parity blocks",
                        "classify calibrated return patterns needed for descent",
                    ],
                    literature_queries=[
                        "collatz parity vector accelerated map drift",
                        "collatz hidden state parity block invariants",
                    ],
                    formal_queries=[
                        "accelerated collatz parity blocks",
                        "collatz return map drift bound",
                    ],
                    confidence_prior=0.66,
                    novelty_note="Rotate away from the stale quotient kill loop by tracking the hidden carry ledger explicitly.",
                )
                return DeltaProposal(
                    delta_type="world_delta",
                    title=packet.world_name,
                    summary_md=packet.novelty_note,
                    family_key=packet.family_key,
                    world_packet=packet,
                    target_node_key=str(gap["node_key"]),
                )
            if str(strongest["family_key"]) == "hidden_state":
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
