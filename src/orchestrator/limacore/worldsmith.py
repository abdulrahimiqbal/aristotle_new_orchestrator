from __future__ import annotations

from .models import DeltaProposal, ProblemSpec, WorldPacket


WORLD_FAMILY_LIBRARY = {
    "coordinate_lift": "Lift the state into offset coordinates that linearize local moves.",
    "quotient": "Pass to a quotient that collapses redundant transport degrees of freedom.",
    "hidden_state": "Expose a latent bookkeeping variable invisible in the raw statement.",
    "cocycle": "Represent dynamics through a cocycle over a simpler base evolution.",
    "balancing_world": "Translate the problem into a balancing or compression energy picture.",
    "symbolic_dynamics": "Encode transitions as a constrained shift or rewrite system.",
    "operator_world": "Package the move as an operator with monotone auxiliary structure.",
    "order_or_convexity": "Use partial order or convexity to force canonical terminal behavior.",
    "graph_or_rewrite": "Treat the system as a terminating and confluent rewrite graph.",
    "other": "Fallback family when no sharper ontology is available.",
}


class Worldsmith:
    def propose_world(
        self,
        problem: ProblemSpec,
        gap: dict,
        *,
        preferred_family_key: str | None = None,
        avoid_family_keys: set[str] | tuple[str, ...] = (),
    ) -> DeltaProposal:
        avoided = {str(key) for key in avoid_family_keys}
        packet = self._choose_packet(problem, gap, preferred_family_key=preferred_family_key, avoided=avoided)
        packet.validate()
        return DeltaProposal(
            delta_type="world_delta",
            title=packet.world_name,
            summary_md=packet.novelty_note,
            family_key=packet.family_key,
            world_packet=packet,
            target_node_key=str(gap["node_key"]),
        )

    def _choose_packet(
        self,
        problem: ProblemSpec,
        gap: dict,
        *,
        preferred_family_key: str | None,
        avoided: set[str],
    ) -> WorldPacket:
        candidates: list[str]
        if preferred_family_key:
            candidates = [preferred_family_key]
        elif problem.slug == "collatz":
            candidates = ["quotient", "hidden_state", "cocycle", "operator_world", "coordinate_lift", "other"]
        elif problem.slug == "inward-compression-conjecture":
            candidates = ["balancing_world", "order_or_convexity", "coordinate_lift", "other"]
        else:
            candidates = ["coordinate_lift", "hidden_state", "operator_world", "other"]
        for family_key in candidates:
            if family_key in avoided:
                continue
            packet = self._packet_for_family(problem, gap, family_key)
            if packet is not None:
                return packet
        return self._packet_for_family(problem, gap, "coordinate_lift") or self._packet_for_family(problem, gap, "other")  # type: ignore[return-value]

    def _packet_for_family(self, problem: ProblemSpec, gap: dict, family_key: str) -> WorldPacket | None:
        slug = problem.slug
        if slug == "inward-compression-conjecture" and family_key == "balancing_world":
            return WorldPacket(
                world_name="Balanced compression coordinates",
                family_key="balancing_world",
                new_objects=[
                    "offset coordinates b_i = a_i - (i-1)",
                    "convex compression energy E(b)",
                    "stable balanced profile",
                ],
                bridge_to_problem="Translate strictly increasing states into weakly increasing offset coordinates where each legal move shifts mass inward without changing length or total offset sum.",
                why_easier_here="Balanced offset coordinates expose the hidden monotonicity: the move becomes a local averaging step with a convex Lyapunov functional and a canonical balanced terminal profile.",
                local_law="Each legal move weakly decreases convex compression energy and strictly decreases it unless the state is already balanced.",
                kill_test="Search for a legal move that preserves the sum but increases convex compression energy in offset coordinates.",
                theorem_skeleton="If offset energy strictly descends and balanced profiles are unique for fixed length and sum, then every legal sequence terminates at the same terminal state.",
                formal_agenda=[
                    "define offset coordinates and prove the move preserves weak monotonicity",
                    "prove convex compression energy strictly decreases on each legal move",
                    "show balanced terminal profile is unique for fixed length and sum",
                ],
                literature_queries=[
                    "discrete convexity termination confluence invariant energy",
                    "abelian sandpile balancing canonical terminal state",
                ],
                formal_queries=[
                    "termination via measure descent",
                    "confluence from unique normal form",
                ],
                confidence_prior=0.87,
                novelty_note="This world makes the balancing ontology first-class instead of residue heuristics.",
            )
        if slug == "collatz" and family_key == "quotient":
            return WorldPacket(
                world_name="Odd-step quotient probe",
                family_key="quotient",
                new_objects=["odd-step quotient", "compressed parity macro-step"],
                bridge_to_problem="Collapse even transport into a macro-step on odd inputs.",
                why_easier_here="The odd-step quotient removes inert even drift and focuses on the unstable branch points.",
                local_law="Macro-step size correlates with parity density in bounded windows.",
                kill_test="Find a small odd macro-state where the quotient predicts descent but the real orbit expands.",
                theorem_skeleton="A quotient with bounded expansion and strict descent on average would imply global termination.",
                formal_agenda=["define the odd macro-step", "probe bounded expansion", "search counterexample"],
                literature_queries=["collatz odd step quotient acceleration"],
                formal_queries=["collatz accelerated map descent"],
                confidence_prior=0.32,
                novelty_note="Useful as a falsification target rather than a survivor by default.",
            )
        if slug == "collatz" and family_key == "hidden_state":
            return WorldPacket(
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
        if family_key == "operator_world":
            return WorldPacket(
                world_name="Operator descent layer",
                family_key="operator_world",
                new_objects=["descent operator", "operator norm budget"],
                bridge_to_problem=f"Treat {problem.title} as an operator with a bounded descent budget at {gap['node_key']}.",
                why_easier_here="Operator norms compress many local moves into a single monotonicity statement.",
                local_law="A calibrated operator norm weakly decreases under admissible moves.",
                kill_test="Find an admissible move that increases the calibrated operator norm.",
                theorem_skeleton="If the operator norm descends and the operator family is finite on the frontier, descent follows.",
                formal_agenda=["define the operator norm", "prove monotonicity", "enumerate finite operator families"],
                literature_queries=[problem.title],
                formal_queries=[gap["title"]],
                confidence_prior=0.5,
                novelty_note="Operator layer fallback.",
            )
        if family_key == "coordinate_lift":
            return WorldPacket(
                world_name="Coordinate lift search",
                family_key="coordinate_lift",
                new_objects=["lifted coordinates"],
                bridge_to_problem=f"Lift {problem.title} into a coordinate system aligned with {gap['node_key']}.",
                why_easier_here="The lifted coordinates separate invariants from local motion.",
                local_law="A simple potential behaves monotonically in the lifted model.",
                kill_test="Search for a lifted step that violates the monotone potential.",
                theorem_skeleton="If the lift preserves solutions and enforces descent, the target theorem closes in the lifted world.",
                formal_agenda=["define the lift", "prove local monotonicity"],
                literature_queries=[problem.title],
                formal_queries=[gap["title"]],
                confidence_prior=0.55,
                novelty_note="Generic coordinate lift fallback.",
            )
        if family_key == "balancing_world":
            return WorldPacket(
                world_name="Balanced compression coordinates",
                family_key="balancing_world",
                new_objects=["offset coordinates", "balanced profile"],
                bridge_to_problem=f"Expose a balancing ontology for {problem.title}.",
                why_easier_here="Balanced coordinates make terminal behavior canonical.",
                local_law="A convex balance functional descends on every move.",
                kill_test="Find a move that preserves the balance functional.",
                theorem_skeleton="Unique balanced terminal states force confluence.",
                formal_agenda=["define the balance functional", "prove descent"],
                literature_queries=[problem.title],
                formal_queries=[gap["title"]],
                confidence_prior=0.76,
                novelty_note="Balanced fallback world.",
            )
        if family_key == "other":
            return WorldPacket(
                world_name="Generic frontier search",
                family_key="other",
                new_objects=["frontier invariant"],
                bridge_to_problem=f"Search for a compact invariant aligned with {gap['node_key']}.",
                why_easier_here="A small invariant can expose the next surviving line.",
                local_law="Candidate invariant should not increase across admissible moves.",
                kill_test="Find a counterexample to the candidate invariant.",
                theorem_skeleton="If the invariant survives bounded probes, it becomes the next bridge.",
                formal_agenda=["define the invariant", "probe bounded motion"],
                literature_queries=[problem.title],
                formal_queries=[gap["title"]],
                confidence_prior=0.42,
                novelty_note="Fallback world when nothing sharper is available.",
            )
        return None
