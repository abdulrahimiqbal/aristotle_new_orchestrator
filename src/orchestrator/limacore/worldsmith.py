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
    def propose_world(self, problem: ProblemSpec, gap: dict) -> DeltaProposal:
        slug = problem.slug
        if slug == "inward-compression-conjecture":
            packet = WorldPacket(
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
        elif slug == "collatz":
            packet = WorldPacket(
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
        else:
            packet = WorldPacket(
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
        packet.validate()
        return DeltaProposal(
            delta_type="world_delta",
            title=packet.world_name,
            summary_md=packet.novelty_note,
            family_key=packet.family_key,
            world_packet=packet,
            target_node_key=str(gap["node_key"]),
        )
