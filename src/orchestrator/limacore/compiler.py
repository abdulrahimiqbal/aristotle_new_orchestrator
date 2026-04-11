from __future__ import annotations

from .models import AristotleAgenda, AristotleJobSpec, DeltaProposal, GroundingBundle, ReductionPacket


class CompileError(ValueError):
    pass


def compile_delta_to_reduction(
    gap: dict,
    delta: DeltaProposal,
    grounding: GroundingBundle,
) -> tuple[ReductionPacket, AristotleAgenda]:
    if delta.world_packet is None and delta.delta_type == "world_delta":
        raise CompileError("world_delta requires a world packet")
    bridge = ""
    local_law = ""
    kill_test = ""
    theorem_skeleton = ""
    rationale = delta.summary_md
    obligations: list[str] = []
    title = delta.title
    if delta.world_packet is not None:
        packet = delta.world_packet
        bridge = packet.bridge_to_problem
        local_law = packet.local_law
        kill_test = packet.kill_test
        theorem_skeleton = packet.theorem_skeleton
        obligations = list(packet.formal_agenda[:3])
        rationale = (
            f"{packet.novelty_note}\n\n"
            f"Grounding: {len(grounding.formal_analogs)} formal, "
            f"{len(grounding.literature_analogs)} literature, "
            f"{len(grounding.internal_analogs)} internal analogs."
        )
    else:
        bridge = str(delta.edits.get("bridge_claim") or "").strip()
        local_law = str(delta.edits.get("local_law") or "").strip()
        kill_test = str(delta.edits.get("kill_test") or "").strip()
        theorem_skeleton = str(delta.edits.get("theorem_skeleton") or "").strip()
        obligations = [x for x in delta.edits.get("obligations", []) if str(x).strip()][:3]
    if not bridge or not local_law or not kill_test or not theorem_skeleton:
        raise CompileError("delta did not compile into a tiny formal agenda")
    if not obligations:
        obligations = [bridge, local_law, theorem_skeleton][:3]
    if len(obligations) > 4:
        raise CompileError("agenda must stay bounded")
    job_specs = [
        AristotleJobSpec("bridge_lemma", "Bridge probe", "bridge_claim", {"claim": bridge, "gap": gap["node_key"]}),
        AristotleJobSpec("local_law", "Local law probe", "local_energy_law", {"claim": local_law}),
        AristotleJobSpec("counterexample_search", "Kill probe", "kill_test", {"claim": kill_test}),
        AristotleJobSpec("theorem_skeleton_probe", "Skeleton probe", "terminal_form_uniqueness", {"claim": theorem_skeleton}),
    ]
    reduction = ReductionPacket(
        selected_gap=str(gap["node_key"]),
        bridge_claim=bridge,
        local_law=local_law,
        kill_test=kill_test,
        theorem_skeleton=theorem_skeleton,
        obligations=obligations,
        cohort_plan=[{"cohort_kind": "aristotle_agenda", "title": title, "jobs": [spec.job_kind for spec in job_specs]}],
        rationale_md=rationale,
    )
    agenda = AristotleAgenda(
        title=title,
        bridge_claim=bridge,
        local_law=local_law,
        kill_test=kill_test,
        theorem_skeleton=theorem_skeleton,
        obligations=obligations,
        job_specs=job_specs,
    )
    return reduction, agenda
