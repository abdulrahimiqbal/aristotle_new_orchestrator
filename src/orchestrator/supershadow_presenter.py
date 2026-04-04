from __future__ import annotations

import json
from typing import Any


def _load_json_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _load_json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _concept_sort_key(
    concept: dict[str, Any]
) -> tuple[Any, ...]:
    status_rank = {
        "super_candidate": 5,
        "converging": 4,
        "has_signs": 3,
        "no_signs_yet": 2,
        "proposed": 1,
        "dead": 0,
    }.get(str(concept.get("universe_status") or "proposed"), 0)
    return (
        status_rank,
        int(concept.get("compression_power") or 0),
        int(concept.get("fit_to_known_facts") or 0),
        int(concept.get("ontological_delta") or 0),
        int(concept.get("falsifiability") or 0),
        int(concept.get("family_novelty") or 0),
        int(concept.get("transfer_value") or 0),
        -int(concept.get("family_saturation_penalty") or 0),
        int(concept.get("bridgeability") or 0),
        -int(concept.get("speculative_risk") or 0),
        -int(concept.get("grounding_cost") or 0),
        str(concept.get("family_kind") or ""),
        str(concept.get("concept_family") or ""),
        str(concept.get("created_at") or ""),
    )


def _present_handoff(row: dict[str, Any]) -> dict[str, Any]:
    payload = _load_json_object(row.get("payload_json"))
    pretty = str(row.get("payload_json") or "{}")
    if payload:
        pretty = json.dumps(payload, indent=2, ensure_ascii=False)
    review_kind = str(payload.get("review_kind") or "").strip().lower()
    return {
        "action_label": "Super-universe review"
        if review_kind == "super_universe_candidate"
        else "Handoff to Shadow",
        "title": str(payload.get("title") or "Shadow handoff"),
        "summary": str(payload.get("summary") or ""),
        "why_compressive": str(payload.get("why_compressive") or ""),
        "bridge_lemmas": payload.get("bridge_lemmas") or [],
        "shadow_task": str(payload.get("shadow_task") or ""),
        "recommended_next_step": str(payload.get("recommended_next_step") or ""),
        "grounding_notes": str(payload.get("grounding_notes") or ""),
        "concept_id": str(payload.get("concept_id") or row.get("concept_id") or ""),
        "concept_title": str(payload.get("concept_title") or ""),
        "concept_scores": payload.get("concept_scores") or {},
        "super_universe_candidate": payload.get("super_universe_candidate") or {},
        "payload_json_pretty": pretty,
    }


def _present_incubation(row: dict[str, Any]) -> dict[str, Any]:
    payload = _load_json_object(row.get("concept_packet_json"))
    grounded_ids = _load_json_list(row.get("grounded_promotion_ids_json"))
    events = list(row.get("events") or [])
    latest_event = dict(events[0]) if events else None
    return {
        "title": str(row.get("title") or payload.get("title") or "Supershadow incubation"),
        "status": str(row.get("status") or "incubating"),
        "summary": str(payload.get("summary") or ""),
        "why_compressive": str(payload.get("why_compressive") or ""),
        "shadow_task": str(payload.get("shadow_task") or ""),
        "recommended_next_step": str(payload.get("recommended_next_step") or ""),
        "bridge_lemmas": payload.get("bridge_lemmas") or [],
        "grounding_notes": str(payload.get("grounding_notes") or ""),
        "shadow_last_run_id": str(row.get("shadow_last_run_id") or ""),
        "shadow_last_summary": str(row.get("shadow_last_summary") or ""),
        "grounded_promotion_ids": grounded_ids,
        "latest_event": latest_event,
        "concept_title": str(payload.get("concept_title") or ""),
    }


def _build_next_step(
    *,
    run_count: int,
    pending_handoffs: int,
    active_incubations: int,
    best_concept: dict[str, Any] | None,
    concept_count: int,
) -> dict[str, str]:
    if pending_handoffs > 0:
        return {
            "title": "Review the Shadow handoff queue",
            "body": (
                f"{pending_handoffs} concept handoff(s) are waiting. Approve only the concepts "
                "that actually compress the grounded frontier."
            ),
        }
    if active_incubations > 0:
        return {
            "title": "Track concept transfer into Shadow",
            "body": (
                f"{active_incubations} incubation(s) are active. Check whether Shadow has operationalized them into bridge lemmas or grounding requests."
            ),
        }
    if run_count == 0:
        return {
            "title": "Generate the first conceptual sweep",
            "body": (
                "Run Supershadow to search for a dominant new language, then distill the best survivor into a falsifier and first bridge."
            ),
        }
    if best_concept and int(best_concept.get("compression_power") or 0) >= 4:
        return {
            "title": "Pressure-test the dominant worldview",
            "body": (
                "Start with the strongest concept below. Ask whether its kill-test is sharp and whether the first bridge is earned rather than decorative."
            ),
        }
    if concept_count > 0:
        return {
            "title": "Keep refining the board",
            "body": (
                "There are concepts on the board, but no dominant survivor yet. Generate another pass and look for a stronger worldview instead of polishing weak bridges."
            ),
        }
    return {
        "title": "Keep inventing",
        "body": "Supershadow should keep searching for a more compressive language.",
    }


def build_supershadow_ui_context(
    *,
    concepts: list[dict[str, Any]],
    handoffs: list[dict[str, Any]],
    incubations: list[dict[str, Any]],
    runs: list[dict[str, Any]],
) -> dict[str, Any]:
    presented_concepts: list[dict[str, Any]] = []
    for concept in concepts:
        presented = dict(concept)
        presented["concepts"] = _load_json_list(presented.get("concepts_json"))
        presented["ontological_moves"] = _load_json_list(
            presented.get("ontological_moves_json")
        )
        presented["bridge_lemmas"] = _load_json_list(
            presented.get("bridge_lemmas_json")
        )
        presented["self_test_results"] = _load_json_list(
            presented.get("self_test_results_json")
        )
        presented["signs_of_life"] = _load_json_list(
            presented.get("signs_of_life_json")
        )
        presented["negative_signs"] = _load_json_list(
            presented.get("negative_signs_json")
        )
        presented["super_universe_candidate"] = _load_json_object(
            presented.get("super_universe_json")
        )
        for key in (
            "compression_power",
            "fit_to_known_facts",
            "ontological_delta",
            "falsifiability",
            "bridgeability",
            "grounding_cost",
            "speculative_risk",
            "family_novelty",
            "transfer_value",
            "family_saturation_penalty",
        ):
            presented[key] = int(presented.get(key) or 0)
        presented["concept_family"] = str(presented.get("concept_family") or "")
        presented["family_kind"] = str(presented.get("family_kind") or "")
        presented["parent_family"] = str(presented.get("parent_family") or "")
        presented["why_not_same_as_existing_family"] = str(
            presented.get("why_not_same_as_existing_family") or ""
        )
        presented["smallest_transfer_probe"] = str(
            presented.get("smallest_transfer_probe") or ""
        )
        presented["universe_status"] = str(
            presented.get("universe_status") or "proposed"
        )
        presented["universe_thesis"] = str(presented.get("universe_thesis") or "")
        presented["conditional_theorem"] = str(
            presented.get("conditional_theorem") or ""
        )
        presented["invention_lesson"] = str(presented.get("invention_lesson") or "")
        presented["fact_links"] = list(presented.get("fact_links") or [])
        presented["tensions"] = list(presented.get("tensions") or [])
        presented["kill_tests"] = list(presented.get("kill_tests") or [])
        presented_concepts.append(presented)

    ranked_concepts = sorted(presented_concepts, key=_concept_sort_key, reverse=True)
    best_concept = dict(ranked_concepts[0]) if ranked_concepts else None

    pending_handoffs: list[dict[str, Any]] = []
    reviewed_handoffs: list[dict[str, Any]] = []
    for row in handoffs:
        presented = dict(row)
        presented["preview"] = _present_handoff(presented)
        if str(row.get("status") or "").lower() == "pending":
            pending_handoffs.append(presented)
        else:
            reviewed_handoffs.append(presented)

    active_incubations: list[dict[str, Any]] = []
    archived_incubations: list[dict[str, Any]] = []
    for row in incubations:
        presented = dict(row)
        presented["preview"] = _present_incubation(presented)
        status = str(row.get("status") or "").lower()
        if status in {"incubating", "operationalized"}:
            active_incubations.append(presented)
        else:
            archived_incubations.append(presented)

    next_step = _build_next_step(
        run_count=len(runs),
        pending_handoffs=len(pending_handoffs),
        active_incubations=len(active_incubations),
        best_concept=best_concept,
        concept_count=len(ranked_concepts),
    )
    latest_run = dict(runs[0]) if runs else None
    return {
        "supershadow_ranked_concepts": ranked_concepts,
        "supershadow_best_concept": best_concept,
        "supershadow_pending_handoffs": pending_handoffs,
        "supershadow_reviewed_handoffs": reviewed_handoffs,
        "supershadow_active_incubations": active_incubations,
        "supershadow_archived_incubations": archived_incubations,
        "supershadow_latest_run": latest_run,
        "supershadow_next_step": next_step,
        "supershadow_primary_cta": "Generate first sweep"
        if not runs
        else "Generate another sweep",
        "supershadow_metrics": {
            "pending_handoffs": len(pending_handoffs),
            "reviewed_handoffs": len(reviewed_handoffs),
            "active_incubations": len(active_incubations),
            "concept_count": len(ranked_concepts),
            "run_count": len(runs),
        },
    }
