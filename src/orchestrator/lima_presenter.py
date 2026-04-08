from __future__ import annotations

import json
from typing import Any


def _load_json(raw: Any, default: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


def _pretty(raw: Any) -> str:
    parsed = _load_json(raw, {})
    try:
        return json.dumps(parsed, indent=2, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(raw or "")


def _present_handoff(row: dict[str, Any]) -> dict[str, Any]:
    payload = _load_json(row.get("payload_json"), {})
    return {
        "title": str(payload.get("title") or row.get("universe_title") or "Lima handoff"),
        "destination_kind": str(row.get("destination_kind") or payload.get("destination_kind") or "review_packet"),
        "fracture_summary": str(payload.get("fracture_summary") or ""),
        "key_obligations": payload.get("key_obligations") or [],
        "payload_json_pretty": _pretty(row.get("payload_json")),
    }


def _decision_state(
    *,
    latest_run: dict[str, Any] | None,
    pending_handoffs: list[dict[str, Any]],
    universes: list[dict[str, Any]],
    queued_obligations: list[dict[str, Any]],
) -> dict[str, str]:
    if pending_handoffs:
        count = len(pending_handoffs)
        return {
            "label": "Review needed",
            "title": f"{count} handoff packet{'s' if count != 1 else ''} waiting",
            "body": "Read the recommendation, then hold, approve, or reject. Approval keeps it as a reviewed packet and does not create a live Aristotle job.",
            "tone": "review",
            "next_title": "Review the handoff queue",
            "next_body": "Start with the queued packet and its formal obligations before treating the idea as ready for promotion.",
        }
    if latest_run:
        obligation_count = len(queued_obligations)
        universe_count = len(universes)
        return {
            "label": "Idle",
            "title": "Latest run is ready to inspect",
            "body": f"Lima has {universe_count} candidate universe{'s' if universe_count != 1 else ''} and {obligation_count} queued obligation{'s' if obligation_count != 1 else ''} for this problem.",
            "tone": "idle",
            "next_title": "Run another search pass",
            "next_body": "Use another pass when you want Lima to explore, stress-test, or formalize from the current memory.",
        }
    return {
        "label": "Ready",
        "title": "Ready for the first Lima run",
        "body": "Pick a mode and run Lima. It will invent candidate universes, attack them, queue formal obligations, and create review packets only when there is something to judge.",
        "tone": "ready",
        "next_title": "Run the first search pass",
        "next_body": "This creates the first research memory layer for the selected problem.",
    }


def _review_guidance(
    *,
    pending_handoffs: list[dict[str, Any]],
    queued_obligations: list[dict[str, Any]],
    fractures: list[dict[str, Any]],
) -> dict[str, Any]:
    if not pending_handoffs:
        return {}

    warning_text = " ".join(
        [
            str(h.get("preview", {}).get("fracture_summary") or "")
            for h in pending_handoffs
        ]
        + [str(f.get("failure_type") or "") for f in fractures]
        + [str(f.get("breakpoint_md") or "") for f in fractures]
    ).lower()
    has_prior_art_pressure = "prior_art" in warning_text or "prior-art" in warning_text

    if has_prior_art_pressure and queued_obligations:
        body = (
            "Hold the packet for obligation checks. Lima found prior-art pressure, so the useful next move is to test the narrow finite and Lean obligations before approval."
        )
        bullets = [
            f"Inspect {len(queued_obligations)} queued obligation(s).",
            "Check novelty against the literature memory.",
            "Reject if the quotient move only restates known 3x+1 cycle or residue work.",
        ]
    elif queued_obligations:
        body = (
            "Hold the packet until the queued obligations are inspected. The packet may be useful, but the next decision should be grounded in the formal checks."
        )
        bullets = [
            f"Inspect {len(queued_obligations)} queued obligation(s).",
            "Approve only if the obligations remain narrow and non-vacuous.",
            "Approval still does not create a live Aristotle job.",
        ]
    else:
        body = (
            "Review the packet before approval. There are no queued obligations attached, so the decision depends on the packet evidence and fracture summary."
        )
        bullets = [
            "Check the fracture summary.",
            "Approve only if the packet has a concrete next test.",
            "Approval still does not create a live Aristotle job.",
        ]

    return {
        "label": "Recommendation",
        "title": "Hold for obligation review" if queued_obligations else "Review before approval",
        "body": body,
        "bullets": bullets,
        "primary_action_label": "Hold for obligations" if queued_obligations else "Hold for review",
    }


def build_lima_ui_context(snapshot: dict[str, Any], *, lima_flash: dict | None = None) -> dict[str, Any]:
    state = dict(snapshot.get("state") or {})
    latest_run = dict(snapshot.get("latest_run") or {}) if snapshot.get("latest_run") else None
    universes = [dict(u) for u in snapshot.get("universes") or []]
    handoffs = [dict(h) for h in snapshot.get("handoffs") or []]
    pending_handoffs: list[dict[str, Any]] = []
    reviewed_handoffs: list[dict[str, Any]] = []
    for handoff in handoffs:
        handoff["preview"] = _present_handoff(handoff)
        if str(handoff.get("status") or "") == "pending":
            pending_handoffs.append(handoff)
        else:
            reviewed_handoffs.append(handoff)

    fractures = [dict(f) for f in snapshot.get("fractures") or []]
    obligations = [dict(o) for o in snapshot.get("obligations") or []]
    for obligation in obligations:
        obligation["lineage"] = _load_json(obligation.get("lineage_json"), {})
        obligation["formal_payload"] = _load_json(obligation.get("formal_payload_json"), {})
        obligation["formal_submission_ref"] = _load_json(
            obligation.get("formal_submission_ref_json"), {}
        )
        obligation["aristotle_ref"] = _load_json(obligation.get("aristotle_ref_json"), {})
    literature_extracts = [dict(e) for e in snapshot.get("literature_extracts") or []]
    literature_links = [dict(e) for e in snapshot.get("literature_links") or []]
    formal_reviews = [dict(r) for r in snapshot.get("formal_reviews") or []]
    artifacts = [dict(a) for a in snapshot.get("artifacts") or []]
    families = [dict(f) for f in snapshot.get("families") or []]
    for family in families:
        family["required_delta"] = _load_json(family.get("required_delta_json"), [])
        family["governance_evidence"] = _load_json(family.get("governance_evidence_json"), {})
    family_search_controls = [
        f
        for f in families
        if str(f.get("search_action") or "") in {"mutate", "cooldown", "retire"}
        or str(f.get("governance_state") or "") in {"hard_ban", "soft_ban", "cooldown", "explore"}
    ]
    policy_layers = [dict(layer) for layer in snapshot.get("policy_layers") or []]
    for layer in policy_layers:
        layer["policy"] = _load_json(layer.get("policy_json"), {})
        layer["evidence"] = _load_json(layer.get("evidence_json"), {})
    transfer_metrics = [dict(metric) for metric in snapshot.get("transfer_metrics") or []]
    for metric in transfer_metrics:
        metric["metric"] = _load_json(metric.get("metric_json"), {})
    artifact_counts: dict[str, int] = {}
    for artifact in artifacts:
        kind = str(artifact.get("artifact_kind") or "artifact")
        artifact_counts[kind] = artifact_counts.get(kind, 0) + 1

    promising = [u for u in universes if str(u.get("universe_status")) in {"promising", "formalized", "handed_off"}]
    latest_summary = ""
    if latest_run:
        latest_summary = str(latest_run.get("run_summary_md") or "")
    queued_obligations = [
        o for o in obligations if str(o.get("status") or "") in {"queued", "queued_local", "queued_formal_review"}
    ]
    local_obligations = [
        o for o in obligations if str(o.get("status") or "") in {"queued", "queued_local", "running_local", "verified_local", "refuted_local"}
    ]
    formal_obligations = [
        o
        for o in obligations
        if str(o.get("status") or "") in {
            "queued_formal_review",
            "approved_for_formal",
            "submitted_formal",
            "verified_formal",
            "refuted_formal",
            "inconclusive",
        }
    ]
    decision_state = _decision_state(
        latest_run=latest_run,
        pending_handoffs=pending_handoffs,
        universes=universes,
        queued_obligations=queued_obligations,
    )
    review_guidance = _review_guidance(
        pending_handoffs=pending_handoffs,
        queued_obligations=queued_obligations,
        fractures=fractures,
    )

    return {
        "lima_problem": snapshot.get("problem") or {},
        "lima_problems": snapshot.get("problems") or [],
        "lima_state": state,
        "lima_frontier_pretty": _pretty(state.get("frontier_json")),
        "lima_pressure_pretty": _pretty(state.get("pressure_map_json")),
        "lima_policy_pretty": _pretty(state.get("policy_json")),
        "lima_latest_run": latest_run,
        "lima_latest_summary": latest_summary,
        "lima_runs": snapshot.get("runs") or [],
        "lima_families": families,
        "lima_family_search_controls": family_search_controls,
        "lima_universes": universes,
        "lima_promising_universes": promising,
        "lima_fractures": fractures,
        "lima_obligations": obligations,
        "lima_queued_obligations": queued_obligations,
        "lima_local_obligations": local_obligations,
        "lima_formal_obligations": formal_obligations,
        "lima_literature_sources": snapshot.get("literature_sources") or [],
        "lima_literature_extracts": literature_extracts,
        "lima_literature_links": literature_links,
        "lima_formal_reviews": formal_reviews,
        "lima_artifacts": artifacts,
        "lima_artifact_counts": artifact_counts,
        "lima_pending_handoffs": pending_handoffs,
        "lima_reviewed_handoffs": reviewed_handoffs,
        "lima_policy_revisions": snapshot.get("policy_revisions") or [],
        "lima_policy_layers": policy_layers,
        "lima_transfer_metrics": transfer_metrics,
        "lima_flash": lima_flash,
        "lima_primary_cta": "Run Lima",
        "lima_decision_state": decision_state,
        "lima_review_guidance": review_guidance,
        "lima_modes": [
            {
                "value": "balanced",
                "label": "Balanced",
                "hint": "general search pass",
            },
            {
                "value": "wild",
                "label": "Wild",
                "hint": "broader invention",
            },
            {
                "value": "stress",
                "label": "Stress",
                "hint": "break candidates harder",
            },
            {
                "value": "forge",
                "label": "Forge",
                "hint": "push toward formal obligations",
            },
        ],
        "lima_metrics": {
            "run_count": len(snapshot.get("runs") or []),
            "family_count": len(snapshot.get("families") or []),
            "universe_count": len(universes),
            "fracture_count": len(fractures),
            "queued_obligations": len(queued_obligations),
            "local_obligations": len(local_obligations),
            "formal_obligations": len(formal_obligations),
            "pending_handoffs": len(pending_handoffs),
            "reviewed_handoffs": len(reviewed_handoffs),
            "formal_reviews": len(formal_reviews),
            "artifacts": len(artifacts),
            "literature_sources": len(snapshot.get("literature_sources") or []),
            "policy_layers": len(policy_layers),
            "transfer_metrics": len(transfer_metrics),
        },
    }
