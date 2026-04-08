from __future__ import annotations

import re
from typing import Any

_NON_WORD_RE = re.compile(r"[^a-z0-9]+")
LOCAL_CHECK_KINDS = {"finite_check", "counterexample_search", "invariant_check", "consistency"}
FORMAL_REVIEW_KINDS = {"lean_goal", "bridge_lemma", "equivalence"}


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _truncate(value: Any, limit: int = 180) -> str:
    text = _clean_text(value)
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1].rstrip()}..."


def _slug(value: Any) -> str:
    lowered = _clean_text(value).lower()
    if not lowered:
        return ""
    return _NON_WORD_RE.sub("-", lowered).strip("-")


def _choice(*values: Any, fallback: str = "") -> str:
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return fallback


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _status_group(status: str) -> str:
    if status in {"queued", "queued_local", "running_local"}:
        return "needs_local"
    if status in {"queued_formal_review", "approved_for_formal", "inconclusive"}:
        return "needs_human"
    if status in {"submitted_formal"}:
        return "formal"
    return "closed"


def _bundle_handoffs(pending_handoffs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for handoff in pending_handoffs:
        preview = dict(handoff.get("preview") or {})
        obligations = preview.get("key_obligations") or []
        obligation_fingerprint = "|".join(
            sorted(_slug(item.get("title") or item.get("statement_md") or "") for item in obligations if item)
        )
        fingerprint = "::".join(
            [
                _slug(handoff.get("destination_kind") or preview.get("destination_kind") or ""),
                _slug(preview.get("title") or handoff.get("universe_title") or ""),
                obligation_fingerprint or _slug(preview.get("fracture_summary") or ""),
            ]
        )
        fingerprint = fingerprint or str(handoff.get("id") or "")
        grouped.setdefault(fingerprint, []).append(handoff)

    bundles: list[dict[str, Any]] = []
    for items in grouped.values():
        ordered = sorted(items, key=lambda item: str(item.get("created_at") or ""), reverse=True)
        primary = ordered[0]
        preview = dict(primary.get("preview") or {})
        key_obligations = preview.get("key_obligations") or []
        fracture_summary = _truncate(preview.get("fracture_summary"), 180)
        why_now = fracture_summary or _truncate(
            f"{len(ordered)} similar packet(s) are pointing at the same survivor trajectory.",
            180,
        )
        risk_lines = []
        if key_obligations:
            risk_lines.extend(
                _truncate(item.get("title") or item.get("statement_md"), 100)
                for item in key_obligations[:3]
            )
        if fracture_summary:
            risk_lines.append(fracture_summary)
        passed_lines = []
        if len(ordered) > 1:
            passed_lines.append(f"Bundled {len(ordered)} similar handoff packets into one review object.")
        passed_lines.append("Reached handoff stage without creating any live Aristotle work.")
        if not key_obligations:
            passed_lines.append("No key obligation was attached to the packet preview.")
        confidence = 78
        if key_obligations:
            confidence -= 16
        if "prior_art" in why_now.lower() or "prior-art" in why_now.lower():
            confidence -= 12
        bundles.append(
            {
                "kind": "handoff",
                "group": "escalated",
                "bundle_size": len(ordered),
                "primary_handoff_id": str(primary.get("id") or ""),
                "primary": primary,
                "title": _choice(preview.get("title"), primary.get("universe_title"), fallback="Lima handoff"),
                "status": _choice(primary.get("status"), fallback="pending"),
                "destination_kind": _choice(preview.get("destination_kind"), primary.get("destination_kind")),
                "summary": _truncate(preview.get("compact_summary"), 170),
                "why_now": why_now,
                "passed_lines": passed_lines[:3],
                "risk_lines": risk_lines[:3],
                "recommended_action": "Review bundle" if len(ordered) > 1 else "Review packet",
                "confidence_score": max(25, min(95, confidence)),
                "raw_items": ordered,
            }
        )

    return sorted(
        bundles,
        key=lambda bundle: (bundle["confidence_score"], bundle["bundle_size"], bundle["title"]),
        reverse=True,
    )


def _bundle_obligations(obligations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for obligation in obligations:
        key = _choice(
            obligation.get("canonical_hash"),
            "::".join(
                [
                    _slug(obligation.get("obligation_kind")),
                    _slug(obligation.get("title")),
                    _slug(obligation.get("universe_title")),
                    _slug(obligation.get("prove_or_kill_md") or obligation.get("statement_md")),
                ]
            ),
        )
        grouped.setdefault(key, []).append(obligation)

    bundles: list[dict[str, Any]] = []
    for items in grouped.values():
        ordered = sorted(
            items,
            key=lambda item: (
                int(item.get("priority") or 0),
                _as_float(item.get("estimated_formalization_value")),
                str(item.get("created_at") or ""),
            ),
            reverse=True,
        )
        primary = ordered[0]
        status = str(primary.get("status") or "")
        group = _status_group(status)
        prove_or_kill = _truncate(
            _choice(
                primary.get("prove_or_kill_md"),
                primary.get("statement_md"),
                fallback="No prove-or-kill line recorded yet.",
            ),
            160,
        )
        summary = _truncate(
            _choice(
                primary.get("result_summary_md"),
                primary.get("why_exists_md"),
                primary.get("statement_md"),
            ),
            160,
        )
        passed_lines = []
        if len(ordered) > 1:
            passed_lines.append(f"Bundled {len(ordered)} similar obligations into one queue item.")
        if status in {"approved_for_formal", "submitted_formal"}:
            passed_lines.append("This obligation has already cleared part of the formal escalation path.")
        elif status == "queued_formal_review":
            passed_lines.append("This obligation survived local compilation and is waiting for formal review.")
        elif status in {"verified_local", "verified_formal"}:
            passed_lines.append("This obligation already has a positive result on record.")
        risk_lines = [prove_or_kill]
        if summary and summary != prove_or_kill:
            risk_lines.append(summary)
        value = _as_float(primary.get("estimated_formalization_value"))
        cost = _as_float(primary.get("estimated_execution_cost"))
        confidence = 45 + int(value * 8) - int(cost * 4)
        if status in {"approved_for_formal", "submitted_formal"}:
            confidence += 16
        if status in {"inconclusive", "refuted_local", "refuted_formal"}:
            confidence -= 12
        if str(primary.get("obligation_kind") or "") in FORMAL_REVIEW_KINDS:
            confidence += 8
        recommended_action = {
            "needs_local": "Let Lima run the check",
            "needs_human": "Inspect obligation",
            "formal": "Check Aristotle result",
            "closed": "Keep for lineage only",
        }[group]
        bundles.append(
            {
                "kind": "obligation",
                "group": group,
                "bundle_size": len(ordered),
                "primary_obligation_id": str(primary.get("id") or ""),
                "primary": primary,
                "title": _choice(primary.get("title"), fallback="Lima obligation"),
                "status": status or "queued",
                "obligation_kind": _choice(primary.get("obligation_kind"), fallback="obligation"),
                "summary": summary,
                "why_now": prove_or_kill,
                "passed_lines": passed_lines[:3],
                "risk_lines": risk_lines[:3],
                "recommended_action": recommended_action,
                "confidence_score": max(20, min(95, confidence)),
                "value_cost": f"value {primary.get('estimated_formalization_value') or 0} / cost {primary.get('estimated_execution_cost') or 0}",
                "raw_items": ordered,
            }
        )

    return sorted(
        bundles,
        key=lambda bundle: (bundle["confidence_score"], bundle["bundle_size"], bundle["title"]),
        reverse=True,
    )


def _build_blocker_packet(top_blocker: dict[str, Any], fractures: list[dict[str, Any]]) -> dict[str, Any] | None:
    tone = str(top_blocker.get("tone") or "")
    if tone not in {"risk", "warning"}:
        return None
    blocker_support = fractures[0] if fractures else {}
    return {
        "kind": "blocker",
        "group": "blocked",
        "bundle_size": 1,
        "title": _choice(top_blocker.get("title"), fallback="Dominant blocker"),
        "status": _choice(top_blocker.get("label"), fallback="blocked"),
        "summary": _truncate(top_blocker.get("body"), 170),
        "why_now": _truncate(
            _choice(
                blocker_support.get("breakpoint_md"),
                top_blocker.get("body"),
                fallback="This is the strongest reason the current frontier has not earned promotion.",
            ),
            180,
        ),
        "passed_lines": ["Surfaced because it dominates the current queue more than any survivor packet does."],
        "risk_lines": [
            _truncate(
                _choice(
                    blocker_support.get("surviving_fragment_md"),
                    top_blocker.get("body"),
                ),
                120,
            )
        ],
        "recommended_action": "Inspect blocker",
        "confidence_score": 58,
        "raw_items": [blocker_support] if blocker_support else [],
    }


def build_lima_steward_view(
    *,
    pending_handoffs: list[dict[str, Any]],
    obligations: list[dict[str, Any]],
    fractures: list[dict[str, Any]],
    top_candidate: dict[str, Any],
    top_blocker: dict[str, Any],
) -> dict[str, Any]:
    handoff_bundles = _bundle_handoffs(pending_handoffs)
    obligation_bundles = _bundle_obligations(obligations)
    escalated_obligations = [
        bundle
        for bundle in obligation_bundles
        if bundle["group"] == "needs_human" and bundle["confidence_score"] >= 54
    ]
    packets = handoff_bundles[:3] + escalated_obligations[:2]
    blocker_packet = _build_blocker_packet(top_blocker, fractures)
    if blocker_packet:
        packets.append(blocker_packet)
    packets = sorted(
        packets,
        key=lambda packet: (packet["confidence_score"], packet["bundle_size"], packet["title"]),
        reverse=True,
    )[:4]

    bundled_handoffs = sum(max(0, bundle["bundle_size"] - 1) for bundle in handoff_bundles)
    bundled_obligations = sum(max(0, bundle["bundle_size"] - 1) for bundle in obligation_bundles)
    hidden_bundles = [
        bundle
        for bundle in obligation_bundles
        if bundle not in escalated_obligations and bundle["group"] in {"needs_local", "formal", "closed"}
    ]
    auto_managed_count = sum(bundle["bundle_size"] for bundle in hidden_bundles)
    blocked_count = sum(1 for packet in packets if packet["kind"] == "blocker")

    if packets:
        headline = f"{len(packets)} escalated item(s) made it through steward triage."
        body = (
            "Lima is bundling similar queue items and showing you only the survivors, blockers, "
            "and ambiguous formal-review packets that still need a human decision."
        )
    elif auto_managed_count:
        headline = "Routine queue motion is being handled below the fold."
        body = (
            "No high-confidence survivor packet currently needs you. The remaining queue is mostly "
            "local checks, formal tracking, or archived lineage."
        )
    else:
        headline = "No escalations waiting."
        body = "Lima has not produced a survivor strong enough to interrupt you right now."

    best_candidate_line = _truncate(
        _choice(
            top_candidate.get("title"),
            top_candidate.get("thesis"),
            fallback="No candidate has separated itself from the pack yet.",
        ),
        120,
    )

    return {
        "summary": {
            "headline": headline,
            "body": body,
            "escalated_count": len(packets),
            "bundled_count": bundled_handoffs + bundled_obligations,
            "auto_managed_count": auto_managed_count,
            "blocked_count": blocked_count,
            "best_candidate_line": best_candidate_line,
        },
        "packets": packets,
        "handoff_bundles": handoff_bundles,
        "obligation_bundles": obligation_bundles,
    }
