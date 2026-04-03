from __future__ import annotations

import json
from typing import Any


def _load_payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _hypothesis_sort_key(hypothesis: dict[str, Any]) -> tuple[int, int, int, str]:
    tier_rank = {"A": 3, "B": 2, "C": 1}.get(
        str(hypothesis.get("groundability_tier") or "").upper(),
        0,
    )
    return (
        int(hypothesis.get("score_0_100") or 0),
        tier_rank,
        len(hypothesis.get("evidence_rows") or []),
        str(hypothesis.get("created_at") or ""),
    )


def _present_promotion(row: dict[str, Any]) -> dict[str, Any]:
    payload = _load_payload(row.get("payload_json"))
    kind = str(payload.get("kind") or "").strip().lower()
    pretty = str(row.get("payload_json") or "{}")
    if payload:
        pretty = json.dumps(payload, indent=2, ensure_ascii=False)
    preview = {
        "kind": kind or "promotion",
        "action_label": "Review promotion",
        "headline": "Review this promotion payload",
        "summary": "Check whether this should become live work.",
        "campaign_id": str(payload.get("campaign_id") or ""),
        "target_id": str(payload.get("target_id") or ""),
        "move_kind": str(payload.get("move_kind") or ""),
        "move_note": str(payload.get("move_note") or ""),
        "submit_behavior": "",
        "payload_json_pretty": pretty,
    }
    if kind == "new_target":
        preview["action_label"] = "Create live target"
        preview["headline"] = str(payload.get("description") or "No target description provided.")
        preview["summary"] = "Adds a concrete target to the live campaign."
        return preview
    if kind == "new_experiment":
        preview["action_label"] = "Launch live experiment"
        preview["headline"] = str(payload.get("objective") or "No experiment objective provided.")
        preview["summary"] = "Creates a live experiment tied to an existing target."
        preview["submit_behavior"] = (
            "Wait for the manager tick"
            if payload.get("defer_aristotle_submit")
            else "Submit to Aristotle immediately after approval"
        )
        return preview
    return preview


def _build_next_step(
    *,
    run_count: int,
    pending_promotions: int,
    best_hypothesis: dict[str, Any] | None,
    hypothesis_count: int,
) -> dict[str, str]:
    if pending_promotions > 0:
        return {
            "title": "Review the live promotion queue",
            "body": (
                f"{pending_promotions} proposal(s) are waiting. Approve only the items "
                "that deserve real targets or experiments."
            ),
        }
    if run_count == 0:
        return {
            "title": "Generate the first idea batch",
            "body": (
                "Run the shadow lab to get speculative leads, then review anything that "
                "looks concrete enough to promote."
            ),
        }
    if best_hypothesis and int(best_hypothesis.get("score_0_100") or 0) >= 70:
        return {
            "title": "Pressure-test the strongest idea",
            "body": (
                "Start with the highest-scoring hypothesis below. Its kill test tells you "
                "the fastest way to falsify or promote it."
            ),
        }
    if hypothesis_count > 0:
        return {
            "title": "Broaden the search",
            "body": (
                "There are ideas on the board, but nothing looks obviously live-ready yet. "
                "Run again after new evidence lands or when you want a wider sweep."
            ),
        }
    return {
        "title": "Generate another batch",
        "body": "Keep iterating until a hypothesis turns into a concrete live action.",
    }


def build_shadow_ui_context(
    *,
    hypotheses: list[dict[str, Any]],
    promotions: list[dict[str, Any]],
    runs: list[dict[str, Any]],
) -> dict[str, Any]:
    presented_hypotheses: list[dict[str, Any]] = []
    for hypothesis in hypotheses:
        presented = dict(hypothesis)
        presented["score_0_100"] = int(presented.get("score_0_100") or 0)
        presented["groundability_tier"] = str(presented.get("groundability_tier") or "")
        presented["kill_test"] = str(presented.get("kill_test") or "")
        presented["evidence_rows"] = list(presented.get("evidence_rows") or [])
        presented_hypotheses.append(presented)
    ranked_hypotheses = sorted(presented_hypotheses, key=_hypothesis_sort_key, reverse=True)
    best_hypothesis = dict(ranked_hypotheses[0]) if ranked_hypotheses else None
    pending_promotions: list[dict[str, Any]] = []
    reviewed_promotions: list[dict[str, Any]] = []
    for row in promotions:
        presented = dict(row)
        presented["preview"] = _present_promotion(presented)
        if str(row.get("status") or "").lower() == "pending":
            pending_promotions.append(presented)
        else:
            reviewed_promotions.append(presented)
    next_step = _build_next_step(
        run_count=len(runs),
        pending_promotions=len(pending_promotions),
        best_hypothesis=best_hypothesis,
        hypothesis_count=len(ranked_hypotheses),
    )
    latest_run = dict(runs[0]) if runs else None
    return {
        "shadow_ranked_hypotheses": ranked_hypotheses,
        "shadow_best_hypothesis": best_hypothesis,
        "shadow_pending_promotions": pending_promotions,
        "shadow_reviewed_promotions": reviewed_promotions,
        "shadow_latest_run": latest_run,
        "shadow_next_step": next_step,
        "shadow_primary_cta": "Generate first batch" if not runs else "Generate another batch",
        "shadow_metrics": {
            "pending_promotions": len(pending_promotions),
            "reviewed_promotions": len(reviewed_promotions),
            "hypothesis_count": len(ranked_hypotheses),
            "run_count": len(runs),
        },
    }
