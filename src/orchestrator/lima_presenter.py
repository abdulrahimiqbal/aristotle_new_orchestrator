from __future__ import annotations

import json
from typing import Any

from orchestrator.lima_steward import (
    build_lima_steward_view,
    count_true_pending_human_items,
)


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


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _truncate(value: Any, limit: int = 160) -> str:
    text = _clean_text(value)
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1].rstrip()}…"


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _status_weight(status: str) -> int:
    order = {
        "formalized": 7,
        "handed_off": 6,
        "promising": 5,
        "weakened": 3,
        "proposed": 2,
        "dead": 1,
    }
    return order.get(status, 0)


def _choice(*values: Any, fallback: str = "") -> str:
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return fallback


def _present_handoff(row: dict[str, Any]) -> dict[str, Any]:
    payload = _load_json(row.get("payload_json"), {})
    return {
        "title": str(payload.get("title") or row.get("universe_title") or "Lima handoff"),
        "destination_kind": str(row.get("destination_kind") or payload.get("destination_kind") or "review_packet"),
        "fracture_summary": str(payload.get("fracture_summary") or ""),
        "key_obligations": payload.get("key_obligations") or [],
        "payload_json_pretty": _pretty(row.get("payload_json")),
        "recommended_action": "Hold for obligations" if payload.get("key_obligations") else "Approve reviewed",
        "compact_summary": _choice(
            payload.get("fracture_summary"),
            payload.get("summary_md"),
            payload.get("why_now_md"),
            row.get("universe_title"),
            fallback="Review the packet evidence before promotion.",
        ),
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


def _top_candidate(
    universes: list[dict[str, Any]], fractures: list[dict[str, Any]]
) -> dict[str, Any]:
    fracture_by_universe: dict[str, list[dict[str, Any]]] = {}
    fracture_by_family: dict[str, list[dict[str, Any]]] = {}
    for fracture in fractures:
        universe_id = str(fracture.get("universe_id") or "")
        family_key = str(fracture.get("family_key") or "")
        if universe_id:
            fracture_by_universe.setdefault(universe_id, []).append(fracture)
        if family_key:
            fracture_by_family.setdefault(family_key, []).append(fracture)

    scored: list[tuple[float, dict[str, Any]]] = []
    for universe in universes:
        status = str(universe.get("universe_status") or "")
        score = _status_weight(status) * 10
        score += _as_float(universe.get("fit_score"))
        score += _as_float(universe.get("compression_score"))
        score += _as_float(universe.get("formalizability_score"))
        score += _as_float(universe.get("falsifiability_score")) * 0.5
        scored.append((score, universe))

    if not scored:
        return {
            "title": "No candidate yet",
            "status": "waiting",
            "family_key": "none",
            "thesis": "Run Lima to generate the first candidate universe.",
            "why_it_matters": "The strongest candidate will appear here once Lima has produced at least one world to test.",
            "top_weakness": "No fracture data yet.",
            "score_chips": [],
            "has_details": False,
        }

    _, universe = max(scored, key=lambda item: item[0])
    related_fractures = fracture_by_universe.get(str(universe.get("id") or ""), [])
    if not related_fractures:
        related_fractures = fracture_by_family.get(str(universe.get("family_key") or ""), [])
    top_fracture = max(
        related_fractures,
        key=lambda fracture: (_as_float(fracture.get("confidence")), str(fracture.get("created_at") or "")),
        default=None,
    )
    score_chips = [
        {"label": "fit", "value": universe.get("fit_score")},
        {"label": "compression", "value": universe.get("compression_score")},
        {"label": "formal", "value": universe.get("formalizability_score")},
    ]
    return {
        "id": universe.get("id"),
        "title": _choice(universe.get("title"), fallback="Untitled universe"),
        "status": str(universe.get("universe_status") or "proposed"),
        "family_key": _choice(universe.get("family_key"), fallback="unassigned"),
        "branch_of_math": _choice(universe.get("branch_of_math"), fallback="unknown domain"),
        "thesis": _truncate(_choice(universe.get("solved_world"), universe.get("title")), 180),
        "why_it_matters": _truncate(
            _choice(
                universe.get("why_problem_is_easy_here"),
                universe.get("solved_world"),
                universe.get("branch_of_math"),
            ),
            180,
        ),
        "top_weakness": _truncate(
            _choice(
                top_fracture.get("breakpoint_md") if top_fracture else "",
                top_fracture.get("failure_type") if top_fracture else "",
                fallback="No major fracture recorded for this family yet.",
            ),
            160,
        ),
        "top_weakness_label": _choice(
            top_fracture.get("failure_type") if top_fracture else "",
            fallback="weakness",
        ),
        "score_chips": [
            {"label": chip["label"], "value": _as_float(chip["value"])}
            for chip in score_chips
            if chip["value"] is not None and str(chip["value"]) != ""
        ][:3],
        "solved_world": _clean_text(universe.get("solved_world")),
        "why_easier": _clean_text(universe.get("why_problem_is_easy_here")),
        "lineage_pretty": _pretty(universe),
        "has_details": True,
    }


def _candidate_cards(
    universes: list[dict[str, Any]], fractures: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    fracture_by_universe: dict[str, list[dict[str, Any]]] = {}
    fracture_by_family: dict[str, list[dict[str, Any]]] = {}
    for fracture in fractures:
        universe_id = str(fracture.get("universe_id") or "")
        family_key = str(fracture.get("family_key") or "")
        if universe_id:
            fracture_by_universe.setdefault(universe_id, []).append(fracture)
        if family_key:
            fracture_by_family.setdefault(family_key, []).append(fracture)

    cards = []
    for universe in universes:
        related_fractures = fracture_by_universe.get(str(universe.get("id") or ""), [])
        if not related_fractures:
            related_fractures = fracture_by_family.get(str(universe.get("family_key") or ""), [])
        top_fracture = max(
            related_fractures,
            key=lambda fracture: (_as_float(fracture.get("confidence")), str(fracture.get("created_at") or "")),
            default=None,
        )
        score_chips = [
            {"label": "fit", "value": _as_float(universe.get("fit_score"))},
            {"label": "compression", "value": _as_float(universe.get("compression_score"))},
            {"label": "formal", "value": _as_float(universe.get("formalizability_score"))},
        ]
        cards.append(
            {
                **universe,
                "thesis": _truncate(
                    _choice(universe.get("solved_world"), universe.get("title")),
                    180,
                ),
                "why_it_matters": _truncate(
                    _choice(
                        universe.get("why_problem_is_easy_here"),
                        universe.get("solved_world"),
                        universe.get("branch_of_math"),
                    ),
                    180,
                ),
                "top_weakness": _truncate(
                    _choice(
                        top_fracture.get("breakpoint_md") if top_fracture else "",
                        top_fracture.get("failure_type") if top_fracture else "",
                        fallback="No major fracture recorded for this candidate yet.",
                    ),
                    170,
                ),
                "top_weakness_label": _choice(
                    top_fracture.get("failure_type") if top_fracture else "",
                    fallback="weakness",
                ),
                "score_chips": score_chips[:3],
                "lineage_pretty": _pretty(universe),
            }
        )
    return cards


def _top_blocker(
    fractures: list[dict[str, Any]],
    obligations: list[dict[str, Any]],
    literature_links: list[dict[str, Any]],
) -> dict[str, str]:
    if fractures:
        fracture = max(
            fractures,
            key=lambda item: (_as_float(item.get("confidence")), str(item.get("created_at") or "")),
        )
        return {
            "label": _choice(fracture.get("failure_type"), fallback="fracture"),
            "title": _choice(fracture.get("universe_title"), fallback="Candidate fracture"),
            "body": _truncate(
                _choice(
                    fracture.get("breakpoint_md"),
                    fracture.get("surviving_fragment_md"),
                    fallback="This fracture is the clearest reason Lima is not ready to promote the current frontier.",
                ),
                180,
            ),
            "tone": "risk",
        }

    prior_art_links = [
        link for link in literature_links if str(link.get("relation_kind") or "") == "prior_art"
    ]
    if prior_art_links:
        link = prior_art_links[0]
        return {
            "label": "prior art",
            "title": _choice(link.get("source_title"), fallback="Prior-art pressure"),
            "body": _truncate(
                _choice(
                    link.get("note"),
                    link.get("universe_title"),
                    fallback="Literature overlap is the biggest blocker right now.",
                ),
                180,
            ),
            "tone": "warning",
        }

    failed = [
        obligation
        for obligation in obligations
        if str(obligation.get("status") or "")
        in {"refuted_local", "refuted_formal", "inconclusive"}
    ]
    if failed:
        obligation = failed[0]
        return {
            "label": _choice(obligation.get("status"), fallback="failed obligation"),
            "title": _choice(obligation.get("title"), fallback="Obligation pressure"),
            "body": _truncate(
                _choice(
                    obligation.get("result_summary_md"),
                    obligation.get("prove_or_kill_md"),
                    fallback="A failed obligation is blocking promotion of the current candidate.",
                ),
                180,
            ),
            "tone": "warning",
        }

    return {
        "label": "clear enough",
        "title": "No dominant blocker surfaced",
        "body": "Lima has not surfaced a single overwhelming fracture, prior-art collision, or failed obligation yet.",
        "tone": "calm",
    }


def _safety_summary(
    obligations: list[dict[str, Any]], pending_handoffs: list[dict[str, Any]]
) -> dict[str, Any]:
    local_pending = len(
        [
            obligation
            for obligation in obligations
            if str(obligation.get("status") or "") in {"queued", "queued_local", "running_local"}
        ]
    )
    formal_pending = len(
        [
            obligation
            for obligation in obligations
            if str(obligation.get("status") or "")
            in {"queued_formal_review", "approved_for_formal", "submitted_formal"}
        ]
    )
    if pending_handoffs:
        headline = f"{len(pending_handoffs)} packet{'s' if len(pending_handoffs) != 1 else ''} gated"
        body = "Human review is still in front of any promotion. Approval remains review-only."
    elif local_pending or formal_pending:
        headline = "Checks are throttling progress"
        body = f"{local_pending} local and {formal_pending} formal obligation(s) are still acting as gates."
    else:
        headline = "No live authority by default"
        body = "Lima is in research-and-review mode unless a survivor explicitly clears later gates."
    return {
        "headline": headline,
        "body": body,
        "local_pending": local_pending,
        "formal_pending": formal_pending,
    }


def _workspace_progress(
    universes: list[dict[str, Any]],
    obligations: list[dict[str, Any]],
    handoffs: list[dict[str, Any]],
    *,
    top_candidate: dict[str, Any],
    top_blocker: dict[str, Any],
) -> dict[str, Any]:
    promising_statuses = {"promising", "formalized", "handed_off"}
    strong_survivor_statuses = {"formalized", "handed_off"}
    local_support_statuses = {"verified_local", "verified_formal"}
    formal_candidate_statuses = {"approved_for_formal", "submitted_formal", "verified_formal"}
    formal_result_statuses = {"verified_formal", "refuted_formal", "inconclusive"}

    milestones = [
        {
            "label": "frontier live",
            "done": bool(top_candidate.get("id"))
            or any(str(universe.get("universe_status") or "") in promising_statuses for universe in universes),
        },
        {
            "label": "strong survivor",
            "done": any(
                str(universe.get("universe_status") or "") in strong_survivor_statuses
                for universe in universes
            ),
        },
        {
            "label": "local evidence",
            "done": any(
                str(obligation.get("status") or "") in local_support_statuses
                for obligation in obligations
            ),
        },
        {
            "label": "formal candidate",
            "done": any(
                str(obligation.get("status") or "") in formal_candidate_statuses
                for obligation in obligations
            ),
        },
        {
            "label": "formal result",
            "done": any(
                str(obligation.get("status") or "") in formal_result_statuses
                for obligation in obligations
            ),
        },
        {
            "label": "human queue clear",
            "done": count_true_pending_human_items(handoffs=handoffs, obligations=obligations) == 0,
        },
        {
            "label": "blocker retired",
            "done": str(top_blocker.get("tone") or "") == "calm",
        },
    ]

    total = len(milestones)
    resolved = sum(1 for milestone in milestones if milestone["done"])
    progress_percent = int((100 * resolved) / total) if total else 0
    status_line = " · ".join(
        f"{milestone['label']} {'yes' if milestone['done'] else 'no'}"
        for milestone in milestones
    )
    pending_labels = [milestone["label"] for milestone in milestones if not milestone["done"]]
    return {
        "label": "Frontier milestones cleared",
        "resolved": resolved,
        "total": total,
        "progress_percent": progress_percent,
        "open": max(0, total - resolved),
        "status_line": status_line,
        "milestones": milestones,
        "caption": (
            "This is frontier progress, not completion progress. "
            + (
                f"Still open: {', '.join(pending_labels)}."
                if pending_labels
                else "The current frontier has cleared every tracked milestone."
            )
        ),
    }


def _problem_subsets(
    *,
    top_candidate: dict[str, Any],
    top_blocker: dict[str, Any],
    obligation_groups: list[dict[str, Any]],
    steward_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    needs_local = next(
        (group for group in obligation_groups if group.get("key") == "needs_local"),
        {"count": 0},
    )
    needs_human = next(
        (group for group in obligation_groups if group.get("key") == "needs_human"),
        {"count": 0},
    )
    closed = next(
        (group for group in obligation_groups if group.get("key") == "closed"),
        {"count": 0},
    )
    return [
        {
            "title": "Frontier candidate",
            "status": _choice(top_candidate.get("status"), fallback="waiting"),
            "summary": _choice(
                top_candidate.get("title"),
                fallback="No candidate frontier yet.",
            ),
            "detail": _choice(
                top_candidate.get("why_it_matters"),
                top_candidate.get("thesis"),
                fallback="Run Lima to generate the first candidate world.",
            ),
            "tone": "positive",
        },
        {
            "title": "Main blocker",
            "status": _choice(top_blocker.get("label"), fallback="clear enough"),
            "summary": _choice(top_blocker.get("title"), fallback="No dominant blocker"),
            "detail": _choice(
                top_blocker.get("body"),
                fallback="No fracture or failed obligation is dominating the frontier right now.",
            ),
            "tone": "warning" if top_blocker.get("tone") in {"risk", "warning"} else "neutral",
        },
        {
            "title": "Checks and reviews",
            "status": f"{needs_local.get('count', 0)} local / {needs_human.get('count', 0)} human",
            "summary": "Subproblems are being narrowed through obligation gates.",
            "detail": (
                f"{closed.get('count', 0)} obligation(s) are already closed; "
                f"{needs_local.get('count', 0)} still need machine checks and "
                f"{needs_human.get('count', 0)} still need review."
            ),
            "tone": "neutral",
        },
        {
            "title": "Escalated survivors",
            "status": f"{steward_summary.get('escalated_count', 0)} escalated",
            "summary": _choice(
                steward_summary.get("headline"),
                fallback="The steward is filtering the queue before it reaches you.",
            ),
            "detail": _choice(
                steward_summary.get("body"),
                fallback="Only the strongest or most ambiguous packets should stay visible here.",
            ),
            "tone": "positive" if steward_summary.get("escalated_count", 0) else "neutral",
        },
    ]


def _activity_feed(
    latest_run: dict[str, Any] | None,
    fractures: list[dict[str, Any]],
    obligations: list[dict[str, Any]],
    literature_links: list[dict[str, Any]],
) -> list[dict[str, str]]:
    feed: list[dict[str, str]] = []
    if latest_run:
        feed.append(
            {
                "kind": "Run",
                "title": _choice(latest_run.get("mode"), fallback="Latest Lima pass"),
                "body": _truncate(
                    _choice(
                        latest_run.get("run_summary_md"),
                        fallback="Latest run completed and is ready for inspection.",
                    ),
                    220,
                ),
                "meta": _choice(
                    f"{latest_run.get('created_at')} / {latest_run.get('trigger_kind')} / {latest_run.get('mode')}"
                ),
            }
        )
    if fractures:
        fracture = fractures[0]
        feed.append(
            {
                "kind": "Fracture",
                "title": _choice(
                    fracture.get("failure_type"),
                    fracture.get("universe_title"),
                    fallback="Newest fracture",
                ),
                "body": _truncate(
                    _choice(
                        fracture.get("breakpoint_md"),
                        fracture.get("surviving_fragment_md"),
                        fallback="A new fracture entered memory.",
                    ),
                    200,
                ),
                "meta": _choice(fracture.get("created_at"), fracture.get("family_key")),
            }
        )
    recent_obligations = [
        obligation
        for obligation in obligations
        if _choice(obligation.get("result_summary_md"), obligation.get("status"))
    ]
    if recent_obligations:
        obligation = recent_obligations[0]
        feed.append(
            {
                "kind": "Obligation",
                "title": _choice(obligation.get("title"), fallback="Newest obligation signal"),
                "body": _truncate(
                    _choice(
                        obligation.get("result_summary_md"),
                        obligation.get("prove_or_kill_md"),
                        fallback="An obligation changed state.",
                    ),
                    200,
                ),
                "meta": _choice(obligation.get("status"), obligation.get("created_at")),
            }
        )
    if literature_links:
        link = literature_links[0]
        feed.append(
            {
                "kind": "Literature",
                "title": _choice(link.get("source_title"), fallback="Literature signal"),
                "body": _truncate(
                    _choice(
                        link.get("note"),
                        link.get("universe_title"),
                        fallback="Literature memory linked a source to the frontier.",
                    ),
                    200,
                ),
                "meta": _choice(link.get("relation_kind"), link.get("created_at")),
            }
        )
    return feed


def _group_obligations(obligations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = {
        "needs_local": {
            "key": "needs_local",
            "title": "Needs local check",
            "statuses": {"queued", "queued_local", "running_local"},
            "items": [],
        },
        "needs_human": {
            "key": "needs_human",
            "title": "Needs human review",
            "statuses": {"queued_formal_review", "approved_for_formal", "inconclusive"},
            "items": [],
        },
        "formal": {
            "key": "formal",
            "title": "Formally queued/submitted",
            "statuses": {"submitted_formal"},
            "items": [],
        },
        "closed": {
            "key": "closed",
            "title": "Closed",
            "statuses": {"verified_local", "refuted_local", "verified_formal", "refuted_formal", "archived"},
            "items": [],
        },
    }

    for obligation in obligations:
        status = str(obligation.get("status") or "")
        for group in groups.values():
            if status in group["statuses"]:
                group["items"].append(obligation)
                break
        else:
            groups["needs_human"]["items"].append(obligation)

    for group in groups.values():
        items = []
        for obligation in group["items"]:
            next_step = "Inspect result"
            if group["key"] == "needs_local":
                next_step = "Run local check"
            elif group["key"] == "needs_human":
                next_step = "Review formal decision"
            elif group["key"] == "formal":
                next_step = "Inspect Aristotle status"
            elif group["key"] == "closed":
                next_step = "Archive or reuse lineage"
            items.append(
                {
                    **obligation,
                    "prove_or_kill_line": _truncate(
                        _choice(
                            obligation.get("prove_or_kill_md"),
                            obligation.get("statement_md"),
                            fallback="No prove-or-kill note recorded yet.",
                        ),
                        170,
                    ),
                    "value_cost": f"value {obligation.get('estimated_formalization_value') or 0} / cost {obligation.get('estimated_execution_cost') or 0}",
                    "recommended_next_step": next_step,
                    "summary_line": _truncate(
                        _choice(
                            obligation.get("result_summary_md"),
                            obligation.get("why_exists_md"),
                            obligation.get("statement_md"),
                        ),
                        180,
                    ),
                }
            )
        group["items"] = items
        group["count"] = len(items)
    return list(groups.values())


def _compact_literature_summary(
    sources: list[dict[str, Any]], links: list[dict[str, Any]]
) -> dict[str, Any]:
    source_by_title = {str(source.get("title") or ""): source for source in sources}
    prior_art_links = [link for link in links if str(link.get("relation_kind") or "") == "prior_art"]
    novelty_risk = "No novelty pressure recorded yet."
    novelty_tone = "calm"
    if prior_art_links:
        novelty_risk = f"{len(prior_art_links)} prior-art link(s) are pushing against novelty."
        novelty_tone = "warning"
    elif links:
        novelty_risk = f"{len(links)} literature link(s) exist, but none are marked as direct prior art."
    top_items = []
    ranked_links = sorted(
        links,
        key=lambda link: (
            1 if str(link.get("relation_kind") or "") == "prior_art" else 0,
            str(link.get("created_at") or ""),
        ),
        reverse=True,
    )
    for link in ranked_links[:5]:
        source = source_by_title.get(str(link.get("source_title") or ""), {})
        top_items.append(
            {
                "title": _choice(link.get("source_title"), fallback="Untitled source"),
                "relation_kind": _choice(link.get("relation_kind"), fallback="linked"),
                "why_it_matters": _truncate(
                    _choice(
                        link.get("note"),
                        link.get("universe_title"),
                        source.get("abstract_md"),
                        fallback="Linked because it shapes the novelty judgment.",
                    ),
                    180,
                ),
                "meta": _choice(
                    f"{source.get('venue')} / {source.get('year')}" if source.get("venue") or source.get("year") else "",
                    source.get("source_type"),
                ),
                "abstract": _clean_text(source.get("abstract_md")),
            }
        )
    if not top_items:
        for source in sources[:5]:
            top_items.append(
                {
                    "title": _choice(source.get("title"), fallback="Untitled source"),
                    "relation_kind": _choice(source.get("source_type"), fallback="source"),
                    "why_it_matters": _truncate(
                        _choice(
                            source.get("abstract_md"),
                            fallback="Available in literature memory for novelty review.",
                        ),
                        180,
                    ),
                    "meta": _choice(
                        f"{source.get('venue')} / {source.get('year')}" if source.get("venue") or source.get("year") else "",
                        source.get("source_type"),
                    ),
                    "abstract": _clean_text(source.get("abstract_md")),
                }
            )
    return {
        "novelty_risk": novelty_risk,
        "novelty_tone": novelty_tone,
        "top_items": top_items,
    }


def _governance_summary(
    families: list[dict[str, Any]], family_search_controls: list[dict[str, Any]]
) -> dict[str, Any]:
    cooled = [
        family
        for family in family_search_controls
        if str(family.get("governance_state") or family.get("search_action") or "")
        in {"hard_ban", "soft_ban", "cooldown", "retire"}
    ]
    repeated = sorted(
        families,
        key=lambda family: (
            int(family.get("repeat_failure_count") or 0),
            int(family.get("failure_count") or 0),
        ),
        reverse=True,
    )
    return {
        "cooled_count": len(cooled),
        "repeated_patterns": repeated[:6],
        "steering_families": family_search_controls[:8],
    }


def _primary_cta(
    pending_handoffs: list[dict[str, Any]], grouped_obligations: list[dict[str, Any]]
) -> dict[str, str]:
    needs_human = next(
        (group for group in grouped_obligations if group["key"] == "needs_human"),
        {"count": 0},
    )
    needs_local = next(
        (group for group in grouped_obligations if group["key"] == "needs_local"),
        {"count": 0},
    )
    if pending_handoffs:
        return {
            "kind": "anchor",
            "href": "#action-queue",
            "label": f"Review queue ({len(pending_handoffs)})",
            "summary": "Pending handoffs are waiting on a human decision.",
        }
    if needs_human["count"] or needs_local["count"]:
        return {
            "kind": "anchor",
            "href": "#obligations",
            "label": "Inspect obligations",
            "summary": "Checks are queued and should be inspected before more promotion.",
        }
    return {
        "kind": "run",
        "label": "Run Lima",
        "summary": "Start another pass from the current memory.",
    }


def _action_queue(
    pending_handoffs: list[dict[str, Any]],
    grouped_obligations: list[dict[str, Any]],
    formal_reviews: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for handoff in pending_handoffs[:4]:
        items.append(
            {
                "kind": "handoff",
                "title": handoff["preview"]["title"],
                "status": str(handoff.get("status") or "pending"),
                "summary": _truncate(handoff["preview"].get("compact_summary"), 150),
                "recommended_action": handoff["preview"].get("recommended_action", "Review packet"),
                "payload": handoff,
            }
        )
    needs_human = next(
        (group for group in grouped_obligations if group["key"] == "needs_human"),
        None,
    )
    if needs_human:
        for obligation in needs_human["items"][:3]:
            items.append(
                {
                    "kind": "obligation",
                    "title": obligation.get("title"),
                    "status": obligation.get("status"),
                    "summary": obligation.get("summary_line"),
                    "recommended_action": obligation.get("recommended_next_step"),
                    "payload": obligation,
                }
            )
    for review in formal_reviews[:2]:
        items.append(
            {
                "kind": "formal_review",
                "title": _choice(review.get("obligation_title"), fallback="Formal review packet"),
                "status": _choice(review.get("status"), fallback="queued"),
                "summary": _truncate(
                    _choice(
                        review.get("review_decision"),
                        review.get("rupture_summary_md"),
                        fallback="Formal review is waiting for inspection.",
                    ),
                    150,
                ),
                "recommended_action": "Inspect formal review",
                "payload": review,
            }
        )
    return items


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
    top_candidate = _top_candidate(universes, fractures)
    top_blocker = _top_blocker(fractures, obligations, literature_links)
    safety_summary = _safety_summary(obligations, pending_handoffs)
    activity_feed = _activity_feed(latest_run, fractures, obligations, literature_links)
    candidate_cards = _candidate_cards(universes, fractures)
    obligation_groups = _group_obligations(obligations)
    literature_summary = _compact_literature_summary(
        snapshot.get("literature_sources") or [], literature_links
    )
    governance_summary = _governance_summary(families, family_search_controls)
    action_queue = _action_queue(pending_handoffs, obligation_groups, formal_reviews)
    steward_view = build_lima_steward_view(
        pending_handoffs=pending_handoffs,
        obligations=obligations,
        fractures=fractures,
        top_candidate=top_candidate,
        top_blocker=top_blocker,
    )
    workspace_progress = _workspace_progress(
        universes,
        obligations,
        handoffs,
        top_candidate=top_candidate,
        top_blocker=top_blocker,
    )
    problem_subsets = _problem_subsets(
        top_candidate=top_candidate,
        top_blocker=top_blocker,
        obligation_groups=obligation_groups,
        steward_summary=steward_view["summary"],
    )
    primary_cta = _primary_cta(pending_handoffs, obligation_groups)
    if steward_view["summary"]["escalated_count"] > 0:
        primary_cta = {
            "kind": "anchor",
            "href": "#steward-review",
            "label": f"Review escalations ({steward_view['summary']['escalated_count']})",
            "summary": steward_view["summary"]["headline"],
        }
    now_summary = _choice(
        steward_view["summary"]["body"],
        decision_state.get("body"),
        latest_summary,
        fallback="Lima is ready for the next research checkpoint.",
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
        "lima_primary_cta": primary_cta,
        "lima_decision_state": decision_state,
        "lima_review_guidance": review_guidance,
        "lima_top_candidate": top_candidate,
        "lima_candidate_cards": candidate_cards,
        "lima_top_blocker": top_blocker,
        "lima_safety_summary": safety_summary,
        "lima_workspace_progress": workspace_progress,
        "lima_problem_subsets": problem_subsets,
        "lima_activity_feed": activity_feed,
        "lima_obligation_groups": obligation_groups,
        "lima_literature_summary": literature_summary,
        "lima_governance_summary": governance_summary,
        "lima_action_queue": action_queue,
        "lima_steward_summary": steward_view["summary"],
        "lima_steward_packets": steward_view["packets"],
        "lima_handoff_bundles": steward_view["handoff_bundles"],
        "lima_obligation_bundles": steward_view["obligation_bundles"],
        "lima_now_summary": now_summary,
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
            "steward_escalated": steward_view["summary"]["escalated_count"],
            "steward_bundled": steward_view["summary"]["bundled_count"],
            "steward_auto_managed": steward_view["summary"]["auto_managed_count"],
            "artifacts": len(artifacts),
            "literature_sources": len(snapshot.get("literature_sources") or []),
            "policy_layers": len(policy_layers),
            "transfer_metrics": len(transfer_metrics),
        },
    }
