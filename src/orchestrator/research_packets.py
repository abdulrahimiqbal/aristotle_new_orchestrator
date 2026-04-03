"""Campaign-scoped research packet parsing and formatting."""

from __future__ import annotations

import json
from typing import Any


def _clip(text: Any, limit: int) -> str:
    if text is None:
        return ""
    return str(text).strip()[:limit]


def _coerce_str_list(raw: Any, *, max_items: int, max_len: int) -> list[str]:
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for item in raw[:max_items]:
        text = _clip(item, max_len)
        if text:
            out.append(text)
    return out


def _coerce_reference_list(raw: Any) -> list[dict[str, str]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, str]] = []
    for item in raw[:12]:
        if not isinstance(item, dict):
            continue
        title = _clip(item.get("title"), 200)
        url = _clip(item.get("url"), 500)
        note = _clip(item.get("note"), 500)
        if not (title or url or note):
            continue
        entry: dict[str, str] = {}
        if title:
            entry["title"] = title
        if url:
            entry["url"] = url
        if note:
            entry["note"] = note
        out.append(entry)
    return out


def _coerce_attack_families(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw[:8]:
        if not isinstance(item, dict):
            continue
        title = _clip(item.get("title"), 200)
        family_id = _clip(item.get("id"), 80)
        if not (title or family_id):
            continue
        family: dict[str, Any] = {
            "id": family_id or title.lower().replace(" ", "_")[:80],
            "title": title or family_id,
        }
        status = _clip(item.get("status"), 32).lower()
        if status:
            family["status"] = status
        why_now = _clip(item.get("why_now"), 800)
        if why_now:
            family["why_now"] = why_now
        fronts = _coerce_str_list(item.get("fronts"), max_items=12, max_len=120)
        if fronts:
            family["fronts"] = fronts
        steps = _coerce_str_list(item.get("steps"), max_items=8, max_len=300)
        if steps:
            family["steps"] = steps
        lemma_templates = _coerce_str_list(
            item.get("lemma_templates"), max_items=8, max_len=300
        )
        if lemma_templates:
            family["lemma_templates"] = lemma_templates
        experiment_templates = _coerce_str_list(
            item.get("experiment_templates"), max_items=6, max_len=300
        )
        if experiment_templates:
            family["experiment_templates"] = experiment_templates
        watchouts = _coerce_str_list(item.get("watchouts"), max_items=6, max_len=220)
        if watchouts:
            family["watchouts"] = watchouts
        out.append(family)
    return out


def coerce_research_packet(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    out: dict[str, Any] = {}
    summary = _clip(data.get("summary"), 3000)
    if summary:
        out["summary"] = summary
    frontier = _coerce_str_list(data.get("current_frontier"), max_items=10, max_len=320)
    if frontier:
        out["current_frontier"] = frontier
    known_true = _coerce_str_list(data.get("known_true"), max_items=16, max_len=320)
    if known_true:
        out["known_true"] = known_true
    known_false = _coerce_str_list(data.get("known_false"), max_items=16, max_len=320)
    if known_false:
        out["known_false"] = known_false
    anti_goals = _coerce_str_list(data.get("anti_goals"), max_items=12, max_len=240)
    if anti_goals:
        out["anti_goals"] = anti_goals
    finite_examples = _coerce_str_list(data.get("finite_examples"), max_items=12, max_len=320)
    if finite_examples:
        out["finite_examples"] = finite_examples
    formal_anchors = _coerce_str_list(data.get("formal_anchors"), max_items=12, max_len=240)
    if formal_anchors:
        out["formal_anchors"] = formal_anchors
    attack_families = _coerce_attack_families(data.get("attack_families"))
    if attack_families:
        out["attack_families"] = attack_families
    references = _coerce_reference_list(data.get("references"))
    if references:
        out["references"] = references
    operator_notes = _clip(data.get("operator_notes"), 4000)
    if operator_notes:
        out["operator_notes"] = operator_notes
    return out


def parse_research_packet(raw: str | None) -> dict[str, Any]:
    if not raw or not str(raw).strip():
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return coerce_research_packet(data)


def research_packet_to_json(data: Any) -> str:
    return json.dumps(coerce_research_packet(data), ensure_ascii=False)


def research_packet_json_from_input(raw: str | None) -> str:
    text = (raw or "").strip()
    if not text:
        return "{}"
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return json.dumps({"summary": text[:3000]}, ensure_ascii=False)
    return research_packet_to_json(data)


def select_attack_families(
    packet: dict[str, Any], active_fronts: list[str] | None = None, *, limit: int = 3
) -> list[dict[str, Any]]:
    families = packet.get("attack_families")
    if not isinstance(families, list):
        return []
    active = {str(x).strip() for x in (active_fronts or []) if str(x).strip()}
    scored: list[tuple[int, int, dict[str, Any]]] = []
    for idx, family in enumerate(families):
        if not isinstance(family, dict):
            continue
        fronts = {
            str(x).strip()
            for x in family.get("fronts", [])
            if isinstance(x, str) and x.strip()
        }
        score = 0
        if fronts and active.intersection(fronts):
            score += 3
        status = str(family.get("status") or "").lower()
        if status == "primary":
            score += 2
        elif status == "secondary":
            score += 1
        scored.append((score, -idx, family))
    scored.sort(reverse=True)
    return [family for _, _, family in scored[:limit]]


def format_research_packet_markdown(
    packet: dict[str, Any],
    *,
    active_fronts: list[str] | None = None,
    max_chars: int = 9000,
) -> str:
    packet = coerce_research_packet(packet)
    if not packet:
        return ""

    lines: list[str] = [
        "## Research packet (campaign-scoped operator steer; prefer this over generic priors)"
    ]
    summary = packet.get("summary")
    if isinstance(summary, str) and summary:
        lines.append(summary)
    frontier = packet.get("current_frontier")
    if isinstance(frontier, list) and frontier:
        lines.append("")
        lines.append("### Frontier state")
        for item in frontier[:8]:
            lines.append(f"- {item}")
    known_true = packet.get("known_true")
    if isinstance(known_true, list) and known_true:
        lines.append("")
        lines.append("### Known true / already earned")
        for item in known_true[:8]:
            lines.append(f"- {item}")
    known_false = packet.get("known_false")
    if isinstance(known_false, list) and known_false:
        lines.append("")
        lines.append("### Known false / dead ends")
        for item in known_false[:8]:
            lines.append(f"- {item}")

    families = select_attack_families(packet, active_fronts, limit=3)
    if families:
        lines.append("")
        lines.append("### Preferred attack families")
        for family in families:
            title = str(family.get("title") or family.get("id") or "route")
            status = str(family.get("status") or "").lower()
            fronts = family.get("fronts") or []
            header = f"- {title}"
            if status:
                header += f" [{status}]"
            if fronts:
                header += f" fronts={','.join(str(x) for x in fronts[:6])}"
            lines.append(header)
            why_now = _clip(family.get("why_now"), 800)
            if why_now:
                lines.append(f"  why_now: {why_now}")
            for label, key, cap in (
                ("steps", "steps", 4),
                ("lemma_templates", "lemma_templates", 4),
                ("experiment_templates", "experiment_templates", 3),
                ("watchouts", "watchouts", 3),
            ):
                vals = family.get(key)
                if isinstance(vals, list) and vals:
                    lines.append(f"  {label}:")
                    for item in vals[:cap]:
                        lines.append(f"  - {item}")

    anti_goals = packet.get("anti_goals")
    if isinstance(anti_goals, list) and anti_goals:
        lines.append("")
        lines.append("### Anti-goals")
        for item in anti_goals[:8]:
            lines.append(f"- {item}")

    finite_examples = packet.get("finite_examples")
    if isinstance(finite_examples, list) and finite_examples:
        lines.append("")
        lines.append("### Finite examples / checks to leverage")
        for item in finite_examples[:6]:
            lines.append(f"- {item}")

    formal_anchors = packet.get("formal_anchors")
    if isinstance(formal_anchors, list) and formal_anchors:
        lines.append("")
        lines.append("### Formal anchors")
        for item in formal_anchors[:6]:
            lines.append(f"- {item}")

    refs = packet.get("references")
    if isinstance(refs, list) and refs:
        lines.append("")
        lines.append("### References")
        for ref in refs[:6]:
            if not isinstance(ref, dict):
                continue
            title = _clip(ref.get("title"), 200)
            url = _clip(ref.get("url"), 500)
            note = _clip(ref.get("note"), 300)
            entry = "- "
            if title:
                entry += title
            if url:
                entry += f" ({url})" if title else url
            if note:
                entry += f" — {note}" if (title or url) else note
            if entry != "- ":
                lines.append(entry)

    operator_notes = packet.get("operator_notes")
    if isinstance(operator_notes, str) and operator_notes:
        lines.append("")
        lines.append("### Operator notes")
        lines.append(operator_notes)

    text = "\n".join(lines).strip()
    if len(text) > max_chars:
        return text[: max_chars - 1] + "…"
    return text
