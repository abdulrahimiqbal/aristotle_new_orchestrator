"""Supershadow lab: conceptual invention engine with zero live execution authority."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from typing import Any

from orchestrator import config as app_config
from orchestrator.db import Database
from orchestrator.llm import invoke_llm
from orchestrator.models import ExperimentStatus, TargetStatus, Verdict
from orchestrator.research_packets import parse_research_packet

logger = logging.getLogger("orchestrator.supershadow")
_GLOBAL_SUPERSHADOW_RUN_LOCK = False

_STRIP_JSON_FENCE = re.compile(
    r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE
)
_COUNTER_KEY_SANITIZE = re.compile(r"[^a-z0-9_.:-]+")

SUPERSHADOW_GLOBAL_GOAL_ID = "global_collatz_supershadow"
_VALID_FACT_ROLES = frozenset({"explains", "compresses", "conflicts", "requires"})
_VALID_SCORE_KEYS = (
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
)
_VALID_FAMILY_KINDS = frozenset({"established", "adjacent", "new"})
_VALID_UNIVERSE_STATUSES = frozenset(
    {
        "proposed",
        "no_signs_yet",
        "has_signs",
        "converging",
        "dead",
        "super_candidate",
    }
)
_VALID_SELF_TEST_RESULTS = frozenset(
    {"survived", "strengthened", "collapsed", "unclear"}
)
_STALE_FAMILY_REPEAT_LIMIT = 3
_STALE_FAMILY_COOLDOWN_RUNS = 3
_REPEAT_FAMILY_EXPLANATION_PLACEHOLDER = (
    "Repeated family must state what changed before it deserves transfer."
)
_COLLATZ_STRONG_MARKERS = (
    "collatz",
    "3n+1",
    "3n + 1",
    "3*n+1",
    "3*n + 1",
    "3 * n + 1",
    "hailstone",
)
_COLLATZ_CONTEXT_MARKERS = (
    "parity-vector",
    "parity vector",
    "parity-state",
    "parity state",
    "odd-input",
    "odd input",
    "odd subsystem",
    "2-adic collatz",
    "mod 8 descent",
    "mod 16 descent",
    "naive height",
    "lyapunov on odd subsystem",
    "define col",
    "collatz induces",
)

_BUILTIN_FACTS = [
    {
        "fact_key": "builtin:parity_vector_modular_dependency",
        "label": "General parity-vector modular dependency is grounded.",
        "detail": "The system treats parity-vector modular dependency as an earned grounded fact.",
        "kind": "modular",
        "provenance": "builtin_seed",
    },
    {
        "fact_key": "builtin:modular_descent_mod_8",
        "label": "Modular descent mod 8 is grounded.",
        "detail": "Descent behavior modulo 8 is considered part of the grounded current frontier.",
        "kind": "modular",
        "provenance": "builtin_seed",
    },
    {
        "fact_key": "builtin:modular_descent_mod_16_bounded",
        "label": "Modular descent mod 16 has strong bounded evidence.",
        "detail": "There is strong bounded support for the mod 16 descent picture, but it is not yet upgraded to the same status as the mod 8 result.",
        "kind": "bounded_modular",
        "provenance": "builtin_seed",
    },
    {
        "fact_key": "builtin:collatz_2_adic_extension",
        "label": "The 2-adic Collatz extension has been formalized.",
        "detail": "A 2-adic extension of Collatz is already formalized and should be treated as a genuine formal anchor.",
        "kind": "formalized_extension",
        "provenance": "builtin_seed",
    },
    {
        "fact_key": "builtin:naive_height_falsified_global",
        "label": "The naive height invariant was falsified globally.",
        "detail": "A naive global height invariant does not survive globally and should not be treated as a general monotone quantity.",
        "kind": "falsified_invariant",
        "provenance": "builtin_seed",
    },
    {
        "fact_key": "builtin:naive_height_survives_odd_inputs",
        "label": "The naive height invariant survives on odd inputs.",
        "detail": "The same naive height signal survives on odd inputs, creating a genuine tension between local usefulness and global failure.",
        "kind": "odd_subdynamics",
        "provenance": "builtin_seed",
    },
    {
        "fact_key": "builtin:bounded_descent_large_ranges",
        "label": "Bounded descent checks have succeeded at large finite ranges.",
        "detail": "Large finite bounded-descent checks have been successful and must be treated as real evidence rather than noise.",
        "kind": "finite_check",
        "provenance": "builtin_seed",
    },
]

_PRESSURE_HINTS = {
    "modular": "Several modular and parity facts are individually grounded. Search for a single ambient object that makes these modular dependencies structural instead of accidental.",
    "bounded_modular": "Strong bounded evidence is present without a full conceptual bridge. Look for a completion, compactification, or dual description that explains why bounded success should persist.",
    "formalized_extension": "A 2-adic extension already exists formally. Treat it as a real landing zone rather than decorative context, and ask what neighboring language it suggests.",
    "falsified_invariant": "A naive invariant failed globally. Good concepts should explain that failure instead of hiding it.",
    "odd_subdynamics": "Odd-input behavior retains structure after the global invariant fails. Search for a state-space split, quotient, or induced operator that makes this natural.",
    "finite_check": "Large finite success needs a language that explains why those checks are unsurprising without simply renaming the finite frontier.",
    "known_true": "Live operator knowledge already treats this as grounded. Compress it with other facts instead of restating it.",
    "known_false": "This is a dead end or a negative constraint. Useful concepts must survive contact with it.",
    "verified_target": "A campaign target has been verified; concepts should regard it as a real anchor.",
    "refuted_target": "A campaign target has been refuted; concepts should explain why it looked plausible and why it failed.",
    "experiment": "A completed experiment contributed evidence; concepts should interpret that evidence rather than duplicate the task.",
}


def _clip_text(value: Any, limit: int) -> str:
    return str(value or "")[:limit]


def _append_warning_once(warnings: list[str], warning: str) -> None:
    if warning not in warnings:
        warnings.append(warning)


def _counter_suffix(value: Any) -> str:
    raw = str(value or "unknown").strip().lower().replace(" ", "_")
    raw = _COUNTER_KEY_SANITIZE.sub("_", raw)
    return raw[:80] or "unknown"


def _strip_json_fence(text: str) -> str:
    return _STRIP_JSON_FENCE.sub("", text.strip()).strip()


def _safe_json_loads(raw: str) -> dict[str, Any]:
    text = _strip_json_fence(raw)
    if not text:
        return {}
    decoder = json.JSONDecoder()
    candidates = [text]
    first_obj = text.find("{")
    if first_obj > 0:
        candidates.append(text[first_obj:])
    for candidate in candidates:
        candidate = candidate.strip()
        if not candidate:
            continue
        try:
            value, _ = decoder.raw_decode(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return {}


def _supershadow_json_retry_user_message(user: str) -> str:
    return (
        user
        + "\n\nIMPORTANT: Return only one valid JSON object."
        + " No markdown fences, no commentary, no executable live task fields."
        + " Keep the response compact: no more than 3 concepts,"
        + " no more than 1 shadow_handoff total,"
        + " and keep long text fields under 700 characters."
    )


def _supershadow_json_repair_user_message(raw: str) -> str:
    preview = _clip_text(raw, 12000)
    return (
        "Your previous answer was invalid JSON."
        " Rewrite it as ONE valid JSON object matching the requested schema.\n"
        "Keep only the highest-signal concepts and stay compact:\n"
        "- at most 3 concepts\n"
        "- at most 1 shadow handoff total\n"
        "- every concept must explain grounded facts and include a kill test\n"
        "- return ONLY JSON\n\n"
        "Invalid draft to repair:\n"
        f"{preview}"
    )


async def _invoke_supershadow_json(
    *,
    system: str,
    user: str,
    model: str,
    temperature: float,
    log_name: str,
) -> tuple[dict[str, Any], str, int]:
    raw = await invoke_llm(
        system,
        user,
        model=model,
        temperature=temperature,
        json_object=True,
    )
    data = _safe_json_loads(raw)
    if data:
        return data, raw, 0

    logger.warning(
        "%s_invalid_json attempt=1 preview=%s", log_name, _clip_text(raw, 400)
    )
    retry_temp = min(0.2, temperature)
    retry_raw = await invoke_llm(
        system,
        _supershadow_json_retry_user_message(user),
        model=model,
        temperature=retry_temp,
        json_object=True,
    )
    retry_data = _safe_json_loads(retry_raw)
    if retry_data:
        logger.info("%s_json_retry_recovered retry_temperature=%s", log_name, retry_temp)
        return retry_data, retry_raw, 1

    logger.warning(
        "%s_invalid_json attempt=2 preview=%s", log_name, _clip_text(retry_raw, 400)
    )
    repair_raw = await invoke_llm(
        system,
        _supershadow_json_repair_user_message(retry_raw or raw),
        model=model,
        temperature=0.0,
        json_object=True,
    )
    repair_data = _safe_json_loads(repair_raw)
    if repair_data:
        logger.info("%s_json_repair_recovered", log_name)
        return repair_data, repair_raw, 2

    logger.warning(
        "%s_invalid_json attempt=3 preview=%s", log_name, _clip_text(repair_raw, 400)
    )
    return {}, repair_raw, 2


def _str_list(value: Any, *, max_items: int, max_item_chars: int) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value[:max_items]:
        text = str(item or "").strip()
        if text:
            out.append(text[:max_item_chars])
    return out


def _append_tail_unique(
    raw: Any, item: str, *, limit: int, max_chars: int = 800
) -> list[str]:
    prev = raw if isinstance(raw, list) else []
    out = [
        str(value or "").strip()[:max_chars]
        for value in prev
        if str(value or "").strip()
    ]
    entry = str(item or "").strip()[:max_chars]
    if not entry:
        return out[-limit:]
    if not out or out[-1] != entry:
        out.append(entry)
    return out[-limit:]


def _merge_policy(old: dict[str, Any], delta: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(old)
    if not delta:
        return out
    if isinstance(delta.get("weights"), dict):
        for key, value in delta["weights"].items():
            if isinstance(key, str) and len(key) < 200:
                out[key] = value
    notes = delta.get("notes")
    if isinstance(notes, str) and notes.strip():
        out["_supershadow_notes_tail"] = _append_tail_unique(
            out.get("_supershadow_notes_tail"),
            notes,
            limit=6,
            max_chars=1200,
        )
    lessons = delta.get("lessons")
    if isinstance(lessons, list):
        tail = out.get("_supershadow_invention_lessons_tail", [])
        for lesson in lessons:
            tail = _append_tail_unique(tail, str(lesson or ""), limit=10, max_chars=800)
        out["_supershadow_invention_lessons_tail"] = tail
    return out


def _normalize_family_cooldowns(raw: Any) -> dict[str, int]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, int] = {}
    for key, value in raw.items():
        family = _family_slug(key)
        if not family:
            continue
        try:
            runs = int(value)
        except (TypeError, ValueError):
            runs = 0
        if runs > 0:
            out[family] = min(12, runs)
    return out


def _advance_family_cooldowns(
    current: dict[str, int], suppressed_families: list[str]
) -> dict[str, int]:
    out: dict[str, int] = {}
    for family, runs in _normalize_family_cooldowns(current).items():
        if runs > 1:
            out[family] = runs - 1
    for family in suppressed_families:
        slug = _family_slug(family)
        if slug:
            out[slug] = max(out.get(slug, 0), _STALE_FAMILY_COOLDOWN_RUNS)
    return out


def _normalize_universe_status_value(value: Any, *, default: str = "proposed") -> str:
    status = str(value or "").strip().lower()
    if status in _VALID_UNIVERSE_STATUSES:
        return status
    return default


def _universe_status_rank(value: Any) -> int:
    status = _normalize_universe_status_value(value)
    return {
        "super_candidate": 5,
        "converging": 4,
        "has_signs": 3,
        "no_signs_yet": 2,
        "proposed": 1,
        "dead": 0,
    }.get(status, 0)


def _normalize_self_test_results(raw: Any) -> list[dict[str, str]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, str]] = []
    for item in raw[:6]:
        if not isinstance(item, dict):
            continue
        attack = _clip_text(item.get("attack"), 900).strip()
        result = str(item.get("result") or "").strip().lower()
        note = _clip_text(item.get("note"), 1200).strip()
        if not attack:
            continue
        if result not in _VALID_SELF_TEST_RESULTS:
            result = "unclear"
        out.append(
            {
                "attack": attack,
                "result": result,
                "note": note,
            }
        )
    return out


def _normalize_super_universe_candidate(
    raw: Any,
    fact_lookup: dict[str, dict[str, Any]],
    fact_labels: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    why_now = _clip_text(raw.get("why_now"), 1400).strip()
    survived_attacks = _str_list(
        raw.get("survived_attacks"), max_items=4, max_item_chars=900
    )
    fact_audit = raw.get("full_fact_audit")
    explains: list[dict[str, Any]] = []
    awkward: list[dict[str, Any]] = []
    if isinstance(fact_audit, dict):
        for key, bucket in (("explains", explains), ("awkward", awkward)):
            for item in fact_audit.get(key) or []:
                normalized = _normalize_fact_reference(item, fact_lookup, fact_labels)
                if normalized:
                    bucket.append(normalized)
    smallest_aristotle_probe = _clip_text(
        raw.get("smallest_aristotle_probe") or raw.get("aristotle_probe"),
        1400,
    ).strip()
    if not (why_now and survived_attacks and smallest_aristotle_probe):
        return {}
    return {
        "why_now": why_now,
        "survived_attacks": survived_attacks,
        "full_fact_audit": {
            "explains": explains[:8],
            "awkward": awkward[:8],
        },
        "smallest_aristotle_probe": smallest_aristotle_probe,
    }


def _infer_universe_status(
    raw_status: Any,
    *,
    self_test_results: list[dict[str, str]],
    signs_of_life: list[str],
    negative_signs: list[str],
    super_candidate: bool,
) -> str:
    if super_candidate:
        return "super_candidate"
    status = _normalize_universe_status_value(raw_status, default="")
    if status:
        return status
    survived = sum(
        1
        for item in self_test_results
        if str(item.get("result") or "") in {"survived", "strengthened"}
    )
    collapsed = sum(
        1 for item in self_test_results if str(item.get("result") or "") == "collapsed"
    )
    if collapsed and not survived and not signs_of_life:
        return "dead"
    if survived >= 2 and len(signs_of_life) >= 2:
        return "converging"
    if survived >= 1 or signs_of_life:
        return "has_signs"
    if negative_signs or self_test_results:
        return "no_signs_yet"
    return "proposed"


def _normalize_universe_memory(raw: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for key, value in raw.items():
        slug = _family_slug(key)
        if not slug or not isinstance(value, dict):
            continue
        try:
            seen_count = int(value.get("seen_count") or 0)
        except (TypeError, ValueError):
            seen_count = 0
        try:
            tests_run = int(value.get("tests_run") or 0)
        except (TypeError, ValueError):
            tests_run = 0
        try:
            super_candidate_runs = int(value.get("super_candidate_runs") or 0)
        except (TypeError, ValueError):
            super_candidate_runs = 0
        out[slug] = {
            "slug": slug,
            "title": _clip_text(value.get("title"), 200).strip(),
            "status": _normalize_universe_status_value(value.get("status")),
            "seen_count": max(0, min(999, seen_count)),
            "tests_run": max(0, min(999, tests_run)),
            "positive_signs": _str_list(
                value.get("positive_signs"), max_items=4, max_item_chars=500
            ),
            "negative_signs": _str_list(
                value.get("negative_signs"), max_items=4, max_item_chars=500
            ),
            "conditional_theorem": _clip_text(
                value.get("conditional_theorem"), 1200
            ).strip(),
            "invention_lesson": _clip_text(
                value.get("invention_lesson"), 900
            ).strip(),
            "super_candidate_runs": max(0, min(999, super_candidate_runs)),
        }
    return out


def _update_universe_memory(
    raw_memory: Any, concepts: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    memory = _normalize_universe_memory(raw_memory)
    for concept in concepts:
        slug = _family_slug(concept.get("concept_family") or concept.get("title"))
        if not slug:
            continue
        entry = memory.setdefault(
            slug,
            {
                "slug": slug,
                "title": "",
                "status": "proposed",
                "seen_count": 0,
                "tests_run": 0,
                "positive_signs": [],
                "negative_signs": [],
                "conditional_theorem": "",
                "invention_lesson": "",
                "super_candidate_runs": 0,
            },
        )
        entry["title"] = _clip_text(concept.get("title"), 200).strip() or entry.get(
            "title", ""
        )
        entry["status"] = _normalize_universe_status_value(
            concept.get("universe_status"), default=str(entry.get("status") or "proposed")
        )
        entry["seen_count"] = int(entry.get("seen_count") or 0) + 1
        entry["tests_run"] = int(entry.get("tests_run") or 0) + len(
            concept.get("self_test_results") or []
        )
        entry["positive_signs"] = _str_list(
            concept.get("signs_of_life"), max_items=4, max_item_chars=500
        ) or list(entry.get("positive_signs") or [])
        entry["negative_signs"] = _str_list(
            concept.get("negative_signs"), max_items=4, max_item_chars=500
        ) or list(entry.get("negative_signs") or [])
        conditional_theorem = _clip_text(concept.get("conditional_theorem"), 1200).strip()
        if conditional_theorem:
            entry["conditional_theorem"] = conditional_theorem
        invention_lesson = _clip_text(concept.get("invention_lesson"), 900).strip()
        if invention_lesson:
            entry["invention_lesson"] = invention_lesson
        if concept.get("super_universe_candidate"):
            entry["super_candidate_runs"] = int(entry.get("super_candidate_runs") or 0) + 1
    return memory


def _family_slug(value: Any) -> str:
    raw = str(value or "").strip().lower()
    raw = _COUNTER_KEY_SANITIZE.sub("_", raw)
    raw = raw.strip("._:-")
    return raw[:120]


def _normalize_family_kind(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in _VALID_FAMILY_KINDS:
        return text
    return "established"


def _extract_tags(text: str) -> set[str]:
    lowered = text.lower()
    tags: set[str] = set()
    if "mod " in lowered or "modular" in lowered or "parity" in lowered:
        tags.add("modular")
    if "2-adic" in lowered or "adic" in lowered:
        tags.add("formalized_extension")
    if "bounded" in lowered or "finite" in lowered:
        tags.add("finite_check")
    if "height" in lowered or "invariant" in lowered:
        tags.add("falsified_invariant")
    if "odd" in lowered:
        tags.add("odd_subdynamics")
    return tags


def _fact_signature(fact: dict[str, Any]) -> str:
    return " ".join(
        [
            str(fact.get("label") or ""),
            str(fact.get("detail") or ""),
            str(fact.get("kind") or ""),
        ]
    )


def _looks_like_collatz_text(text: str) -> bool:
    lowered = text.lower()
    if any(marker in lowered for marker in _COLLATZ_STRONG_MARKERS):
        return True
    hits = sum(1 for marker in _COLLATZ_CONTEXT_MARKERS if marker in lowered)
    return hits >= 2


def _campaign_matches_collatz_mission(db: Database, campaign: dict[str, Any]) -> bool:
    campaign_id = str(campaign.get("id") or "")
    blobs = [
        str(campaign.get("prompt") or ""),
        str(campaign.get("research_packet_json") or ""),
    ]
    if campaign_id:
        blobs.extend(db.get_target_descriptions(campaign_id)[:24])
    combined = "\n".join(blob for blob in blobs if blob.strip())
    return _looks_like_collatz_text(combined)


def _research_packet_facts(packet: dict[str, Any], campaign_id: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for section, kind in (
        ("known_true", "known_true"),
        ("known_false", "known_false"),
        ("finite_examples", "finite_check"),
        ("formal_anchors", "formalized_extension"),
    ):
        raw = packet.get(section)
        if not isinstance(raw, list):
            continue
        for idx, item in enumerate(raw[:12]):
            text = str(item or "").strip()
            if not text:
                continue
            out.append(
                {
                    "fact_key": f"live:{campaign_id}:packet:{section}:{idx}",
                    "label": text[:320],
                    "detail": f"Research packet {section} entry for campaign {campaign_id}.",
                    "kind": kind,
                    "provenance": f"campaign:{campaign_id}:research_packet",
                }
            )
    return out


def _target_facts(db: Database, campaign_id: str) -> list[dict[str, Any]]:
    state = db.get_campaign_state(campaign_id)
    out: list[dict[str, Any]] = []
    for target in state.targets[:40]:
        if target.status not in (TargetStatus.VERIFIED, TargetStatus.REFUTED):
            continue
        status = "verified_target" if target.status == TargetStatus.VERIFIED else "refuted_target"
        out.append(
            {
                "fact_key": f"live:{campaign_id}:target:{target.id}",
                "label": target.description[:320],
                "detail": f"Campaign {campaign_id} target {target.id} has status {target.status.value}.",
                "kind": status,
                "provenance": f"campaign:{campaign_id}:target:{target.id}",
            }
        )
    return out


def _experiment_facts(db: Database, campaign_id: str) -> list[dict[str, Any]]:
    state = db.get_campaign_state(campaign_id)
    completed = [
        exp
        for exp in state.experiments
        if exp.status in (ExperimentStatus.COMPLETED, ExperimentStatus.FAILED)
        and (exp.result_summary or "").strip()
    ]
    completed.sort(
        key=lambda exp: (exp.completed_at or exp.submitted_at or ""),
        reverse=True,
    )
    out: list[dict[str, Any]] = []
    for exp in completed[:16]:
        verdict = exp.verdict.value if exp.verdict else ""
        detail = (
            f"Campaign {campaign_id} experiment {exp.id} on target {exp.target_id} "
            f"ended with verdict {verdict or 'unknown'}."
        )
        if exp.result_summary:
            detail += f" Summary: {_clip_text(exp.result_summary, 500)}"
        out.append(
            {
                "fact_key": f"live:{campaign_id}:experiment:{exp.id}",
                "label": _clip_text(exp.objective, 320),
                "detail": detail[:900],
                "kind": "experiment",
                "provenance": f"campaign:{campaign_id}:experiment:{exp.id}",
            }
        )
    return out


def _dedupe_facts(facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for fact in facts:
        key = str(fact.get("fact_key") or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(fact)
    return out


def _build_grounded_fact_basis(db: Database) -> list[dict[str, Any]]:
    facts = [dict(fact) for fact in _BUILTIN_FACTS]
    collatz_campaigns = [
        campaign
        for campaign in db.get_all_campaigns()
        if _campaign_matches_collatz_mission(db, campaign)
    ][:20]
    for campaign in collatz_campaigns:
        campaign_id = str(campaign["id"])
        try:
            state = db.get_campaign_state(campaign_id)
        except ValueError:
            continue
        packet = parse_research_packet(state.campaign.research_packet_json)
        facts.extend(_research_packet_facts(packet, campaign_id))
        facts.extend(_target_facts(db, campaign_id))
        facts.extend(_experiment_facts(db, campaign_id))
    return _dedupe_facts(facts)


def _build_pressure_map(facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    clusters: dict[str, list[dict[str, Any]]] = {}
    for fact in facts:
        tags = _extract_tags(_fact_signature(fact))
        tags.add(str(fact.get("kind") or "misc"))
        for tag in tags:
            clusters.setdefault(tag, []).append(fact)

    pressure_map: list[dict[str, Any]] = []
    for tag, rows in clusters.items():
        if not rows:
            continue
        pressure_map.append(
            {
                "cluster": tag,
                "fact_keys": [str(row.get("fact_key") or "") for row in rows[:8]],
                "facts": [str(row.get("label") or "") for row in rows[:4]],
                "pressure": _PRESSURE_HINTS.get(
                    tag,
                    "Look for the smallest language shift that makes these facts feel structural at once.",
                ),
            }
        )

    tension_rows: list[dict[str, Any]] = []
    facts_text = " ".join(_fact_signature(fact).lower() for fact in facts)
    if "falsified" in facts_text and "odd" in facts_text:
        tension_rows.append(
            {
                "cluster": "invariant_tension",
                "fact_keys": [
                    "builtin:naive_height_falsified_global",
                    "builtin:naive_height_survives_odd_inputs",
                ],
                "facts": [
                    "Naive height fails globally.",
                    "Naive height survives on odd inputs.",
                ],
                "pressure": "Search for a split state space, induced odd map, or quotient where the odd-input signal is native instead of accidental.",
            }
        )
    if "modular descent mod 8" in facts_text and "modular descent mod 16" in facts_text:
        tension_rows.append(
            {
                "cluster": "modular_extension_gap",
                "fact_keys": [
                    "builtin:modular_descent_mod_8",
                    "builtin:modular_descent_mod_16_bounded",
                ],
                "facts": [
                    "Mod 8 descent is grounded.",
                    "Mod 16 descent has strong bounded evidence.",
                ],
                "pressure": "Find the minimal enlargement that explains why mod 8 is stable and mod 16 is nearby but not yet conceptually absorbed.",
            }
        )
    pressure_map.extend(tension_rows)
    pressure_map.sort(key=lambda row: (len(row.get("fact_keys") or []), row.get("cluster") or ""), reverse=True)
    return pressure_map[:10]


def _build_family_memory(
    db: Database, *, cooldowns: dict[str, int] | None = None
) -> list[dict[str, Any]]:
    concepts = db.list_supershadow_concepts(SUPERSHADOW_GLOBAL_GOAL_ID, limit=160)
    incubations = db.list_supershadow_incubations(SUPERSHADOW_GLOBAL_GOAL_ID, limit=160)
    cooldowns = _normalize_family_cooldowns(cooldowns)

    concept_by_id = {str(row.get("id") or ""): dict(row) for row in concepts}
    families: dict[str, dict[str, Any]] = {}
    for row in concepts:
        title = str(row.get("title") or "")
        family = _family_slug(row.get("concept_family") or title)
        if not family:
            continue
        entry = families.setdefault(
            family,
            {
                "concept_family": family,
                "family_kind": _normalize_family_kind(row.get("family_kind")),
                "parent_family": _family_slug(row.get("parent_family")),
                "concept_count": 0,
                "active_incubations": 0,
                "grounded_count": 0,
                "reviewed_handoffs": 0,
                "recent_titles": [],
                "cooldown_runs_remaining": int(cooldowns.get(family) or 0),
            },
        )
        entry["cooldown_runs_remaining"] = max(
            int(entry.get("cooldown_runs_remaining") or 0),
            int(cooldowns.get(family) or 0),
        )
        entry["concept_count"] += 1
        if title and title not in entry["recent_titles"]:
            entry["recent_titles"].append(title[:160])
            entry["recent_titles"] = entry["recent_titles"][:4]
        family_kind = _normalize_family_kind(row.get("family_kind"))
        if family_kind == "new" or (
            family_kind == "adjacent" and entry.get("family_kind") != "new"
        ):
            entry["family_kind"] = family_kind
        parent_family = _family_slug(row.get("parent_family"))
        if parent_family and not entry.get("parent_family"):
            entry["parent_family"] = parent_family

    for incubation in incubations:
        concept_id = str(incubation.get("concept_id") or "")
        concept = concept_by_id.get(concept_id, {})
        family = _family_slug(concept.get("concept_family") or concept.get("title"))
        if not family:
            continue
        entry = families.setdefault(
            family,
            {
                "concept_family": family,
                "family_kind": _normalize_family_kind(concept.get("family_kind")),
                "parent_family": _family_slug(concept.get("parent_family")),
                "concept_count": 0,
                "active_incubations": 0,
                "grounded_count": 0,
                "reviewed_handoffs": 0,
                "recent_titles": [],
                "cooldown_runs_remaining": int(cooldowns.get(family) or 0),
            },
        )
        entry["cooldown_runs_remaining"] = max(
            int(entry.get("cooldown_runs_remaining") or 0),
            int(cooldowns.get(family) or 0),
        )
        entry["reviewed_handoffs"] += 1
        status = str(incubation.get("status") or "").strip().lower()
        if status in {"incubating", "operationalized"}:
            entry["active_incubations"] += 1
        if status == "grounded":
            entry["grounded_count"] += 1

    for family, runs in cooldowns.items():
        entry = families.setdefault(
            family,
            {
                "concept_family": family,
                "family_kind": "established",
                "parent_family": "",
                "concept_count": 0,
                "active_incubations": 0,
                "grounded_count": 0,
                "reviewed_handoffs": 0,
                "recent_titles": [],
                "cooldown_runs_remaining": 0,
            },
        )
        entry["cooldown_runs_remaining"] = max(
            int(entry.get("cooldown_runs_remaining") or 0),
            int(runs or 0),
        )

    rows = list(families.values())
    for row in rows:
        row["cooldown_runs_remaining"] = int(
            row.get("cooldown_runs_remaining") or cooldowns.get(
                str(row.get("concept_family") or ""), 0
            )
            or 0
        )
        row["stalled"] = (
            int(row.get("concept_count") or 0) >= 3
            and int(row.get("active_incubations") or 0) == 0
            and int(row.get("grounded_count") or 0) == 0
        )
    rows.sort(
        key=lambda row: (
            int(row.get("grounded_count") or 0),
            int(row.get("active_incubations") or 0),
            int(row.get("concept_count") or 0),
            str(row.get("concept_family") or ""),
        ),
        reverse=True,
    )
    return rows[:20]


SUPERSHADOW_SYSTEM = """You are Supershadow Lab: an upstream conceptual invention engine for the Collatz project.

You are not Shadow, and you are not Aristotle/live.

Role separation:
- Supershadow invents mathematical universes.
- Shadow turns surviving universes into disciplined proof programs.
- Aristotle/live grounds only the strongest, narrowest probes after human approval.

Critical constraint:
- You have zero live execution authority.
- Do not create or imply live experiments, campaign targets, executable objectives, or Aristotle tasks.
- Your only outbound action is a conceptual handoff or a rare super-universe review packet.

Mission:
- Invent new mathematical universes that could make Collatz feel easy instead of stubborn.
- Optimize for worldview power first: ask what ambient world, operator, grammar, energy, completion, or geometry would make the theorem feel almost tautological.
- Run the universe loop inside your own response: invent, self-attack, look for signs of life, then either deepen, downgrade, or kill the universe.
- Stay grounded against the fact basis, but do not force discovery to look prematurely respectable.
- A weird universe that survives attack is better than a polished restatement of modular folklore.

You must explicitly search over language shifts such as:
- new state spaces
- completions or compactifications
- new conserved or Lyapunov-like quantities
- symbolic-dynamics or grammar views of trajectories
- renormalization or scaling operators
- algebraic encodings of parity dynamics
- dual descriptions where descent is easier
- embeddings where Collatz becomes linear, contractive, monotone, or spectrally constrained
- reformulations where trajectory behavior becomes structure classification

Your output is STRICT JSON with this shape:
{
  "worldview_summary": "2-8 sentences about the current universe hunt",
  "run_summary": "one compact paragraph for the run log",
  "concepts": [
    {
      "title": "short title",
      "concept_family": "stable universe slug like odd_state_quotient or graded_2_adic_module",
      "family_kind": "established|adjacent|new",
      "parent_family": "required when family_kind is adjacent, else empty string",
      "why_not_same_as_existing_family": "why this is genuinely a new or adjacent universe rather than a restatement",
      "worldview_summary": "why this universe matters",
      "universe_thesis": "one-sentence thesis for why this universe could make Collatz natural",
      "conditional_theorem": "if this universe is right, what theorem-shaped claim would imply Collatz or sharply reduce the frontier",
      "concepts": ["first conceptual claim", "second conceptual claim"],
      "ontological_moves": ["new ambient space", "new operator", "new quotient"],
      "explains_facts": [
        {
          "fact_key": "must refer to a grounded fact key from the user message",
          "fact_label": "optional copy of the fact label",
          "role": "explains|compresses|conflicts|requires",
          "note": "how this universe relates to that fact"
        }
      ],
      "tensions": [
        {
          "text": "what remains awkward, contradictory, or unresolved in this universe"
        }
      ],
      "kill_tests": [
        {
          "description": "smallest falsifier",
          "expected_failure_signal": "what concrete signal would kill the universe",
          "suggested_grounding_path": "how Shadow or Lean could pressure-test it later"
        }
      ],
      "self_test_results": [
        {
          "attack": "the strongest internal objection you tried",
          "result": "survived|strengthened|collapsed|unclear",
          "note": "what happened under that attack"
        }
      ],
      "signs_of_life": ["concrete signal that this universe might be real"],
      "negative_signs": ["concrete signal that this universe may still be fake"],
      "universe_status": "proposed|no_signs_yet|has_signs|converging|dead|super_candidate",
      "invention_lesson": "what this universe taught you about inventing stronger universes",
      "bridge_lemmas": ["lemma family that would connect this back to formal work"],
      "smallest_transfer_probe": "smallest Shadow-facing bridge or bounded diagnostic that would make this universe actionable",
      "reduce_frontier_or_rename": "does this reduce the frontier or merely rename it?",
      "super_universe_candidate": {
        "why_now": "why this deserves scarce Aristotle attention",
        "survived_attacks": ["attack 1", "attack 2"],
        "full_fact_audit": {
          "explains": ["fact_key_1", "fact_key_2"],
          "awkward": ["fact_key_3"]
        },
        "smallest_aristotle_probe": "single narrow grounding probe worth human approval"
      },
      "scores": {
        "compression_power": 0,
        "fit_to_known_facts": 0,
        "ontological_delta": 0,
        "falsifiability": 0,
        "bridgeability": 0,
        "grounding_cost": 0,
        "speculative_risk": 0,
        "family_novelty": 0,
        "transfer_value": 0,
        "family_saturation_penalty": 0
      },
      "shadow_handoffs": [
        {
          "title": "handoff title",
          "summary": "what Shadow should operationalize",
          "why_compressive": "why this universe explains several facts at once",
          "bridge_lemmas": ["bridge lemma 1", "bridge lemma 2"],
          "shadow_task": "what proof-program work Shadow should do next",
          "recommended_next_step": "single next move for Shadow",
          "grounding_notes": "how to stay tethered to Lean and bounded checks"
        }
      ]
    }
  ]
}

Rules:
- 1 to 3 concepts, but prefer 1 dominant universe and at most 1 backup.
- Prefer one alive universe over family diversity.
- If a family is marked stalled or repeatedly appears without transfer, avoid emitting it again unless you can name a materially different mechanism or a much cheaper probe.
- Every live concept must explain grounded facts and include at least one kill test.
- Bridge lemmas are optional during discovery; include them only when you can name a sharp first bridge instead of vague formalization theater.
- Every concept must declare a concept_family and, when possible, the smallest_transfer_probe that would make it actionable for Shadow.
- If you repeat an existing family, you must explain what changed and why this is not the same family again.
- Supershadow should not be rewarded for novelty alone. High ontological delta without compression is weak.
- A good concept names a mechanism, a theorem-shaped claim, and the best reason it could still collapse.
- A concept that merely renames the frontier should score poorly.
- Emit super_universe_candidate only rarely, and only when the universe survives multiple self-attacks, explains multiple grounded facts, names a theorem-shaped claim, and proposes a tiny Aristotle probe.
- Only the single strongest surviving concept should emit a shadow_handoff, and only if it also has a sharp falsifier plus at least one credible bridge lemma.
- No direct live-work fields such as campaign_id, target_id, objective, move_kind, new_experiment, new_target, or Aristotle instructions.
- Keep JSON valid. No markdown fences."""


SUPERSHADOW_DISTILLATION_SYSTEM = """You are Supershadow Self-Test: the second pass after universe discovery.

Input: one candidate universe that already looks promising.

Goal:
- attack this universe as if you want to kill it,
- record whether it survives, strengthens, or collapses,
- if it survives, sharpen it into the minimum theorem-shaped claim,
- preserve the conceptual leap if it still looks alive,
- add only the smallest bridge back to Shadow and Lean that the idea truly earns,
- escalate to a super-universe candidate only if the universe looks like a genuinely strong shot.

Return STRICT JSON with the same top-level shape as discovery, but:
- emit exactly 1 concept,
- focus on the same dominant family unless the candidate clearly collapses,
- include at least 2 self_test_results when possible,
- keep only 1 to 2 kill tests,
- include bridge_lemmas only if they are concrete first-bridge statements,
- emit super_universe_candidate only if the universe survives multiple attacks, still fits the data, and has a narrow Aristotle probe,
- emit at most 1 shadow_handoff, and only if the concept now has a sharp falsifier and at least one credible bridge lemma.

Do not introduce live work, campaigns, targets, or Aristotle instructions.
Keep JSON valid. No markdown fences."""


def _score_in_range(value: Any, default: int = 0) -> int:
    try:
        score = int(value)
    except (TypeError, ValueError):
        score = default
    return max(0, min(5, score))


def _looks_low_cost_probe(text: str) -> bool:
    lowered = text.lower()
    return any(
        token in lowered
        for token in (
            "bounded",
            "finite",
            "small",
            "local",
            "first bridge",
            "interface",
            "compatibility",
            "well-defined",
            "residue",
            "mod ",
            "odd",
            "single-step",
        )
    )


def _infer_concept_scores(
    concept: dict[str, Any], family_stats: dict[str, Any] | None = None
) -> dict[str, int]:
    title_blob = " ".join(
        [
            str(concept.get("title") or ""),
            str(concept.get("concept_family") or ""),
            str(concept.get("family_kind") or ""),
            str(concept.get("why_not_same_as_existing_family") or ""),
            str(concept.get("smallest_transfer_probe") or ""),
            str(concept.get("worldview_summary") or ""),
            " ".join(str(item) for item in concept.get("concepts") or []),
            " ".join(str(item) for item in concept.get("ontological_moves") or []),
            str(concept.get("reduce_frontier_or_rename") or ""),
        ]
    ).lower()
    explained_count = len(concept.get("explains_facts") or [])
    kill_tests = concept.get("kill_tests") or []
    bridge_lemmas = concept.get("bridge_lemmas") or []
    self_test_results = concept.get("self_test_results") or []
    signs_of_life = concept.get("signs_of_life") or []
    negative_signs = concept.get("negative_signs") or []
    conditional_theorem = _clip_text(concept.get("conditional_theorem"), 1200).strip()
    super_universe_candidate = concept.get("super_universe_candidate") or {}
    tensions = concept.get("tensions") or []
    family_kind = _normalize_family_kind(concept.get("family_kind"))
    smallest_transfer_probe = _clip_text(
        concept.get("smallest_transfer_probe"), 1200
    ).strip()
    why_not_same = _clip_text(
        concept.get("why_not_same_as_existing_family"), 1200
    ).strip()

    survived_attacks = sum(
        1
        for item in self_test_results
        if str(item.get("result") or "") in {"survived", "strengthened"}
    )
    compression = min(
        5,
        max(1, explained_count + (1 if conditional_theorem else 0) + min(1, len(signs_of_life))),
    )
    fit = min(
        5,
        max(
            1,
            explained_count
            + (1 if tensions else 0)
            + min(1, survived_attacks)
            + min(1, len(signs_of_life)),
        ),
    )
    ontological_delta = min(5, max(1, len(concept.get("ontological_moves") or [])))
    falsifiability = min(
        5,
        max(
            1,
            len(kill_tests)
            + sum(
                1
                for item in kill_tests
                if isinstance(item, dict) and item.get("expected_failure_signal")
            ),
        ),
    )
    bridgeability = max(1, len(bridge_lemmas))
    if bridge_lemmas and smallest_transfer_probe:
        bridgeability += 1
    bridgeability = min(5, bridgeability)
    grounding_cost = 2
    speculative_risk = 2
    if any(token in title_blob for token in ("axiom", "foundational", "foundation shift")):
        grounding_cost = 5
        speculative_risk = 5
    elif any(token in title_blob for token in ("compactification", "completion", "spectral", "functorial", "category")):
        grounding_cost = 3
        speculative_risk = 3
    if smallest_transfer_probe and _looks_low_cost_probe(smallest_transfer_probe):
        grounding_cost = min(grounding_cost, 3)
    if super_universe_candidate:
        grounding_cost = min(grounding_cost + 1, 5)
        speculative_risk = max(speculative_risk, 3)
    if "rename" in title_blob and "reduce" not in title_blob:
        compression = max(1, compression - 1)
    family_novelty = 2
    if family_kind == "new":
        family_novelty = 5
    elif family_kind == "adjacent":
        family_novelty = 4
    if why_not_same:
        family_novelty = min(5, family_novelty + 1)

    transfer_value = 1
    if smallest_transfer_probe:
        transfer_value += 2
        if _looks_low_cost_probe(smallest_transfer_probe):
            transfer_value += 1
    if bridge_lemmas:
        transfer_value += 1
    if kill_tests:
        transfer_value += 1
    if conditional_theorem:
        transfer_value += 1
    if signs_of_life:
        transfer_value += 1
    if super_universe_candidate:
        transfer_value += 1
    transfer_value = min(5, transfer_value)

    family_saturation_penalty = 0
    family_stats = family_stats or {}
    prior_count = int(family_stats.get("concept_count") or 0)
    active_incubations = int(family_stats.get("active_incubations") or 0)
    grounded_count = int(family_stats.get("grounded_count") or 0)
    stalled = bool(family_stats.get("stalled"))
    if family_kind == "new":
        family_saturation_penalty = 0 if prior_count == 0 else 1
    elif active_incubations > 0 or grounded_count > 0:
        family_saturation_penalty = 1
    elif stalled:
        family_saturation_penalty = min(5, 2 + prior_count // 2)
    elif prior_count > 0:
        family_saturation_penalty = min(4, 1 + prior_count // 3)
    return {
        "compression_power": compression,
        "fit_to_known_facts": fit,
        "ontological_delta": ontological_delta,
        "falsifiability": falsifiability,
        "bridgeability": bridgeability,
        "grounding_cost": grounding_cost,
        "speculative_risk": speculative_risk,
        "family_novelty": family_novelty,
        "transfer_value": transfer_value,
        "family_saturation_penalty": family_saturation_penalty,
    }


def _normalize_scores(
    raw_scores: Any,
    concept: dict[str, Any],
    family_stats: dict[str, Any] | None = None,
) -> dict[str, int]:
    inferred = _infer_concept_scores(concept, family_stats)
    normalized = dict(inferred)
    if isinstance(raw_scores, dict):
        for key in (
            "compression_power",
            "fit_to_known_facts",
            "ontological_delta",
            "falsifiability",
            "bridgeability",
            "grounding_cost",
            "speculative_risk",
        ):
            normalized[key] = _score_in_range(raw_scores.get(key), inferred[key])
        for key in ("family_novelty", "transfer_value"):
            normalized[key] = max(
                inferred[key], _score_in_range(raw_scores.get(key), inferred[key])
            )
        normalized["family_saturation_penalty"] = max(
            inferred["family_saturation_penalty"],
            _score_in_range(
                raw_scores.get("family_saturation_penalty"),
                inferred["family_saturation_penalty"],
            ),
        )
    probe = _clip_text(concept.get("smallest_transfer_probe"), 1200).strip()
    if probe and _looks_low_cost_probe(probe):
        normalized["grounding_cost"] = min(normalized["grounding_cost"], 3)
        normalized["transfer_value"] = max(normalized["transfer_value"], 4)
    if _normalize_family_kind(concept.get("family_kind")) == "new":
        normalized["family_novelty"] = max(normalized["family_novelty"], 4)
    if _clip_text(concept.get("conditional_theorem"), 1200).strip():
        normalized["transfer_value"] = max(normalized["transfer_value"], 4)
    if concept.get("super_universe_candidate"):
        normalized["transfer_value"] = max(normalized["transfer_value"], 5)
    return normalized


def _normalize_family_fields(
    concept_raw: dict[str, Any], title: str, family_memory_lookup: dict[str, dict[str, Any]]
) -> dict[str, str]:
    concept_family = _family_slug(concept_raw.get("concept_family") or title)
    family_kind = _normalize_family_kind(concept_raw.get("family_kind"))
    parent_family = _family_slug(concept_raw.get("parent_family"))
    if family_kind == "adjacent" and not parent_family:
        parent_family = concept_family
    why_not_same = _clip_text(
        concept_raw.get("why_not_same_as_existing_family"), 1200
    ).strip()
    if concept_family in family_memory_lookup and not why_not_same:
        why_not_same = _REPEAT_FAMILY_EXPLANATION_PLACEHOLDER
    smallest_transfer_probe = _clip_text(
        concept_raw.get("smallest_transfer_probe"), 1200
    ).strip()
    return {
        "concept_family": concept_family,
        "family_kind": family_kind,
        "parent_family": parent_family,
        "why_not_same_as_existing_family": why_not_same,
        "smallest_transfer_probe": smallest_transfer_probe,
    }


def _family_materially_advances(
    concept: dict[str, Any], family_stats: dict[str, Any] | None
) -> bool:
    if not family_stats:
        return True
    if int(family_stats.get("cooldown_runs_remaining") or 0) > 0:
        return False
    prior_count = int(family_stats.get("concept_count") or 0)
    if prior_count < _STALE_FAMILY_REPEAT_LIMIT or not bool(family_stats.get("stalled")):
        return True

    family_kind = _normalize_family_kind(concept.get("family_kind"))
    if family_kind == "new":
        return True

    title = str(concept.get("title") or "").strip()
    recent_titles = {
        str(item or "").strip() for item in family_stats.get("recent_titles") or []
    }
    if title and title in recent_titles:
        return False

    why_not_same = str(concept.get("why_not_same_as_existing_family") or "").strip()
    if (
        not why_not_same
        or why_not_same == _REPEAT_FAMILY_EXPLANATION_PLACEHOLDER
    ):
        return False

    probe = str(concept.get("smallest_transfer_probe") or "").strip()
    if not probe or not _looks_low_cost_probe(probe):
        return False

    scores = concept.get("scores") or {}
    if int(scores.get("transfer_value") or 0) < 4:
        return False
    if int(scores.get("bridgeability") or 0) < 2:
        return False

    if family_kind == "adjacent":
        return int(scores.get("family_novelty") or 0) >= 4
    return int(scores.get("transfer_value") or 0) >= 5


def _normalize_fact_reference(
    raw: Any,
    fact_lookup: dict[str, dict[str, Any]],
    fact_labels: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    fact_key = ""
    fact_label = ""
    role = "explains"
    note = ""
    if isinstance(raw, dict):
        fact_key = _clip_text(raw.get("fact_key"), 120).strip()
        fact_label = _clip_text(raw.get("fact_label"), 500).strip()
        role = _clip_text(raw.get("role"), 32).strip().lower() or "explains"
        note = _clip_text(raw.get("note"), 1200).strip()
    else:
        text = _clip_text(raw, 500).strip()
        if text in fact_lookup:
            fact_key = text
        else:
            fact_label = text
    fact = None
    if fact_key:
        fact = fact_lookup.get(fact_key)
    if fact is None and fact_label:
        fact = fact_labels.get(fact_label.lower())
    if fact is None:
        return None
    if role not in _VALID_FACT_ROLES:
        role = "explains"
    return {
        "fact_key": str(fact.get("fact_key") or ""),
        "fact_label": str(fact.get("label") or ""),
        "role": role,
        "note": note,
    }


def _normalize_kill_tests(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw[:8]:
        if isinstance(item, dict):
            description = _clip_text(item.get("description"), 1200).strip()
            expected_failure_signal = _clip_text(
                item.get("expected_failure_signal"), 1200
            ).strip()
            suggested_grounding_path = _clip_text(
                item.get("suggested_grounding_path"), 1200
            ).strip()
        else:
            description = _clip_text(item, 1200).strip()
            expected_failure_signal = ""
            suggested_grounding_path = ""
        if not description:
            continue
        out.append(
            {
                "description": description,
                "expected_failure_signal": expected_failure_signal,
                "suggested_grounding_path": suggested_grounding_path,
            }
        )
    return out


def _normalize_tensions(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw[:10]:
        if isinstance(item, dict):
            text = _clip_text(item.get("text"), 1200).strip()
        else:
            text = _clip_text(item, 1200).strip()
        if text:
            out.append({"text": text})
    return out


def _handoff_eligible(scores: dict[str, int]) -> bool:
    return (
        (scores["compression_power"] >= 4 or scores["family_novelty"] >= 4)
        and scores["fit_to_known_facts"] >= 3
        and scores["falsifiability"] >= 2
        and scores["bridgeability"] >= 2
        and scores["transfer_value"] >= 3
        and scores["family_saturation_penalty"] <= 3
        and (scores["grounding_cost"] <= 3 or scores["transfer_value"] >= 4)
        and scores["speculative_risk"] <= 4
    )


def _normalize_shadow_handoffs(
    raw: Any,
    *,
    concept: dict[str, Any],
    max_handoffs: int,
    warnings: list[str],
) -> list[dict[str, Any]]:
    if max_handoffs <= 0:
        if raw:
            _append_warning_once(warnings, "handoff_budget_zero")
        return []
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw[:6]:
        if len(out) >= max_handoffs:
            _append_warning_once(warnings, "handoff_cap_applied")
            break
        if not isinstance(item, dict):
            continue
        title = _clip_text(item.get("title"), 240).strip()
        summary = _clip_text(item.get("summary"), 1200).strip()
        why_compressive = _clip_text(item.get("why_compressive"), 1200).strip()
        shadow_task = _clip_text(item.get("shadow_task"), 1200).strip()
        recommended_next_step = _clip_text(
            item.get("recommended_next_step"), 1200
        ).strip()
        grounding_notes = _clip_text(item.get("grounding_notes"), 1200).strip()
        bridge_lemmas = _str_list(
            item.get("bridge_lemmas") or concept.get("bridge_lemmas"),
            max_items=8,
            max_item_chars=400,
        )
        if not title:
            title = f"Handoff: {concept['title']}"
        if not summary:
            summary = _clip_text(concept.get("worldview_summary"), 1200).strip()
        if not why_compressive:
            why_compressive = (
                f"Links {len(concept.get('explains_facts') or [])} grounded fact(s) "
                f"through the language shift '{title}'."
            )
        if not shadow_task:
            shadow_task = _clip_text(
                concept.get("reduce_frontier_or_rename"), 1200
            ).strip() or "Turn this concept into a disciplined proof program."
        if not recommended_next_step:
            recommended_next_step = shadow_task
        if not bridge_lemmas:
            _append_warning_once(warnings, "handoff_missing_bridge_lemmas")
            continue
        handoff_payload = {
            "title": title,
            "summary": summary,
            "why_compressive": why_compressive,
            "bridge_lemmas": bridge_lemmas,
            "shadow_task": shadow_task,
            "recommended_next_step": recommended_next_step,
            "grounding_notes": grounding_notes,
        }
        super_universe_candidate = concept.get("super_universe_candidate") or {}
        if super_universe_candidate:
            handoff_payload["review_kind"] = "super_universe_candidate"
            handoff_payload["super_universe_candidate"] = super_universe_candidate
            if not handoff_payload["grounding_notes"]:
                handoff_payload["grounding_notes"] = (
                    "This packet asks for human review before any Aristotle grounding."
                )
        out.append(handoff_payload)
    return out


def _concept_sort_key(
    concept: dict[str, Any]
) -> tuple[Any, ...]:
    scores = concept.get("scores") or {}
    return (
        _universe_status_rank(concept.get("universe_status")),
        int(scores.get("compression_power") or 0),
        int(scores.get("fit_to_known_facts") or 0),
        int(scores.get("ontological_delta") or 0),
        int(scores.get("falsifiability") or 0),
        int(scores.get("family_novelty") or 0),
        int(scores.get("transfer_value") or 0),
        -int(scores.get("family_saturation_penalty") or 0),
        int(scores.get("bridgeability") or 0),
        -int(scores.get("speculative_risk") or 0),
        -int(scores.get("grounding_cost") or 0),
        str(concept.get("concept_family") or ""),
    )


def _default_handoff_payload(concept: dict[str, Any]) -> list[dict[str, Any]]:
    probe = _clip_text(concept.get("smallest_transfer_probe"), 1200).strip()
    super_universe_candidate = concept.get("super_universe_candidate") or {}
    if not probe and not super_universe_candidate:
        return []
    title = f"Handoff: {concept['title']}"
    grounding_notes = "Use the smallest_transfer_probe before escalating to heavier conceptual machinery."
    if super_universe_candidate:
        title = f"Super-universe review: {concept['title']}"
        grounding_notes = (
            "Human review first. If this really stays alive, escalate only the narrow Aristotle probe."
        )
    return [
        {
            "title": title,
            "summary": _clip_text(concept.get("worldview_summary"), 1200).strip(),
            "why_compressive": (
                f"Preserves the concept family '{concept.get('concept_family')}' while testing a smaller actionable descendant."
            ),
            "bridge_lemmas": list(concept.get("bridge_lemmas") or []),
            "shadow_task": probe
            or _clip_text(
                (super_universe_candidate or {}).get("smallest_aristotle_probe"), 1200
            ).strip(),
            "recommended_next_step": probe
            or _clip_text(
                (super_universe_candidate or {}).get("smallest_aristotle_probe"), 1200
            ).strip(),
            "grounding_notes": grounding_notes,
            "review_kind": "super_universe_candidate"
            if super_universe_candidate
            else "shadow_handoff",
            "super_universe_candidate": super_universe_candidate,
        }
    ]


def _select_breakthrough_candidates(
    concepts: list[dict[str, Any]], warnings: list[str], *, limit: int
) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    by_identity: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for concept in concepts:
        key = (
            str(concept.get("concept_family") or ""),
            str(concept.get("title") or "").strip().lower(),
        )
        by_identity.setdefault(key, []).append(concept)
    unique: list[dict[str, Any]] = []
    for rows in by_identity.values():
        rows.sort(key=_concept_sort_key, reverse=True)
        unique.append(rows[0])
        if len(rows) > 1:
            _append_warning_once(warnings, "concept_repeat_filtered")
    unique.sort(key=_concept_sort_key, reverse=True)
    if len(unique) > limit:
        _append_warning_once(warnings, "concept_board_truncated")
    return unique[:limit]


def _normalize_supershadow_response(
    data: dict[str, Any],
    fact_basis: list[dict[str, Any]],
    family_memory: list[dict[str, Any]],
    *,
    max_handoffs: int,
    selection_limit: int = 3,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    suppressed_families: set[str] = set()
    fact_lookup = {str(fact["fact_key"]): fact for fact in fact_basis}
    fact_labels = {str(fact["label"]).lower(): fact for fact in fact_basis}
    family_memory_lookup = {
        str(row.get("concept_family") or ""): row for row in family_memory
    }

    worldview_summary = _clip_text(data.get("worldview_summary"), 4000).strip()
    if not worldview_summary:
        _append_warning_once(warnings, "missing_worldview_summary")
    run_summary = _clip_text(data.get("run_summary"), 4000).strip() or worldview_summary

    concepts_out: list[dict[str, Any]] = []
    concepts_raw = data.get("concepts")
    if not isinstance(concepts_raw, list):
        concepts_raw = []

    handoff_budget = max(0, max_handoffs)
    for concept_raw in concepts_raw[:12]:
        if not isinstance(concept_raw, dict):
            continue
        title = _clip_text(concept_raw.get("title"), 500).strip()
        if not title:
            _append_warning_once(warnings, "concept_missing_title")
            continue
        explains_facts: list[dict[str, Any]] = []
        for raw_fact in concept_raw.get("explains_facts") or []:
            normalized_fact = _normalize_fact_reference(raw_fact, fact_lookup, fact_labels)
            if normalized_fact:
                explains_facts.append(normalized_fact)
        explains_facts = explains_facts[:12]
        if not explains_facts:
            _append_warning_once(warnings, "concept_missing_explained_facts")
            continue
        bridge_lemmas = _str_list(
            concept_raw.get("bridge_lemmas"), max_items=10, max_item_chars=500
        )
        if not bridge_lemmas:
            _append_warning_once(warnings, "concept_missing_bridge_lemmas")
        kill_tests = _normalize_kill_tests(concept_raw.get("kill_tests"))
        if not kill_tests:
            _append_warning_once(warnings, "concept_missing_kill_tests")
            continue
        self_test_results = _normalize_self_test_results(
            concept_raw.get("self_test_results")
        )
        signs_of_life = _str_list(
            concept_raw.get("signs_of_life"), max_items=6, max_item_chars=600
        )
        negative_signs = _str_list(
            concept_raw.get("negative_signs"), max_items=6, max_item_chars=600
        )
        super_universe_candidate = _normalize_super_universe_candidate(
            concept_raw.get("super_universe_candidate"), fact_lookup, fact_labels
        )
        survived_self_tests = sum(
            1
            for item in self_test_results
            if str(item.get("result") or "") in {"survived", "strengthened"}
        )
        if super_universe_candidate and (
            survived_self_tests < 2
            or len(signs_of_life) < 2
            or len(explains_facts) < 2
            or not _clip_text(concept_raw.get("conditional_theorem"), 1200).strip()
        ):
            _append_warning_once(warnings, "super_universe_downgraded")
            super_universe_candidate = {}

        family_fields = _normalize_family_fields(
            concept_raw, title, family_memory_lookup
        )
        concept = {
            "title": title,
            **family_fields,
            "worldview_summary": _clip_text(
                concept_raw.get("worldview_summary"), 2000
            ).strip()
            or worldview_summary,
            "universe_thesis": _clip_text(
                concept_raw.get("universe_thesis"), 1200
            ).strip()
            or _clip_text(concept_raw.get("worldview_summary"), 1200).strip()
            or title,
            "conditional_theorem": _clip_text(
                concept_raw.get("conditional_theorem"), 1600
            ).strip(),
            "concepts": _str_list(
                concept_raw.get("concepts"), max_items=8, max_item_chars=600
            ),
            "ontological_moves": _str_list(
                concept_raw.get("ontological_moves"), max_items=8, max_item_chars=600
            ),
            "explains_facts": explains_facts,
            "tensions": _normalize_tensions(concept_raw.get("tensions")),
            "kill_tests": kill_tests,
            "self_test_results": self_test_results,
            "signs_of_life": signs_of_life,
            "negative_signs": negative_signs,
            "bridge_lemmas": bridge_lemmas,
            "reduce_frontier_or_rename": _clip_text(
                concept_raw.get("reduce_frontier_or_rename"), 1200
            ).strip(),
            "invention_lesson": _clip_text(
                concept_raw.get("invention_lesson"), 900
            ).strip(),
            "super_universe_candidate": super_universe_candidate,
        }
        concept["universe_status"] = _infer_universe_status(
            concept_raw.get("universe_status"),
            self_test_results=self_test_results,
            signs_of_life=signs_of_life,
            negative_signs=negative_signs,
            super_candidate=bool(super_universe_candidate),
        )
        concept["scores"] = _normalize_scores(
            concept_raw.get("scores"),
            concept,
            family_memory_lookup.get(concept["concept_family"]),
        )
        family_stats = family_memory_lookup.get(concept["concept_family"])
        if not _family_materially_advances(concept, family_stats):
            if int((family_stats or {}).get("cooldown_runs_remaining") or 0) > 0:
                _append_warning_once(warnings, "family_cooldown_active")
            _append_warning_once(warnings, "stale_family_suppressed")
            suppressed_families.add(concept["concept_family"])
            continue
        if not _handoff_eligible(concept["scores"]):
            if concept_raw.get("shadow_handoffs"):
                _append_warning_once(warnings, "handoff_below_threshold")
            concept["shadow_handoffs"] = []
        else:
            handoff_source = concept_raw.get("shadow_handoffs")
            if not handoff_source:
                handoff_source = _default_handoff_payload(concept)
                if handoff_source:
                    _append_warning_once(warnings, "handoff_synthesized_from_probe")
            concept_handoffs = _normalize_shadow_handoffs(
                handoff_source,
                concept=concept,
                max_handoffs=handoff_budget,
                warnings=warnings,
            )
            handoff_budget -= len(concept_handoffs)
            concept["shadow_handoffs"] = concept_handoffs
        concepts_out.append(concept)

    if not concepts_out:
        _append_warning_once(warnings, "no_valid_concepts")
    concepts_out = _select_breakthrough_candidates(
        concepts_out, warnings, limit=selection_limit
    )
    concepts_out.sort(key=_concept_sort_key, reverse=True)
    normalized = {
        "worldview_summary": worldview_summary,
        "run_summary": run_summary,
        "concepts": concepts_out[:selection_limit],
        "_meta": {
            "suppressed_families": sorted(suppressed_families),
        },
    }
    return normalized, warnings


def _build_supershadow_user_message(
    db: Database,
    goal_text: str,
    fact_basis: list[dict[str, Any]],
    pressure_map: list[dict[str, Any]],
    family_memory: list[dict[str, Any]],
    *,
    handoff_budget: int | None = None,
    suppress_handoffs_reason: str | None = None,
) -> str:
    ep = db.get_supershadow_state(SUPERSHADOW_GLOBAL_GOAL_ID)
    worldview_raw = ep.get("worldview_json") or "{}"
    policy_raw = ep.get("policy_json") or "{}"
    try:
        worldview_obj = json.loads(worldview_raw) if worldview_raw.strip() else {}
    except json.JSONDecodeError:
        worldview_obj = {}
    try:
        policy_obj = json.loads(policy_raw) if policy_raw.strip() else {}
    except json.JSONDecodeError:
        policy_obj = {}
    universe_memory = _normalize_universe_memory(
        policy_obj.get("_supershadow_universe_memory")
    )
    invention_lessons = policy_obj.get("_supershadow_invention_lessons_tail", [])
    if not isinstance(invention_lessons, list):
        invention_lessons = []
    warnings_tail = policy_obj.get("_supershadow_validation_warnings_tail", [])
    if not isinstance(warnings_tail, list):
        warnings_tail = []

    lines: list[str] = []
    lines.append("## Supershadow mission")
    lines.append(goal_text[:4000])
    lines.append("")
    lines.append("## Output doctrine")
    lines.append(
        "Invent the one strongest mathematical universe that could make Collatz feel easy."
    )
    lines.append(
        "Supershadow is not allowed to create live work. Do not output executable targets, experiments, or Aristotle tasks."
    )
    lines.append(
        "Run the universe loop yourself: invent, self-attack, look for signs of life, then either deepen or kill the universe."
    )
    lines.append(
        "Discovery first: a good universe explains grounded facts and names a sharp kill-test, even if the first bridge lemma is not clear yet."
    )
    lines.append(
        "Prefer one dominant line over performative family diversity when a single worldview looks alive."
    )
    lines.append(
        "If you revisit a family that is already saturated, you must name a genuinely different mechanism or a much cheaper smallest_transfer_probe."
    )
    lines.append(
        "A super-universe candidate is rare. It should appear only when the universe survives multiple internal attacks and still looks like a strong shot."
    )
    lines.append(
        "Only the strongest surviving universe should be handed to Shadow, and only after the self-test pass sharpens its falsifier and first bridge."
    )
    if suppress_handoffs_reason:
        lines.append(f"Shadow handoff budget this run: 0. {suppress_handoffs_reason}")
    elif handoff_budget is not None:
        lines.append(
            f"Shadow handoff budget this run: at most {max(0, handoff_budget)} handoff(s)."
        )
    lines.append("")
    lines.append("## Grounded fact basis")
    for fact in fact_basis[:48]:
        lines.append(
            f"- {fact['fact_key']} | {fact['label']} | kind={fact['kind']} | provenance={fact['provenance']}"
        )
        lines.append(f"  detail: {_clip_text(fact.get('detail'), 800)}")
    lines.append("")
    lines.append("## Pressure map")
    for row in pressure_map[:12]:
        facts = ", ".join(row.get("fact_keys") or [])
        lines.append(
            f"- cluster={row.get('cluster')} | facts={facts}\n  pressure: {_clip_text(row.get('pressure'), 700)}"
        )
    lines.append("")
    lines.append("## Concept family memory")
    if family_memory:
        for row in family_memory[:12]:
            recent_titles = " | ".join(row.get("recent_titles") or [])
            lines.append(
                f"- family={row.get('concept_family')} | kind={row.get('family_kind')} | parent={row.get('parent_family') or '—'} | "
                f"concepts={row.get('concept_count')} | active_incubations={row.get('active_incubations')} | grounded={row.get('grounded_count')} | stalled={row.get('stalled')}"
            )
            if recent_titles:
                lines.append(f"  recent_titles: {recent_titles[:700]}")
    else:
        lines.append("- none")
    stalled_families = [
        row
        for row in family_memory
        if bool(row.get("stalled"))
        and int(row.get("concept_count") or 0) >= _STALE_FAMILY_REPEAT_LIMIT
    ]
    lines.append("")
    lines.append("## Saturated families to avoid")
    if stalled_families:
        for row in stalled_families[:8]:
            lines.append(
                f"- avoid family={row.get('concept_family')} unless this run materially lowers transfer cost; prior_concepts={row.get('concept_count')}"
            )
    else:
        lines.append("- none")
    lines.append("")
    cooldown_families = [
        row
        for row in family_memory
        if int(row.get("cooldown_runs_remaining") or 0) > 0
    ]
    lines.append("## Families on cooldown")
    if cooldown_families:
        for row in cooldown_families[:8]:
            lines.append(
                f"- avoid family={row.get('concept_family')} for {int(row.get('cooldown_runs_remaining') or 0)} more run(s); last repeats did not materially advance"
            )
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Universe memory")
    if universe_memory:
        universe_rows = sorted(
            universe_memory.values(),
            key=lambda row: (
                _universe_status_rank(row.get("status")),
                int(row.get("super_candidate_runs") or 0),
                int(row.get("tests_run") or 0),
                int(row.get("seen_count") or 0),
                str(row.get("slug") or ""),
            ),
            reverse=True,
        )
        for row in universe_rows[:10]:
            lines.append(
                f"- universe={row.get('slug')} | status={row.get('status')} | seen={row.get('seen_count')} | tests={row.get('tests_run')} | super_candidate_runs={row.get('super_candidate_runs')}"
            )
            if row.get("conditional_theorem"):
                lines.append(
                    f"  conditional_theorem: {_clip_text(row.get('conditional_theorem'), 700)}"
                )
            if row.get("positive_signs"):
                lines.append(
                    f"  positive_signs: {' | '.join(row.get('positive_signs') or [])[:700]}"
                )
            if row.get("negative_signs"):
                lines.append(
                    f"  negative_signs: {' | '.join(row.get('negative_signs') or [])[:700]}"
                )
            if row.get("invention_lesson"):
                lines.append(
                    f"  lesson: {_clip_text(row.get('invention_lesson'), 700)}"
                )
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Recent invention lessons")
    if invention_lessons:
        for lesson in invention_lessons[-8:]:
            lines.append(f"- {_clip_text(lesson, 700)}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Recent warnings")
    if warnings_tail:
        for warning in warnings_tail[-10:]:
            lines.append(f"- {warning}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Latest worldview memory")
    compact_worldview = {
        "summary": worldview_obj.get("summary"),
        "latest_concept_titles": worldview_obj.get("latest_concept_titles"),
        "latest_universes": worldview_obj.get("latest_universes"),
    }
    lines.append(json.dumps(compact_worldview, ensure_ascii=False, indent=2)[:6000])
    return "\n".join(lines)


def _build_supershadow_distillation_user_message(
    *,
    goal_text: str,
    fact_basis: list[dict[str, Any]],
    pressure_map: list[dict[str, Any]],
    concept: dict[str, Any],
    handoff_budget: int,
) -> str:
    lines: list[str] = []
    lines.append("## Distillation mission")
    lines.append(goal_text[:4000])
    lines.append("")
    lines.append("## Candidate worldview to sharpen")
    lines.append(json.dumps(concept, ensure_ascii=False, indent=2)[:12000])
    lines.append("")
    lines.append("## Distillation doctrine")
    lines.append(
        "Do not widen the search. Attack this universe first, then sharpen it into the minimum falsifiable thesis if it survives."
    )
    lines.append(
        "Preserve the conceptual leap if it still looks alive, but cut decorative structure."
    )
    lines.append(
        "Return exactly one concept. Keep 1-2 kill tests. Add at least 2 self_test_results when possible. Add bridge lemmas only if they are genuine first bridges."
    )
    lines.append(
        f"Shadow handoff budget after distillation: at most {max(0, handoff_budget)} handoff(s)."
    )
    lines.append(
        "Only emit super_universe_candidate if this universe still looks like a strong shot after the attacks and the Aristotle probe is tiny."
    )
    lines.append("")
    lines.append("## Grounded fact basis")
    for fact in fact_basis[:24]:
        lines.append(
            f"- {fact['fact_key']} | {fact['label']} | kind={fact['kind']} | provenance={fact['provenance']}"
        )
    lines.append("")
    lines.append("## Pressure map")
    for row in pressure_map[:8]:
        facts = ", ".join(row.get("fact_keys") or [])
        lines.append(
            f"- cluster={row.get('cluster')} | facts={facts}\n  pressure: {_clip_text(row.get('pressure'), 500)}"
        )
    return "\n".join(lines)


async def run_supershadow_global_lab(
    db: Database,
    *,
    goal_text: str,
    trigger_kind: str = "manual",
    handoff_budget: int | None = None,
    suppress_handoffs_reason: str | None = None,
) -> dict[str, Any]:
    global _GLOBAL_SUPERSHADOW_RUN_LOCK
    if not app_config.LLM_API_KEY:
        return {"ok": False, "error": "LLM_API_KEY not set"}
    if _GLOBAL_SUPERSHADOW_RUN_LOCK:
        return {"ok": False, "error": "supershadow_global_run_in_progress"}
    _GLOBAL_SUPERSHADOW_RUN_LOCK = True
    db.ensure_supershadow_state_row(SUPERSHADOW_GLOBAL_GOAL_ID, goal_text=goal_text)
    try:
        if handoff_budget is None:
            handoff_budget = int(app_config.SUPERSHADOW_MAX_HANDOFFS_PER_RUN)
        ep = db.get_supershadow_state(SUPERSHADOW_GLOBAL_GOAL_ID)
        try:
            old_policy = json.loads(ep.get("policy_json") or "{}")
        except json.JSONDecodeError:
            old_policy = {}
        if not isinstance(old_policy, dict):
            old_policy = {}
        family_cooldowns = _normalize_family_cooldowns(
            old_policy.get("_supershadow_family_cooldowns")
        )
        fact_basis = _build_grounded_fact_basis(db)
        pressure_map = _build_pressure_map(fact_basis)
        family_memory = _build_family_memory(db, cooldowns=family_cooldowns)
        user = _build_supershadow_user_message(
            db,
            goal_text,
            fact_basis,
            pressure_map,
            family_memory,
            handoff_budget=handoff_budget,
            suppress_handoffs_reason=suppress_handoffs_reason,
        )
        model = (
            app_config.SUPERSHADOW_LLM_MODEL
            or app_config.SHADOW_LLM_MODEL
            or app_config.LLM_MODEL
        )
        temperature = float(app_config.SUPERSHADOW_LLM_TEMPERATURE)
        request_meta = {
            "model": model,
            "temperature": temperature,
            "system_prompt_sha256": hashlib.sha256(
                SUPERSHADOW_SYSTEM.encode("utf-8")
            ).hexdigest(),
            "user_prompt_sha256": hashlib.sha256(user.encode("utf-8")).hexdigest(),
            "schema_version": 2,
            "trigger_kind": trigger_kind,
            "handoff_budget": handoff_budget,
            "suppress_handoffs_reason": suppress_handoffs_reason or "",
        }

        try:
            data, raw, json_retry_count = await _invoke_supershadow_json(
                system=SUPERSHADOW_SYSTEM,
                user=user,
                model=model,
                temperature=temperature,
                log_name="supershadow_global",
            )
        except Exception:
            logger.exception("Supershadow LLM call failed")
            return {"ok": False, "error": "llm_request_failed"}

        if not data:
            logger.warning(
                "supershadow_invalid_json trigger=%s preview=%s",
                trigger_kind,
                _clip_text(raw, 400),
            )
            return {
                "ok": False,
                "error": "invalid_json",
                "raw_preview": raw[:2000],
                "json_retry_count": json_retry_count,
            }

        discovery_normalized, validation_warnings = _normalize_supershadow_response(
            data,
            fact_basis,
            family_memory,
            max_handoffs=0,
            selection_limit=3,
        )
        concepts = list(discovery_normalized.get("concepts") or [])

        distillation_raw = ""
        distillation_json_retry_count = 0
        distillation_warnings: list[str] = []
        distillation_normalized: dict[str, Any] | None = None
        if concepts:
            distill_user = _build_supershadow_distillation_user_message(
                goal_text=goal_text,
                fact_basis=fact_basis,
                pressure_map=pressure_map,
                concept=concepts[0],
                handoff_budget=handoff_budget,
            )
            try:
                distilled_data, distillation_raw, distillation_json_retry_count = (
                    await _invoke_supershadow_json(
                        system=SUPERSHADOW_DISTILLATION_SYSTEM,
                        user=distill_user,
                        model=model,
                        temperature=min(0.35, temperature),
                        log_name="supershadow_distillation",
                    )
                )
            except Exception:
                logger.exception("Supershadow distillation LLM call failed")
                distilled_data = {}
            if distilled_data:
                distillation_normalized, distillation_warnings = (
                    _normalize_supershadow_response(
                        distilled_data,
                        fact_basis,
                        family_memory,
                        max_handoffs=handoff_budget,
                        selection_limit=1,
                    )
                )
                if distillation_normalized.get("concepts"):
                    distilled_best = dict(distillation_normalized["concepts"][0])
                    remaining = [
                        concept
                        for concept in concepts
                        if (
                            str(concept.get("concept_family") or ""),
                            str(concept.get("title") or "").strip().lower(),
                        )
                        != (
                            str(distilled_best.get("concept_family") or ""),
                            str(distilled_best.get("title") or "").strip().lower(),
                        )
                    ]
                    concepts = [distilled_best] + remaining[:2]
        validation_warnings.extend(distillation_warnings)
        suppressed_families = list(discovery_normalized.get("_meta", {}).get("suppressed_families") or [])
        suppressed_families.extend(
            list((distillation_normalized or {}).get("_meta", {}).get("suppressed_families") or [])
        )
        normalized = {
            "worldview_summary": (
                (distillation_normalized or {}).get("worldview_summary")
                or discovery_normalized.get("worldview_summary")
                or ""
            ),
            "run_summary": (
                (distillation_normalized or {}).get("run_summary")
                or discovery_normalized.get("run_summary")
                or ""
            ),
            "concepts": concepts,
        }
        universe_memory = _update_universe_memory(
            old_policy.get("_supershadow_universe_memory"), concepts
        )
        invention_lessons = [
            str(concept.get("invention_lesson") or "").strip()
            for concept in concepts
            if str(concept.get("invention_lesson") or "").strip()
        ]

        worldview_payload = {
            "summary": normalized.get("worldview_summary") or "",
            "latest_concept_titles": [
                str(concept.get("title") or "")
                for concept in normalized.get("concepts") or []
            ][:8],
            "latest_universes": [
                {
                    "concept_family": str(concept.get("concept_family") or ""),
                    "title": str(concept.get("title") or ""),
                    "status": str(concept.get("universe_status") or ""),
                    "signs_of_life": list(concept.get("signs_of_life") or [])[:2],
                }
                for concept in normalized.get("concepts") or []
            ][:6],
            "pressure_map": pressure_map[:6],
        }
        new_worldview_json = json.dumps(worldview_payload, ensure_ascii=False)
        merged_policy = _merge_policy(
            old_policy,
            {
                "weights": {
                    "prefer_compression": True,
                    "forbid_live_authority": True,
                },
                "notes": normalized.get("run_summary") or "",
                "lessons": invention_lessons,
            },
        )
        next_family_cooldowns = _advance_family_cooldowns(
            family_cooldowns, suppressed_families
        )
        merged_policy["_supershadow_family_cooldowns"] = next_family_cooldowns
        merged_policy["_supershadow_universe_memory"] = universe_memory
        if validation_warnings:
            merged_policy["_supershadow_validation_warnings_tail"] = (
                list(merged_policy.get("_supershadow_validation_warnings_tail", []))
                + validation_warnings
            )[-24:]
        new_policy_json = json.dumps(merged_policy, ensure_ascii=False)
        concepts = list(normalized.get("concepts") or [])
        response_obj = {
            "output": normalized,
            "discovery_output": discovery_normalized,
            "distillation_output": distillation_normalized or {},
            "fact_basis": fact_basis,
            "pressure_map": pressure_map,
                "family_memory": family_memory,
                "universe_memory": universe_memory,
                "meta": {
                **request_meta,
                "validation_warnings": validation_warnings,
                "raw_preview": _clip_text(raw, 4000),
                "json_retry_count": json_retry_count,
                "distillation_raw_preview": _clip_text(distillation_raw, 4000),
                "distillation_json_retry_count": distillation_json_retry_count,
            },
        }
        run_id = db.supershadow_commit_run(
            SUPERSHADOW_GLOBAL_GOAL_ID,
            trigger_kind=trigger_kind,
            worldview_summary=_clip_text(normalized.get("worldview_summary"), 4000),
            run_summary=_clip_text(normalized.get("run_summary"), 4000),
            fact_basis_json=json.dumps(fact_basis, ensure_ascii=False),
            pressure_map_json=json.dumps(pressure_map, ensure_ascii=False),
            response_obj=response_obj,
            new_worldview_json=new_worldview_json,
            new_policy_json=new_policy_json,
            concepts=concepts,
            goal_text=goal_text,
        )
        handoff_count = sum(
            len(concept.get("shadow_handoffs") or []) for concept in concepts
        )
        return {
            "ok": True,
            "run_id": run_id,
            "concept_count": len(concepts),
            "handoff_count": handoff_count,
            "summary": normalized.get("run_summary") or "",
            "validation_warnings": validation_warnings,
            "json_retry_count": json_retry_count,
        }
    finally:
        _GLOBAL_SUPERSHADOW_RUN_LOCK = False


async def supershadow_global_loop(db: Database) -> None:
    if not app_config.SUPERSHADOW_GLOBAL_AUTO_ENABLED:
        return
    while True:
        try:
            pending = len(
                db.list_supershadow_handoff_requests(
                    SUPERSHADOW_GLOBAL_GOAL_ID, status="pending", limit=500
                )
            )
            handoff_budget = int(app_config.SUPERSHADOW_MAX_HANDOFFS_PER_RUN)
            suppress_handoffs_reason = ""
            if pending >= int(app_config.SUPERSHADOW_MAX_PENDING_HANDOFFS):
                handoff_budget = 0
                suppress_handoffs_reason = (
                    f"{pending} handoff(s) are already waiting for review, so this run "
                    "should keep inventing concepts without adding queue pressure."
                )
                db.increment_ops_counter(
                    "supershadow_global:auto_run_handoffs_suppressed_pending_cap", 1
                )
            res = await run_supershadow_global_lab(
                db,
                goal_text=app_config.SUPERSHADOW_GLOBAL_GOAL,
                trigger_kind="auto",
                handoff_budget=handoff_budget,
                suppress_handoffs_reason=suppress_handoffs_reason or None,
            )
            if res.get("ok"):
                db.increment_ops_counter("supershadow_global:auto_run_ok", 1)
                if int(res.get("json_retry_count") or 0) > 0:
                    db.increment_ops_counter(
                        "supershadow_global:auto_run_json_retry_recovered", 1
                    )
            else:
                db.increment_ops_counter("supershadow_global:auto_run_fail", 1)
                err = _counter_suffix(res.get("error"))
                db.increment_ops_counter(f"supershadow_global:auto_run_fail:{err}", 1)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Supershadow global loop tick failed")
            db.increment_ops_counter("supershadow_global:auto_run_exception", 1)
        await asyncio.sleep(
            max(30, int(app_config.SUPERSHADOW_GLOBAL_TICK_INTERVAL_SEC))
        )
