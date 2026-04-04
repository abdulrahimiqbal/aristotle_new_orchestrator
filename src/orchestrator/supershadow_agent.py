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
_STALE_FAMILY_REPEAT_LIMIT = 3
_REPEAT_FAMILY_EXPLANATION_PLACEHOLDER = (
    "Repeated family must state what changed before it deserves transfer."
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
        + " Keep the response compact: no more than 6 concepts,"
        + " no more than 2 shadow_handoffs total,"
        + " and keep long text fields under 700 characters."
    )


def _supershadow_json_repair_user_message(raw: str) -> str:
    preview = _clip_text(raw, 12000)
    return (
        "Your previous answer was invalid JSON."
        " Rewrite it as ONE valid JSON object matching the requested schema.\n"
        "Keep only the highest-signal concepts and stay compact:\n"
        "- at most 6 concepts\n"
        "- at most 2 shadow handoffs total\n"
        "- every concept must explain grounded facts, include a kill test, and include bridge lemmas\n"
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
        prev = out.get("_supershadow_notes_tail", [])
        if not isinstance(prev, list):
            prev = []
        out["_supershadow_notes_tail"] = (prev + [notes.strip()])[-12:]
    return out


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
    for campaign in db.get_all_campaigns()[:20]:
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


def _build_family_memory(db: Database) -> list[dict[str, Any]]:
    concepts = db.list_supershadow_concepts(SUPERSHADOW_GLOBAL_GOAL_ID, limit=160)
    incubations = db.list_supershadow_incubations(SUPERSHADOW_GLOBAL_GOAL_ID, limit=160)

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
            },
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
            },
        )
        entry["reviewed_handoffs"] += 1
        status = str(incubation.get("status") or "").strip().lower()
        if status in {"incubating", "operationalized"}:
            entry["active_incubations"] += 1
        if status == "grounded":
            entry["grounded_count"] += 1

    rows = list(families.values())
    for row in rows:
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
- Supershadow invents conceptual frameworks.
- Shadow turns promising frameworks into disciplined proof programs.
- Aristotle/live grounds specific claims in Lean and bounded computation.

Critical constraint:
- You have zero live execution authority.
- Do not create or imply live experiments, campaign targets, executable objectives, or Aristotle tasks.
- Your only outbound action is a Shadow-facing conceptual handoff request.

Mission:
- Search for ontology-expanding ideas of the kind that once looked strange but later became the right language:
  negative numbers, irrational numbers, imaginary numbers, p-adics, distributions, and similar shifts.
- Optimize for compression, not novelty.
- Search for the smallest conceptual enlargement that makes multiple grounded facts feel natural at once.
- Aggressively explore whether the right language is different.
- Stay tethered to known facts, falsifiability, and grounding cost.

You must explicitly search over language shifts such as:
- new state spaces
- completions or compactifications
- new potential functions
- functorial views of iteration
- algebraic encodings of parity dynamics
- dual descriptions where descent is easier
- embeddings where Collatz becomes linear, contractive, monotone, or spectrally constrained
- reformulations where trajectory behavior becomes structure classification

Your output is STRICT JSON with this shape:
{
  "worldview_summary": "2-8 sentences about the conceptual search direction",
  "run_summary": "one compact paragraph for the run log",
  "concepts": [
    {
      "title": "short title",
      "concept_family": "stable family slug like odd_state_quotient or graded_2_adic_module",
      "family_kind": "established|adjacent|new",
      "parent_family": "required when family_kind is adjacent, else empty string",
      "why_not_same_as_existing_family": "why this is genuinely a new or adjacent family rather than a restatement",
      "worldview_summary": "why this language shift matters",
      "concepts": ["first conceptual claim", "second conceptual claim"],
      "ontological_moves": ["new ambient space", "new operator", "new quotient"],
      "explains_facts": [
        {
          "fact_key": "must refer to a grounded fact key from the user message",
          "fact_label": "optional copy of the fact label",
          "role": "explains|compresses|conflicts|requires",
          "note": "how this concept relates to that fact"
        }
      ],
      "tensions": [
        {
          "text": "what remains awkward, contradictory, or unresolved in this language"
        }
      ],
      "kill_tests": [
        {
          "description": "smallest falsifier",
          "expected_failure_signal": "what concrete signal would kill the concept",
          "suggested_grounding_path": "how Shadow or Lean could pressure-test it later"
        }
      ],
      "bridge_lemmas": ["lemma family that would connect this back to formal work"],
      "smallest_transfer_probe": "smallest Shadow-facing bridge or bounded diagnostic that would make this family actionable",
      "reduce_frontier_or_rename": "does this reduce the frontier or merely rename it?",
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
          "why_compressive": "why this concept explains several facts at once",
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
- 3 to 6 concepts.
- If possible, include at least:
  1 established-family exploit,
  1 adjacent-family concept,
  and 1 genuinely new family.
- If a family is marked stalled or repeatedly appears without transfer, avoid emitting it again unless you can name a materially cheaper smallest_transfer_probe and a concrete reason this pass is different.
- Every concept must explain grounded facts, include at least one kill test, and include bridge lemmas.
- Every concept must declare a concept_family and the smallest_transfer_probe that would make it actionable for Shadow.
- If you repeat an existing family, you must explain what changed and why this is not the same family again.
- Supershadow should not be rewarded for novelty alone. High ontological delta without compression is weak.
- A good concept explains multiple grounded facts at once and makes awkward facts feel natural.
- A concept that merely renames the frontier should score poorly.
- No direct live-work fields such as campaign_id, target_id, objective, move_kind, new_experiment, new_target, or Aristotle instructions.
- Keep JSON valid. No markdown fences."""


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
    tensions = concept.get("tensions") or []
    family_kind = _normalize_family_kind(concept.get("family_kind"))
    smallest_transfer_probe = _clip_text(
        concept.get("smallest_transfer_probe"), 1200
    ).strip()
    why_not_same = _clip_text(
        concept.get("why_not_same_as_existing_family"), 1200
    ).strip()

    compression = min(5, max(1, explained_count))
    fit = min(5, max(1, explained_count + (1 if tensions else 0)))
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
        out.append(
            {
                "title": title,
                "summary": summary,
                "why_compressive": why_compressive,
                "bridge_lemmas": bridge_lemmas,
                "shadow_task": shadow_task,
                "recommended_next_step": recommended_next_step,
                "grounding_notes": grounding_notes,
            }
        )
    return out


def _concept_sort_key(
    concept: dict[str, Any]
) -> tuple[int, int, int, int, int, int, int, int, int, str]:
    scores = concept.get("scores") or {}
    return (
        int(scores.get("transfer_value") or 0),
        int(scores.get("family_novelty") or 0),
        -int(scores.get("family_saturation_penalty") or 0),
        int(scores.get("compression_power") or 0),
        int(scores.get("fit_to_known_facts") or 0),
        int(scores.get("bridgeability") or 0),
        int(scores.get("falsifiability") or 0),
        -int(scores.get("grounding_cost") or 0),
        -int(scores.get("speculative_risk") or 0),
        int(scores.get("ontological_delta") or 0),
        str(concept.get("concept_family") or ""),
    )


def _default_handoff_payload(concept: dict[str, Any]) -> list[dict[str, Any]]:
    probe = _clip_text(concept.get("smallest_transfer_probe"), 1200).strip()
    if not probe:
        return []
    return [
        {
            "title": f"Handoff: {concept['title']}",
            "summary": _clip_text(concept.get("worldview_summary"), 1200).strip(),
            "why_compressive": (
                f"Preserves the concept family '{concept.get('concept_family')}' while testing a smaller actionable descendant."
            ),
            "bridge_lemmas": list(concept.get("bridge_lemmas") or []),
            "shadow_task": probe,
            "recommended_next_step": probe,
            "grounding_notes": "Use the smallest_transfer_probe before escalating to heavier conceptual machinery.",
        }
    ]


def _select_family_diverse_concepts(
    concepts: list[dict[str, Any]], warnings: list[str]
) -> list[dict[str, Any]]:
    by_family: dict[str, list[dict[str, Any]]] = {}
    for concept in concepts:
        family = str(concept.get("concept_family") or "")
        by_family.setdefault(family, []).append(concept)
    unique: list[dict[str, Any]] = []
    for family, rows in by_family.items():
        rows.sort(key=_concept_sort_key, reverse=True)
        unique.append(rows[0])
        if len(rows) > 1:
            _append_warning_once(warnings, "family_repeat_filtered")
    unique.sort(key=_concept_sort_key, reverse=True)

    selected: list[dict[str, Any]] = []
    picked_families: set[str] = set()
    for family_kind in ("new", "adjacent", "established"):
        for concept in unique:
            family = str(concept.get("concept_family") or "")
            if family in picked_families:
                continue
            if str(concept.get("family_kind") or "") == family_kind:
                selected.append(concept)
                picked_families.add(family)
                break
    for concept in unique:
        family = str(concept.get("concept_family") or "")
        if family in picked_families:
            continue
        selected.append(concept)
        picked_families.add(family)
        if len(selected) >= 6:
            break
    return selected[:6]


def _normalize_supershadow_response(
    data: dict[str, Any],
    fact_basis: list[dict[str, Any]],
    family_memory: list[dict[str, Any]],
    *,
    max_handoffs: int,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
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
            continue
        kill_tests = _normalize_kill_tests(concept_raw.get("kill_tests"))
        if not kill_tests:
            _append_warning_once(warnings, "concept_missing_kill_tests")
            continue

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
            "concepts": _str_list(
                concept_raw.get("concepts"), max_items=8, max_item_chars=600
            ),
            "ontological_moves": _str_list(
                concept_raw.get("ontological_moves"), max_items=8, max_item_chars=600
            ),
            "explains_facts": explains_facts,
            "tensions": _normalize_tensions(concept_raw.get("tensions")),
            "kill_tests": kill_tests,
            "bridge_lemmas": bridge_lemmas,
            "reduce_frontier_or_rename": _clip_text(
                concept_raw.get("reduce_frontier_or_rename"), 1200
            ).strip(),
        }
        concept["scores"] = _normalize_scores(
            concept_raw.get("scores"),
            concept,
            family_memory_lookup.get(concept["concept_family"]),
        )
        if not _family_materially_advances(
            concept, family_memory_lookup.get(concept["concept_family"])
        ):
            _append_warning_once(warnings, "stale_family_suppressed")
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
    concepts_out = _select_family_diverse_concepts(concepts_out, warnings)
    concepts_out.sort(key=_concept_sort_key, reverse=True)
    normalized = {
        "worldview_summary": worldview_summary,
        "run_summary": run_summary,
        "concepts": concepts_out[:12],
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

    lines: list[str] = []
    lines.append("## Supershadow mission")
    lines.append(goal_text[:4000])
    lines.append("")
    lines.append("## Output doctrine")
    lines.append(
        "Search for the smallest language shift that compresses multiple grounded facts at once."
    )
    lines.append(
        "Supershadow is not allowed to create live work. Do not output executable targets, experiments, or Aristotle tasks."
    )
    lines.append(
        "A good concept explains grounded facts, names a kill-test, and provides bridge lemmas back to Shadow and Lean."
    )
    lines.append(
        "Each run should try to spend part of its budget on family discovery: if possible include one established family, one adjacent family, and one genuinely new family."
    )
    lines.append(
        "If you revisit a family that is already saturated, you must lower the transfer cost with a sharper smallest_transfer_probe or explain exactly what changed."
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
    lines.append("## Previous Supershadow worldview memory (JSON)")
    lines.append(json.dumps(worldview_obj, ensure_ascii=False, indent=2)[:18000])
    lines.append("")
    lines.append("## Supershadow policy memory / warnings tail (JSON)")
    lines.append(json.dumps(policy_obj, ensure_ascii=False, indent=2)[:12000])
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
        fact_basis = _build_grounded_fact_basis(db)
        pressure_map = _build_pressure_map(fact_basis)
        family_memory = _build_family_memory(db)
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
            "schema_version": 1,
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

        normalized, validation_warnings = _normalize_supershadow_response(
            data,
            fact_basis,
            family_memory,
            max_handoffs=handoff_budget,
        )
        ep = db.get_supershadow_state(SUPERSHADOW_GLOBAL_GOAL_ID)
        try:
            old_policy = json.loads(ep.get("policy_json") or "{}")
        except json.JSONDecodeError:
            old_policy = {}
        if not isinstance(old_policy, dict):
            old_policy = {}

        worldview_payload = {
            "summary": normalized.get("worldview_summary") or "",
            "latest_concept_titles": [
                str(concept.get("title") or "")
                for concept in normalized.get("concepts") or []
            ][:8],
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
            },
        )
        if validation_warnings:
            merged_policy["_supershadow_validation_warnings_tail"] = (
                list(merged_policy.get("_supershadow_validation_warnings_tail", []))
                + validation_warnings
            )[-24:]
        new_policy_json = json.dumps(merged_policy, ensure_ascii=False)
        concepts = list(normalized.get("concepts") or [])
        response_obj = {
            "output": normalized,
            "fact_basis": fact_basis,
            "pressure_map": pressure_map,
            "family_memory": family_memory,
            "meta": {
                **request_meta,
                "validation_warnings": validation_warnings,
                "raw_preview": _clip_text(raw, 4000),
                "json_retry_count": json_retry_count,
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
