"""Shadow lab: speculative research agent (writes only shadow_* tables)."""

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
from orchestrator.models import ExperimentStatus

logger = logging.getLogger("orchestrator.shadow")
_GLOBAL_SHADOW_RUN_LOCK = False

_STRIP_JSON_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)
_COUNTER_KEY_SANITIZE = re.compile(r"[^a-z0-9_.:-]+")
_PROMOTION_TOKEN_RE = re.compile(r"[a-z0-9^]+")
_PROMOTION_NUMBER_RE = re.compile(r"10\^\d+|\d+")
_PROMOTION_STOPWORDS = frozenset(
    {
        "the",
        "and",
        "for",
        "that",
        "with",
        "this",
        "from",
        "into",
        "then",
        "than",
        "only",
        "under",
        "using",
        "prove",
        "show",
        "such",
        "there",
        "exists",
        "every",
        "each",
        "where",
        "when",
        "have",
        "will",
        "would",
        "should",
        "could",
        "their",
        "about",
        "through",
        "collatz",
        "shadow",
    }
)
_PROMOTION_RUBRIC_KEYS = (
    "novel_math",
    "proof_program_leverage",
    "grounding_need",
    "expected_signal",
    "queue_fitness",
)
_PROMOTION_RUBRIC_TOTAL_MIN = 10
_PROMOTION_RUBRIC_MIN_GROUNDING_NEED = 2
_PROMOTION_RUBRIC_MIN_LEVERAGE = 2
_PROMOTION_RUBRIC_MIN_SIGNAL = 2


def _strip_json_fence(text: str) -> str:
    t = text.strip()
    t = _STRIP_JSON_FENCE.sub("", t).strip()
    return t


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
            v, _ = decoder.raw_decode(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(v, dict):
            return v
    return {}


def _counter_suffix(value: Any) -> str:
    raw = str(value or "unknown").strip().lower().replace(" ", "_")
    raw = _COUNTER_KEY_SANITIZE.sub("_", raw)
    return raw[:80] or "unknown"


def _shadow_json_retry_user_message(user: str) -> str:
    return (
        user
        + "\n\nIMPORTANT: Return only one valid JSON object."
        + " No markdown fences, no commentary, no duplicated keys."
        + " Keep the response compact: no more than 6 hypotheses,"
        + " no more than 1 promotion unless absolutely necessary,"
        + " and keep long text fields under 600 characters."
    )


def _shadow_json_repair_user_message(raw: str) -> str:
    preview = _clip_text(raw, 12000)
    return (
        "Your previous answer was invalid JSON."
        " Rewrite it as ONE valid JSON object that matches the system schema.\n"
        "Keep only the highest-signal content and stay compact:\n"
        "- at most 6 hypotheses\n"
        "- at most 1 promotion unless absolutely necessary\n"
        "- keep long text fields under 600 characters\n"
        "- prefer empty strings or shorter lists over verbose prose\n"
        "- return ONLY JSON\n\n"
        "Invalid draft to repair:\n"
        f"{preview}"
    )


async def _invoke_shadow_json(
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

    logger.warning("%s_invalid_json attempt=1 preview=%s", log_name, _clip_text(raw, 400))
    retry_temp = min(0.2, temperature)
    retry_raw = await invoke_llm(
        system,
        _shadow_json_retry_user_message(user),
        model=model,
        temperature=retry_temp,
        json_object=True,
    )
    retry_data = _safe_json_loads(retry_raw)
    if retry_data:
        logger.info("%s_json_retry_recovered retry_temperature=%s", log_name, retry_temp)
        return retry_data, retry_raw, 1

    logger.warning("%s_invalid_json attempt=2 preview=%s", log_name, _clip_text(retry_raw, 400))
    repair_raw = await invoke_llm(
        system,
        _shadow_json_repair_user_message(retry_raw or raw),
        model=model,
        temperature=0.0,
        json_object=True,
    )
    repair_data = _safe_json_loads(repair_raw)
    if repair_data:
        logger.info("%s_json_repair_recovered", log_name)
        return repair_data, repair_raw, 2

    logger.warning("%s_invalid_json attempt=3 preview=%s", log_name, _clip_text(repair_raw, 400))
    return {}, repair_raw, 2


def _clip_text(v: Any, n: int) -> str:
    return str(v or "")[:n]


def _append_warning_once(warnings: list[str], warning: str) -> None:
    if warning not in warnings:
        warnings.append(warning)


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


def _promotion_text(kind: str, payload: dict[str, Any]) -> str:
    if kind == "new_target":
        return _clip_text(payload.get("description"), 2400)
    if kind == "new_experiment":
        return _clip_text(payload.get("objective"), 2400)
    return ""


def _promotion_tokens(text: str) -> set[str]:
    tokens: set[str] = set()
    for tok in _PROMOTION_TOKEN_RE.findall(str(text or "").lower()):
        if len(tok) < 3 or tok in _PROMOTION_STOPWORDS:
            continue
        tokens.add(tok)
    return tokens


def _promotion_number_markers(text: str) -> frozenset[str]:
    return frozenset(_PROMOTION_NUMBER_RE.findall(str(text or "").lower()))


def _int_in_range(value: Any, default: int = 0, *, lo: int = 0, hi: int = 3) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        n = default
    return max(lo, min(hi, n))


def _infer_proof_program_role(kind: str, payload: dict[str, Any]) -> str:
    role = _clip_text(payload.get("proof_program_role"), 64).strip().lower()
    if role:
        return role
    blob = " ".join(
        [
            kind,
            _promotion_text(kind, payload),
            _clip_text(payload.get("grounding_reason"), 400),
            _clip_text(payload.get("expected_signal"), 400),
            _clip_text(payload.get("move_kind"), 64),
        ]
    ).lower()
    if kind == "new_target":
        if any(k in blob for k in ("define", "formalize", "interface", "scaffold", "object")):
            return "new_object"
        return "bridge_lemma"
    if any(k in blob for k in ("kill test", "falsify", "refute", "counterexample")):
        return "kill_test"
    if any(k in blob for k in ("finite", "bounded", "native_decide", "verify", "search")):
        return "finite_check"
    if "equivalence" in blob or "equivalent" in blob:
        return "equivalence"
    if any(k in blob for k in ("define", "formalize", "scaffold", "interface")):
        return "scaffold"
    return "bridge_lemma"


def _promotion_context(kind: str, payload: dict[str, Any]) -> dict[str, Any]:
    text = _promotion_text(kind, payload)
    return {
        "kind": kind,
        "campaign_id": _clip_text(payload.get("campaign_id"), 40),
        "target_id": _clip_text(payload.get("target_id"), 40),
        "text": text,
        "_tokens": _promotion_tokens(text),
        "_numbers": _promotion_number_markers(text),
    }


def _promotion_expected_signal_has_branching(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(
        phrase in lowered
        for phrase in (
            "success or failure",
            "succeeds or fails",
            "either we get",
            "either we learn",
            "if this fails",
            "if it fails",
            "if this succeeds",
            "if it succeeds",
        )
    )


def _promotion_queue_fitness(kind: str, payload: dict[str, Any]) -> int:
    if kind == "new_target":
        return 3
    role = _infer_proof_program_role(kind, payload)
    text = _promotion_text(kind, payload).lower()
    if role in ("kill_test", "finite_check"):
        if "10^7" in text or "10^8" in text or "10^9" in text:
            return 1
        if any(marker in text for marker in ("10^6", "10^5", "10^4", "n ≤ 1000", "n <= 1000")):
            return 3
        return 2
    if any(k in text for k in ("for all n", "all residue classes", "all mod 8", "all mod 16")):
        return 1
    return 2


def _infer_promotion_rubric(kind: str, payload: dict[str, Any]) -> dict[str, int]:
    role = _infer_proof_program_role(kind, payload)
    novelty_reason = _clip_text(payload.get("novelty_reason"), 1200).strip()
    grounding_reason = _clip_text(payload.get("grounding_reason"), 1600).strip()
    expected_signal = _clip_text(payload.get("expected_signal"), 1600).strip()
    move_kind = _clip_text(payload.get("move_kind"), 64).lower()

    novel_math = 1
    if role in ("new_object", "bridge_lemma", "equivalence"):
        novel_math = 2
    if novelty_reason or kind == "new_target":
        novel_math += 1
    if move_kind in ("refute", "explore") and role in ("kill_test", "finite_check"):
        novel_math = max(novel_math, 2)

    proof_program_leverage = 1
    if role in ("new_object", "bridge_lemma", "equivalence"):
        proof_program_leverage = 3
    elif role in ("kill_test", "finite_check", "scaffold"):
        proof_program_leverage = 2

    grounding_need = 1
    if grounding_reason:
        grounding_need = 2
    if any(k in grounding_reason.lower() for k in ("depends on", "unlock", "needed now", "requires live", "bridge")):
        grounding_need = 3

    signal_score = 1
    if expected_signal:
        signal_score = 2
    if _promotion_expected_signal_has_branching(expected_signal):
        signal_score = 3

    queue_fitness = _promotion_queue_fitness(kind, payload)
    return {
        "novel_math": min(3, novel_math),
        "proof_program_leverage": min(3, proof_program_leverage),
        "grounding_need": min(3, grounding_need),
        "expected_signal": min(3, signal_score),
        "queue_fitness": min(3, queue_fitness),
    }


def _normalize_promotion_rubric(kind: str, payload: dict[str, Any]) -> dict[str, Any]:
    raw = payload.get("rubric_scores")
    inferred = _infer_promotion_rubric(kind, payload)
    normalized: dict[str, int] = {}
    if isinstance(raw, dict):
        for key in _PROMOTION_RUBRIC_KEYS:
            normalized[key] = _int_in_range(raw.get(key), inferred[key])
    else:
        normalized = inferred
    total = sum(normalized[key] for key in _PROMOTION_RUBRIC_KEYS)
    passes = (
        total >= _PROMOTION_RUBRIC_TOTAL_MIN
        and normalized["proof_program_leverage"] >= _PROMOTION_RUBRIC_MIN_LEVERAGE
        and normalized["grounding_need"] >= _PROMOTION_RUBRIC_MIN_GROUNDING_NEED
        and normalized["expected_signal"] >= _PROMOTION_RUBRIC_MIN_SIGNAL
    )
    return {"scores": normalized, "total_0_15": total, "passes": passes}


def _same_promotion_lane(candidate: dict[str, Any], existing: dict[str, Any]) -> bool:
    if candidate.get("kind") != existing.get("kind"):
        return False
    if candidate.get("campaign_id") != existing.get("campaign_id"):
        return False
    if candidate.get("kind") == "new_experiment":
        return candidate.get("target_id") == existing.get("target_id")
    return True


def _looks_like_duplicate_promotion(candidate: dict[str, Any], existing_rows: list[dict[str, Any]]) -> bool:
    candidate_tokens = set(candidate.get("_tokens") or [])
    if not candidate_tokens:
        return False
    candidate_numbers = frozenset(candidate.get("_numbers") or [])
    for existing in existing_rows:
        if not _same_promotion_lane(candidate, existing):
            continue
        existing_tokens = set(existing.get("_tokens") or [])
        if not existing_tokens:
            continue
        intersection = len(candidate_tokens & existing_tokens)
        if intersection == 0:
            continue
        smaller = min(len(candidate_tokens), len(existing_tokens))
        union = len(candidate_tokens | existing_tokens)
        similar = (smaller >= 4 and intersection / smaller >= 0.78) or (
            union >= 6 and intersection / union >= 0.65 and intersection >= 5
        )
        if not similar:
            continue
        existing_numbers = frozenset(existing.get("_numbers") or [])
        if candidate_numbers and existing_numbers and candidate_numbers != existing_numbers:
            continue
        return True
    return False


def _existing_global_grounding_context(db: Database) -> list[dict[str, Any]]:
    existing: list[dict[str, Any]] = []
    for row in db.list_shadow_global_promotion_requests(
        SHADOW_GLOBAL_GOAL_ID, status="pending", limit=120
    ):
        payload = _load_json_object(row.get("payload_json"))
        kind = _clip_text(payload.get("kind"), 40).lower()
        if kind in ("new_target", "new_experiment"):
            existing.append(_promotion_context(kind, payload))

    for c in db.get_all_campaigns()[:80]:
        cid = str(c["id"])
        try:
            state = db.get_campaign_state(cid)
        except ValueError:
            continue
        for t in state.targets[:80]:
            existing.append(
                _promotion_context(
                    "new_target",
                    {"campaign_id": cid, "description": t.description},
                )
            )
        recent_experiments = sorted(
            [e for e in state.experiments if (e.objective or "").strip()],
            key=lambda x: (x.completed_at or x.submitted_at or ""),
            reverse=True,
        )
        for e in recent_experiments[:24]:
            existing.append(
                _promotion_context(
                    "new_experiment",
                    {
                        "campaign_id": cid,
                        "target_id": e.target_id,
                        "objective": e.objective,
                    },
                )
            )
    return existing


def _str_list(v: Any, *, max_items: int, max_item_chars: int) -> list[str]:
    if not isinstance(v, list):
        return []
    out: list[str] = []
    for x in v[:max_items]:
        s = str(x or "").strip()
        if s:
            out.append(s[:max_item_chars])
    return out


def _merge_policy(old: dict[str, Any], delta: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(old)
    if not delta:
        return out
    weights = delta.get("weights")
    if isinstance(weights, dict):
        for k, val in weights.items():
            if isinstance(k, str) and len(k) < 200:
                out[k] = val
    notes = delta.get("notes")
    if isinstance(notes, str) and notes.strip():
        prev = out.get("_shadow_notes_tail", [])
        if not isinstance(prev, list):
            prev = []
        tail = (prev + [notes.strip()])[-12:]
        out["_shadow_notes_tail"] = tail
    return out


def _build_shadow_user_message(db: Database, campaign_id: str) -> str:
    state = db.get_campaign_state(campaign_id)
    ep = db.get_shadow_epistemic_state(campaign_id)
    stance_raw = ep.get("stance_json") or "{}"
    policy_raw = ep.get("policy_json") or "{}"
    try:
        stance_obj = json.loads(stance_raw) if stance_raw.strip() else {}
    except json.JSONDecodeError:
        stance_obj = {}
    try:
        policy_obj = json.loads(policy_raw) if policy_raw.strip() else {}
    except json.JSONDecodeError:
        policy_obj = {}

    lines: list[str] = []
    lines.append("## Campaign (verified / live context — read-only)")
    lines.append(state.campaign.prompt[:12000])
    lines.append("")
    lines.append("## Targets (id · status · description)")
    for t in state.targets:
        lines.append(f"- {t.id} · {t.status.value} · {t.description[:2000]}")
    lines.append("")
    lines.append("## Recent experiments (focus on failures / blockers)")
    failed_like: list[Any] = []
    for e in state.experiments:
        if e.status in (
            ExperimentStatus.COMPLETED,
            ExperimentStatus.FAILED,
        ):
            failed_like.append(e)
    failed_like.sort(key=lambda x: (x.completed_at or x.submitted_at or ""), reverse=True)
    for e in failed_like[:40]:
        v = e.verdict.value if e.verdict else ""
        summ = (e.result_summary or "")[:1200]
        lines.append(
            f"- exp {e.id} · target {e.target_id} · {e.status.value} · verdict {v} · "
            f"move {e.move_kind}\n  objective: {(e.objective or '')[:800]}\n  summary: {summ}"
        )
    lines.append("")
    lines.append("## Strategic reminder")
    lines.append(
        "Invent new mathematics and proof-program structure first. Do not optimize for the number of live promotions."
    )
    lines.append(
        "Promotions are scarce grounding requests. Use them only when live work is needed to formalize a new object/interface, ground a bridge lemma, or run a bounded kill-test that will change the campaign strategy."
    )
    lines.append(
        "Promotion rubric: novel_math, proof_program_leverage, grounding_need, expected_signal, queue_fitness. Only promotions scoring at least 10/15 overall with leverage/grounding_need/expected_signal at least 2 should survive."
    )
    lines.append("")
    lines.append("## Your previous epistemic stance (JSON) — revise freely")
    lines.append(json.dumps(stance_obj, ensure_ascii=False, indent=2)[:24000])
    lines.append("")
    lines.append("## Your evolving policy weights / meta (JSON) — may update policy_delta")
    lines.append(json.dumps(policy_obj, ensure_ascii=False, indent=2)[:12000])
    lines.append("")
    lines.append("## Target id list for new_experiment promotions")
    lines.append(", ".join(t.id for t in state.targets[:80]))
    return "\n".join(lines)


SHADOW_SYSTEM = """You are the Shadow Research Agent for a formal verification campaign (Lean 4 / Aristotle).

You are NOT the live manager. Nothing you say is verified truth. You may speculate aggressively: new lemmas, reformulations,
alternative proof programs, new measures or invariants, even hypothetical foundational shifts or new axioms — but you must
label speculative content clearly inside body_md and lean_snippet (e.g. "SPECULATIVE", "requires new axioms").

Your purpose is to invent new mathematical structure and proof programs without polluting live work.
Do NOT optimize for number of promotions or immediate theorem throughput. Promotions are rare grounding requests:
use them only when live work is needed to formalize a new object/interface, ground a bridge lemma, or run a bounded kill-test.
If the queue already has enough live work, emit 0 promotions and focus on hypotheses + strategy.

You read campaign state and prior shadow stance/policy. You OUTPUT STRICT JSON with this shape:
{
  "stance": {
    "summary": "2-8 sentences: current narrative and what to try next",
    "belief_threads": [ { "id": "short", "claim": "...", "confidence_0_1": 0.3, "falsify": "..." } ],
    "next_focus": "one paragraph"
  },
  "policy_delta": {
    "weights": { "exploration_bias": 0.7, "prefer_reformulation": true },
    "notes": "how you are adjusting your epistemic strategy this run"
  },
  "hypotheses": [
    {
      "kind": "lemma_sketch|reformulation|new_axiom|alternative_arithmetic|exploration|proof_program|other",
      "title": "short",
      "body_md": "markdown, can be long",
      "lean_snippet": "optional Lean fragment or empty string",
      "evidence": [ { "experiment_id": "optional", "target_id": "optional", "note": "..." } ]
    }
  ],
  "promotion_requests": [
    {
      "kind": "new_target",
      "description": "concrete verification target text for Aristotle",
      "proof_program_role": "new_object|bridge_lemma|equivalence|kill_test|finite_check|scaffold",
      "grounding_reason": "why this deserves live grounding now",
      "expected_signal": "what success or failure teaches us",
      "novelty_reason": "how this differs from queued/recent work",
      "rubric_scores": {
        "novel_math": 0,
        "proof_program_leverage": 0,
        "grounding_need": 0,
        "expected_signal": 0,
        "queue_fitness": 0
      }
    },
    {
      "kind": "new_experiment",
      "target_id": "must match an existing target id",
      "objective": "...",
      "move_kind": "explore|prove|refute|...",
      "move_note": "shadow:...",
      "proof_program_role": "new_object|bridge_lemma|equivalence|kill_test|finite_check|scaffold",
      "grounding_reason": "why this deserves live grounding now",
      "expected_signal": "what success or failure teaches us",
      "novelty_reason": "how this differs from queued/recent work",
      "rubric_scores": {
        "novel_math": 0,
        "proof_program_leverage": 0,
        "grounding_need": 0,
        "expected_signal": 0,
        "queue_fitness": 0
      },
      "defer_aristotle_submit": false
    }
  ],
  "run_summary": "one paragraph for the run log"
}

Promotion rubric (score each 0-3 before emitting any promotion):
- novel_math: does this introduce or ground genuinely new structure rather than restating queued work?
- proof_program_leverage: if grounded, does it unlock multiple next moves or a key bridge?
- grounding_need: does this truly need live grounding now, rather than staying speculative?
- expected_signal: will success OR failure teach us something concrete?
- queue_fitness: is this a sharp use of a scarce live slot right now?

Rules:
- hypotheses: 3–12 items; promotion_requests: 0–3 items.
- Order promotion_requests from highest-value grounding request to lowest.
- Only emit promotions that pass the rubric: total score >= 10/15, proof_program_leverage >= 2, grounding_need >= 2, expected_signal >= 2.
- Never claim the main conjecture is proved unless you are restating a verified fact from the context (prefer not to).
- new_experiment.target_id MUST be one of the ids listed in the user message.
- Optional defer_aristotle_submit: if true, the experiment is only created; Aristotle submit waits for the manager tick (no immediate CLI submit on approve).
- Keep JSON valid. No markdown fences."""

SHADOW_GLOBAL_GOAL_ID = "global_collatz"
_SUPERSHADOW_GLOBAL_GOAL_ID = "global_collatz_supershadow"

SHADOW_GLOBAL_SYSTEM = """You are the global Shadow Research Manager with one mission:
produce a mathematically correct proof program for the Collatz conjecture that can eventually be grounded in Lean 4.

You are NOT bound to conservative assumptions in ideation. You may hypothesize new structures, operators, invariants,
bridges, axiom candidates, or alternative arithmetic frameworks. Be explicit when speculative. Work backwards from a solved world:
assume Collatz is solved, then identify the minimum chain of assumptions/lemmas needed to make that world coherent.

Your job is not to act like a second live manager. Your job is to invent the mathematics and proof program that the live system
cannot yet see. Promotions are scarce grounding requests, not the main product. Prefer hypotheses, bridge lemmas, new mathematical
objects, and strategic reframings. Only emit a promotion when live grounding is genuinely needed to:
1. formalize a new object/interface/lemma family the proof program now depends on,
2. ground a bridge lemma or equivalence that unlocks multiple next steps, or
3. run a bounded kill-test / finite check whose result will change the proof program.
If the queue is already busy or the work is only an incremental restatement, emit 0 promotions.

You OUTPUT STRICT JSON with this shape:
{
  "stance": {
    "summary": "2-8 sentences",
    "belief_threads": [ { "id": "short", "claim": "...", "confidence_0_1": 0.3, "falsify": "..." } ],
    "next_focus": "one paragraph"
  },
  "policy_delta": {
    "weights": { "exploration_bias": 0.9, "backward_chaining_bias": 0.95 },
    "notes": "how strategy changed this run"
  },
  "solved_world": {
    "claim": "short theorem-level solved-world claim",
    "assumption_frontier": ["assumption 1", "assumption 2"],
    "bridge_lemmas": ["lemma 1", "lemma 2"],
    "lean_landing_zone": "which pieces can be formalized now vs later"
  },
  "hypotheses": [
    {
      "kind": "lemma_sketch|reformulation|new_axiom|alternative_arithmetic|exploration|proof_program|other",
      "title": "short",
      "body_md": "markdown, may be long, include SPECULATIVE tags where needed",
      "lean_snippet": "optional Lean fragment or empty string",
      "evidence": [ { "campaign_id": "optional", "experiment_id": "optional", "target_id": "optional", "note": "..." } ],
      "source_incubation_ids": ["optional Supershadow incubation ids from the user message"]
    }
  ],
  "promotion_requests": [
    {
      "kind": "new_target",
      "campaign_id": "required",
      "description": "concrete target text",
      "source_incubation_ids": ["optional Supershadow incubation ids from the user message"],
      "proof_program_role": "new_object|bridge_lemma|equivalence|kill_test|finite_check|scaffold",
      "grounding_reason": "why this deserves live grounding now",
      "expected_signal": "what success or failure teaches us",
      "novelty_reason": "how this differs from queued/recent work",
      "rubric_scores": {
        "novel_math": 0,
        "proof_program_leverage": 0,
        "grounding_need": 0,
        "expected_signal": 0,
        "queue_fitness": 0
      }
    },
    {
      "kind": "new_experiment",
      "campaign_id": "required",
      "target_id": "required",
      "objective": "...",
      "source_incubation_ids": ["optional Supershadow incubation ids from the user message"],
      "move_kind": "explore|prove|refute|...",
      "move_note": "shadow:...",
      "proof_program_role": "new_object|bridge_lemma|equivalence|kill_test|finite_check|scaffold",
      "grounding_reason": "why this deserves live grounding now",
      "expected_signal": "what success or failure teaches us",
      "novelty_reason": "how this differs from queued/recent work",
      "rubric_scores": {
        "novel_math": 0,
        "proof_program_leverage": 0,
        "grounding_need": 0,
        "expected_signal": 0,
        "queue_fitness": 0
      },
      "defer_aristotle_submit": false
    }
  ],
  "run_summary": "one paragraph"
}

Promotion rubric (score each 0-3 before emitting any promotion):
- novel_math: does this introduce or ground genuinely new structure rather than restating queued work?
- proof_program_leverage: if grounded, does it unlock multiple next moves or a key bridge?
- grounding_need: does this truly need live grounding now, rather than staying speculative?
- expected_signal: will success OR failure teach us something concrete?
- queue_fitness: is this a sharp use of a scarce live slot right now?

Rules:
- hypotheses: 4-14 items; promotion_requests: 0-3 items.
- Order promotion_requests from most strategically necessary to least.
- Only emit promotions that pass the rubric: total score >= 10/15, proof_program_leverage >= 2, grounding_need >= 2, expected_signal >= 2.
- If a hypothesis or promotion descends from a listed Supershadow incubation, include its incubation id in source_incubation_ids.
- A promotion request must include campaign_id from the allowed list in the user message.
- new_experiment.target_id must match a target under that campaign_id.
- Optional defer_aristotle_submit on promotions: if true, skip immediate Aristotle submit on approve (manager will submit on its next tick if enabled).
- Keep JSON valid. No markdown fences."""


def _build_shadow_global_user_message(
    db: Database,
    goal_text: str,
    *,
    promotion_budget: int | None = None,
    experiment_promotion_budget: int | None = None,
    suppress_promotions_reason: str | None = None,
) -> str:
    ep = db.get_shadow_global_state(SHADOW_GLOBAL_GOAL_ID)
    stance_raw = ep.get("stance_json") or "{}"
    policy_raw = ep.get("policy_json") or "{}"
    try:
        stance_obj = json.loads(stance_raw) if stance_raw.strip() else {}
    except json.JSONDecodeError:
        stance_obj = {}
    try:
        policy_obj = json.loads(policy_raw) if policy_raw.strip() else {}
    except json.JSONDecodeError:
        policy_obj = {}

    all_campaigns = db.get_all_campaigns()
    lines: list[str] = []
    lines.append("## Global mission")
    lines.append(goal_text[:4000])
    lines.append("")
    lines.append("## Strategic reminder")
    lines.append(
        "Invent new mathematics and proof-program structure first. Do not optimize for the number of live promotions."
    )
    lines.append(
        "Use live grounding only when it formalizes a new object/interface, grounds a bridge lemma, or runs a bounded kill-test that changes the strategy."
    )
    lines.append(
        "Promotion rubric: novel_math, proof_program_leverage, grounding_need, expected_signal, queue_fitness. Only promotions scoring at least 10/15 overall with leverage/grounding_need/expected_signal at least 2 should survive."
    )
    if suppress_promotions_reason:
        lines.append(f"Promotion budget this run: 0. {suppress_promotions_reason}")
    else:
        budget_text = "use fewer if possible"
        if promotion_budget is not None:
            budget_text = f"at most {max(0, promotion_budget)} total"
            if experiment_promotion_budget is not None:
                budget_text += f", including at most {max(0, experiment_promotion_budget)} experiments"
        lines.append(f"Promotion budget this run: {budget_text}.")
    lines.append("")
    lines.append("## Campaigns available for promotions (campaign_id -> prompt)")
    for c in all_campaigns[:80]:
        lines.append(f"- {c['id']} -> {(c.get('prompt') or '')[:240]}")
    lines.append("")
    lines.append("## Pending live grounding already in queue (avoid duplicates)")
    pending_promotions = db.list_shadow_global_promotion_requests(
        SHADOW_GLOBAL_GOAL_ID, status="pending", limit=24
    )
    if pending_promotions:
        for row in pending_promotions:
            payload = _load_json_object(row.get("payload_json"))
            kind = _clip_text(payload.get("kind"), 40).lower()
            headline = _promotion_text(kind, payload)[:320]
            if not headline:
                continue
            cid = _clip_text(payload.get("campaign_id"), 40)
            tid = _clip_text(payload.get("target_id"), 40)
            lane = f"{kind} · campaign {cid}"
            if tid:
                lane += f" · target {tid}"
            lines.append(f"- pending {lane} · {headline}")
    else:
        lines.append("- none")
    lines.append("")

    lines.append("## Supershadow concept incubations (preserve lineage; cite incubation ids when used)")
    incubations = db.list_supershadow_incubations(
        _SUPERSHADOW_GLOBAL_GOAL_ID, limit=20
    )
    incubation_events = db.list_supershadow_incubation_events(
        [str(row["id"]) for row in incubations]
    )
    latest_event_by_incubation: dict[str, dict[str, Any]] = {}
    for event in incubation_events:
        incubation_id = str(event.get("incubation_id") or "")
        if incubation_id and incubation_id not in latest_event_by_incubation:
            latest_event_by_incubation[incubation_id] = event
    if incubations:
        for row in incubations:
            payload = _load_json_object(row.get("concept_packet_json"))
            title = _clip_text(row.get("title") or payload.get("title"), 240)
            summary = _clip_text(payload.get("summary"), 800)
            why_compressive = _clip_text(payload.get("why_compressive"), 800)
            shadow_task = _clip_text(payload.get("shadow_task"), 800)
            bridge_lemmas = _str_list(
                payload.get("bridge_lemmas"), max_items=6, max_item_chars=300
            )
            lines.append(
                f"- incubation_id {row['id']} | status {row.get('status') or 'incubating'} | {title}"
            )
            if summary:
                lines.append(f"  summary: {summary}")
            if why_compressive:
                lines.append(f"  why_compressive: {why_compressive}")
            if shadow_task:
                lines.append(f"  shadow_task: {shadow_task}")
            if bridge_lemmas:
                lines.append(f"  bridge_lemmas: {' | '.join(bridge_lemmas)}")
            latest_event = latest_event_by_incubation.get(str(row["id"]))
            if latest_event:
                lines.append(
                    f"  latest_lineage: {latest_event.get('event_kind')} · {(_clip_text(latest_event.get('event_summary'), 600))}"
                )
    else:
        lines.append("- none")
    lines.append("")

    lines.append("## Cross-campaign state and recent outcomes")
    for c in all_campaigns[:20]:
        cid = str(c["id"])
        try:
            state = db.get_campaign_state(cid)
        except ValueError:
            continue
        lines.append(f"### Campaign {cid}: {(state.campaign.prompt or '')[:800]}")
        lines.append("Targets (target_id · status · description):")
        for t in state.targets[:40]:
            lines.append(f"- {cid} · {t.id} · {t.status.value} · {t.description[:400]}")
        recent: list[Any] = []
        for e in state.experiments:
            if e.status in (ExperimentStatus.COMPLETED, ExperimentStatus.FAILED):
                recent.append(e)
        recent.sort(key=lambda x: (x.completed_at or x.submitted_at or ""), reverse=True)
        lines.append("Recent experiments:")
        for e in recent[:25]:
            v = e.verdict.value if e.verdict else ""
            lines.append(
                f"- {cid} · exp {e.id} · target {e.target_id} · {e.status.value} · verdict {v} · "
                f"move {e.move_kind}\n  objective: {(e.objective or '')[:500]}\n  summary: {(e.result_summary or '')[:800]}"
            )
        lines.append("")

    lines.append("## Previous global stance (JSON)")
    lines.append(json.dumps(stance_obj, ensure_ascii=False, indent=2)[:30000])
    lines.append("")
    lines.append("## Global evolving policy (JSON)")
    lines.append(json.dumps(policy_obj, ensure_ascii=False, indent=2)[:16000])
    lines.append("")
    lines.append("## Allowed promotion identifiers")
    lines.append("Format for target references: campaign_id · target_id")
    for c in all_campaigns[:20]:
        cid = str(c["id"])
        try:
            state = db.get_campaign_state(cid)
        except ValueError:
            continue
        for t in state.targets[:80]:
            lines.append(f"- {cid} · {t.id}")
    return "\n".join(lines)


def _normalize_stance(data: dict[str, Any]) -> dict[str, Any]:
    raw = data.get("stance")
    if not isinstance(raw, dict):
        raw = {}
    summary = _clip_text(raw.get("summary") or data.get("run_summary") or "", 4000)
    next_focus = _clip_text(raw.get("next_focus"), 4000)
    bt_out: list[dict[str, Any]] = []
    bt_raw = raw.get("belief_threads")
    if isinstance(bt_raw, list):
        for b in bt_raw[:16]:
            if not isinstance(b, dict):
                continue
            try:
                conf = float(b.get("confidence_0_1", 0.25))
            except (TypeError, ValueError):
                conf = 0.25
            conf = min(1.0, max(0.0, conf))
            bt_out.append(
                {
                    "id": _clip_text(b.get("id"), 80) or "thread",
                    "claim": _clip_text(b.get("claim"), 1000),
                    "confidence_0_1": conf,
                    "falsify": _clip_text(b.get("falsify"), 1000),
                }
            )
    return {"summary": summary, "belief_threads": bt_out, "next_focus": next_focus}


def _normalize_solved_world(data: dict[str, Any]) -> dict[str, Any]:
    sw = data.get("solved_world")
    if not isinstance(sw, dict):
        sw = {}
    return {
        "claim": _clip_text(sw.get("claim"), 1200),
        "assumption_frontier": _str_list(sw.get("assumption_frontier"), max_items=24, max_item_chars=600),
        "bridge_lemmas": _str_list(sw.get("bridge_lemmas"), max_items=24, max_item_chars=600),
        "lean_landing_zone": _clip_text(sw.get("lean_landing_zone"), 3000),
    }


def _score_hypothesis(h: dict[str, Any]) -> tuple[int, dict[str, int], str]:
    title = _clip_text(h.get("title"), 400).lower()
    body = _clip_text(h.get("body_md"), 4000).lower()
    lean = _clip_text(h.get("lean_snippet"), 3000).lower()
    axioms = sum(1 for k in ("axiom", "new axiom", "requires new axioms", "foundation shift") if k in (title + " " + body))
    has_lean = 1 if lean.strip() else 0
    has_falsify = 1 if any(k in body for k in ("falsify", "counterexample", "disprove", "refute")) else 0
    evidence_count = len(h.get("evidence") or []) if isinstance(h.get("evidence"), list) else 0
    score = 35 + has_lean * 20 + has_falsify * 15 + min(3, evidence_count) * 8 - min(3, axioms) * 10
    score = max(0, min(100, score))
    if axioms >= 2:
        tier = "C"
    elif has_lean:
        tier = "A"
    else:
        tier = "B"
    breakdown = {
        "base": 35,
        "lean_bonus": has_lean * 20,
        "falsify_bonus": has_falsify * 15,
        "evidence_bonus": min(3, evidence_count) * 8,
        "axiom_penalty": min(3, axioms) * 10,
    }
    return score, breakdown, tier


def _normalize_source_incubation_ids(
    raw: Any, valid_ids: set[str], *, max_items: int = 4
) -> list[str]:
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in raw[: max_items * 2]:
        incubation_id = _clip_text(item, 80).strip()
        if not incubation_id or incubation_id in seen or incubation_id not in valid_ids:
            continue
        seen.add(incubation_id)
        out.append(incubation_id)
        if len(out) >= max_items:
            break
    return out


def _normalize_global_hypotheses(
    data: dict[str, Any], *, valid_incubation_ids: set[str]
) -> list[dict[str, Any]]:
    hs_raw = data.get("hypotheses")
    hs: list[dict[str, Any]] = []
    if not isinstance(hs_raw, list):
        return hs
    for h in hs_raw[:40]:
        if not isinstance(h, dict):
            continue
        ev_out: list[dict[str, Any]] = []
        ev_raw = h.get("evidence")
        if isinstance(ev_raw, list):
            for ev in ev_raw[:20]:
                if not isinstance(ev, dict):
                    continue
                ev_out.append(
                    {
                        "campaign_id": _clip_text(ev.get("campaign_id"), 40),
                        "experiment_id": _clip_text(ev.get("experiment_id"), 40),
                        "target_id": _clip_text(ev.get("target_id"), 40),
                        "note": _clip_text(ev.get("note"), 1200),
                    }
                )
        score, breakdown, tier = _score_hypothesis(h)
        body = _clip_text(h.get("body_md"), 12000)
        hs.append(
            {
                "kind": _clip_text(h.get("kind") or "exploration", 64),
                "title": _clip_text(h.get("title"), 500),
                "body_md": body,
                "lean_snippet": _clip_text(h.get("lean_snippet"), 12000),
                "evidence": ev_out,
                "source_incubation_ids": _normalize_source_incubation_ids(
                    h.get("source_incubation_ids"), valid_incubation_ids
                ),
                "score_0_100": score,
                "score_breakdown": breakdown,
                "groundability_tier": tier,
                "kill_test": _clip_text(h.get("kill_test"), 1200)
                or "Design the smallest computable contradiction test that would refute this hypothesis quickly.",
            }
        )
    hs.sort(key=lambda x: int(x.get("score_0_100") or 0), reverse=True)
    return hs[:24]


def _normalize_global_promotions(
    data: dict[str, Any],
    db: Database,
    *,
    max_promotions: int,
    max_experiments: int,
    valid_incubation_ids: set[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    raw = data.get("promotion_requests")
    if not isinstance(raw, list):
        return [], []
    warnings: list[str] = []
    if max_promotions <= 0:
        _append_warning_once(warnings, "promotion_budget_zero")
        return [], warnings
    campaigns = {str(c["id"]) for c in db.get_all_campaigns()}
    valid_targets: dict[str, set[str]] = {}
    for cid in list(campaigns)[:200]:
        try:
            st = db.get_campaign_state(cid)
        except ValueError:
            continue
        valid_targets[cid] = {t.id for t in st.targets}
    existing = _existing_global_grounding_context(db)
    out: list[dict[str, Any]] = []
    experiments_kept = 0
    for p in raw[:40]:
        if not isinstance(p, dict):
            continue
        kind = _clip_text(p.get("kind"), 40).lower()
        cid = _clip_text(p.get("campaign_id"), 40)
        if cid not in campaigns:
            continue
        if kind == "new_target":
            desc = _clip_text(p.get("description"), 2000).strip()
            if not desc:
                continue
            grounding_reason = _clip_text(p.get("grounding_reason"), 1600).strip()
            expected_signal = _clip_text(p.get("expected_signal"), 1600).strip()
            novelty_reason = _clip_text(p.get("novelty_reason"), 1200).strip()
            if not grounding_reason:
                _append_warning_once(warnings, "promotion_missing_grounding_reason")
                continue
            if not expected_signal:
                _append_warning_once(warnings, "promotion_missing_expected_signal")
                continue
            if not novelty_reason:
                _append_warning_once(warnings, "promotion_missing_novelty_reason")
                continue
            rubric = _normalize_promotion_rubric(kind, p)
            if not rubric["passes"]:
                _append_warning_once(warnings, "promotion_below_rubric")
                continue
            obj = {
                "kind": kind,
                "campaign_id": cid,
                "description": desc,
                "source_incubation_ids": _normalize_source_incubation_ids(
                    p.get("source_incubation_ids"), valid_incubation_ids
                ),
                "proof_program_role": _infer_proof_program_role(kind, p),
                "grounding_reason": grounding_reason,
                "expected_signal": expected_signal,
                "novelty_reason": novelty_reason,
                "rubric_scores": dict(rubric["scores"]),
                "rubric_total_0_15": int(rubric["total_0_15"]),
            }
        elif kind == "new_experiment":
            tid = _clip_text(p.get("target_id"), 40)
            if tid not in valid_targets.get(cid, set()):
                continue
            objective = _clip_text(p.get("objective"), 2400).strip()
            if not objective:
                continue
            grounding_reason = _clip_text(p.get("grounding_reason"), 1600).strip()
            expected_signal = _clip_text(p.get("expected_signal"), 1600).strip()
            novelty_reason = _clip_text(p.get("novelty_reason"), 1200).strip()
            if not grounding_reason:
                _append_warning_once(warnings, "promotion_missing_grounding_reason")
                continue
            if not expected_signal:
                _append_warning_once(warnings, "promotion_missing_expected_signal")
                continue
            if not novelty_reason:
                _append_warning_once(warnings, "promotion_missing_novelty_reason")
                continue
            rubric = _normalize_promotion_rubric(kind, p)
            if not rubric["passes"]:
                _append_warning_once(warnings, "promotion_below_rubric")
                continue
            obj = {
                "kind": kind,
                "campaign_id": cid,
                "target_id": tid,
                "objective": objective,
                "source_incubation_ids": _normalize_source_incubation_ids(
                    p.get("source_incubation_ids"), valid_incubation_ids
                ),
                "move_kind": _clip_text(p.get("move_kind") or "explore", 64),
                "move_note": _clip_text(p.get("move_note") or "shadow:global", 2000),
                "proof_program_role": _infer_proof_program_role(kind, p),
                "grounding_reason": grounding_reason,
                "expected_signal": expected_signal,
                "novelty_reason": novelty_reason,
                "rubric_scores": dict(rubric["scores"]),
                "rubric_total_0_15": int(rubric["total_0_15"]),
                "defer_aristotle_submit": bool(p.get("defer_aristotle_submit")),
            }
        else:
            continue
        candidate = _promotion_context(kind, obj)
        if _looks_like_duplicate_promotion(candidate, existing):
            _append_warning_once(warnings, "promotion_duplicate_filtered")
            continue
        if len(out) >= max_promotions:
            _append_warning_once(warnings, "promotion_cap_applied")
            break
        if kind == "new_experiment":
            if experiments_kept >= max(0, max_experiments):
                _append_warning_once(warnings, "experiment_promotion_cap_applied")
                continue
            experiments_kept += 1
        out.append(obj)
        existing.append(candidate)
    return out[:20], warnings


def _normalize_global_response(
    data: dict[str, Any],
    db: Database,
    *,
    max_promotions: int | None = None,
    max_experiments: int | None = None,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    max_promotions = (
        int(app_config.SHADOW_GLOBAL_MAX_PROMOTIONS_PER_RUN)
        if max_promotions is None
        else int(max_promotions)
    )
    max_experiments = (
        int(app_config.SHADOW_GLOBAL_MAX_EXPERIMENT_PROMOTIONS_PER_RUN)
        if max_experiments is None
        else int(max_experiments)
    )
    stance = _normalize_stance(data)
    solved_world = _normalize_solved_world(data)
    if not solved_world.get("claim"):
        warnings.append("missing_solved_world_claim")
    valid_incubation_ids = {
        str(row.get("id") or "")
        for row in db.list_supershadow_incubations(
            _SUPERSHADOW_GLOBAL_GOAL_ID, limit=80
        )
        if str(row.get("id") or "")
    }
    hypotheses = _normalize_global_hypotheses(
        data, valid_incubation_ids=valid_incubation_ids
    )
    if not hypotheses:
        warnings.append("no_hypotheses")
    promotions, promotion_warnings = _normalize_global_promotions(
        data,
        db,
        max_promotions=max_promotions,
        max_experiments=max_experiments,
        valid_incubation_ids=valid_incubation_ids,
    )
    warnings.extend(promotion_warnings)
    run_summary = _clip_text(data.get("run_summary"), 4000)
    if not run_summary:
        run_summary = _clip_text(stance.get("summary"), 2000)
    policy_delta = data.get("policy_delta")
    if not isinstance(policy_delta, dict):
        policy_delta = {}
    norm = {
        "stance": stance,
        "policy_delta": policy_delta,
        "solved_world": solved_world,
        "hypotheses": hypotheses,
        "promotion_requests": promotions,
        "run_summary": run_summary,
    }
    return norm, warnings


async def run_shadow_lab(
    db: Database,
    campaign_id: str,
    *,
    trigger_kind: str = "manual",
) -> dict[str, Any]:
    if not app_config.LLM_API_KEY:
        return {"ok": False, "error": "LLM_API_KEY not set"}

    user = _build_shadow_user_message(db, campaign_id)
    model = app_config.SHADOW_LLM_MODEL or app_config.LLM_MODEL
    temp = float(app_config.SHADOW_LLM_TEMPERATURE)

    try:
        data, raw, json_retry_count = await _invoke_shadow_json(
            system=SHADOW_SYSTEM,
            user=user,
            model=model,
            temperature=temp,
            log_name="shadow",
        )
    except Exception:
        logger.exception("Shadow LLM call failed")
        return {"ok": False, "error": "llm_request_failed"}

    if not data:
        logger.warning(
            "shadow_invalid_json campaign_id=%s trigger=%s preview=%s",
            campaign_id,
            trigger_kind,
            _clip_text(raw, 400),
        )
        return {
            "ok": False,
            "error": "invalid_json",
            "raw_preview": raw[:2000],
            "json_retry_count": json_retry_count,
        }

    ep = db.get_shadow_epistemic_state(campaign_id)
    try:
        old_policy = json.loads(ep.get("policy_json") or "{}")
    except json.JSONDecodeError:
        old_policy = {}
    if not isinstance(old_policy, dict):
        old_policy = {}

    stance = data.get("stance")
    if not isinstance(stance, dict):
        stance = {"summary": str(data.get("run_summary") or ""), "belief_threads": [], "next_focus": ""}
    new_stance_json = json.dumps(stance, ensure_ascii=False)

    policy_delta = data.get("policy_delta")
    if not isinstance(policy_delta, dict):
        policy_delta = {}
    merged_policy = _merge_policy(old_policy, policy_delta)
    new_policy_json = json.dumps(merged_policy, ensure_ascii=False)

    hypotheses_raw = data.get("hypotheses")
    hypotheses: list[dict[str, Any]] = []
    if isinstance(hypotheses_raw, list):
        for h in hypotheses_raw[:24]:
            if isinstance(h, dict):
                hypotheses.append(h)

    promos_raw = data.get("promotion_requests")
    promotions: list[dict[str, Any]] = []
    if isinstance(promos_raw, list):
        for p in promos_raw[:16]:
            if isinstance(p, dict):
                promotions.append(p)

    run_summary = str(data.get("run_summary") or "")[:4000]

    try:
        run_id = db.shadow_commit_run(
            campaign_id,
            trigger_kind=trigger_kind,
            summary=run_summary,
            response_obj=data,
            new_stance_json=new_stance_json,
            new_policy_json=new_policy_json,
            hypotheses=hypotheses,
            promotions=promotions,
        )
    except Exception:
        logger.exception("shadow_commit_run failed")
        return {"ok": False, "error": "db_commit_failed"}

    return {
        "ok": True,
        "run_id": run_id,
        "hypothesis_count": len(hypotheses),
        "promotion_count": len(promotions),
        "summary": run_summary,
        "json_retry_count": json_retry_count,
    }


async def run_shadow_global_lab(
    db: Database,
    *,
    goal_text: str,
    trigger_kind: str = "manual",
    promotion_budget: int | None = None,
    experiment_promotion_budget: int | None = None,
    suppress_promotions_reason: str | None = None,
) -> dict[str, Any]:
    global _GLOBAL_SHADOW_RUN_LOCK
    if not app_config.LLM_API_KEY:
        return {"ok": False, "error": "LLM_API_KEY not set"}
    if _GLOBAL_SHADOW_RUN_LOCK:
        return {"ok": False, "error": "shadow_global_run_in_progress"}
    _GLOBAL_SHADOW_RUN_LOCK = True
    db.ensure_shadow_global_state_row(SHADOW_GLOBAL_GOAL_ID, goal_text=goal_text)
    try:
        if promotion_budget is None:
            promotion_budget = int(app_config.SHADOW_GLOBAL_MAX_PROMOTIONS_PER_RUN)
        if experiment_promotion_budget is None:
            experiment_promotion_budget = int(
                app_config.SHADOW_GLOBAL_MAX_EXPERIMENT_PROMOTIONS_PER_RUN
            )
        user = _build_shadow_global_user_message(
            db,
            goal_text,
            promotion_budget=promotion_budget,
            experiment_promotion_budget=experiment_promotion_budget,
            suppress_promotions_reason=suppress_promotions_reason,
        )
        model = app_config.SHADOW_LLM_MODEL or app_config.LLM_MODEL
        temp = float(app_config.SHADOW_LLM_TEMPERATURE)
        request_meta = {
            "model": model,
            "temperature": temp,
            "system_prompt_sha256": hashlib.sha256(SHADOW_GLOBAL_SYSTEM.encode("utf-8")).hexdigest(),
            "user_prompt_sha256": hashlib.sha256(user.encode("utf-8")).hexdigest(),
            "schema_version": 3,
            "trigger_kind": trigger_kind,
            "promotion_budget": promotion_budget,
            "experiment_promotion_budget": experiment_promotion_budget,
            "suppress_promotions_reason": suppress_promotions_reason or "",
        }

        try:
            data, raw, json_retry_count = await _invoke_shadow_json(
                system=SHADOW_GLOBAL_SYSTEM,
                user=user,
                model=model,
                temperature=temp,
                log_name="shadow_global",
            )
        except Exception:
            logger.exception("Global shadow LLM call failed")
            return {"ok": False, "error": "llm_request_failed"}

        if not data:
            logger.warning(
                "shadow_global_invalid_json trigger=%s preview=%s",
                trigger_kind,
                _clip_text(raw, 400),
            )
            return {
                "ok": False,
                "error": "invalid_json",
                "raw_preview": raw[:2000],
                "json_retry_count": json_retry_count,
            }
        normalized, validation_warnings = _normalize_global_response(
            data,
            db,
            max_promotions=promotion_budget,
            max_experiments=experiment_promotion_budget,
        )

        ep = db.get_shadow_global_state(SHADOW_GLOBAL_GOAL_ID)
        try:
            old_policy = json.loads(ep.get("policy_json") or "{}")
        except json.JSONDecodeError:
            old_policy = {}
        if not isinstance(old_policy, dict):
            old_policy = {}

        stance = normalized.get("stance") or {}
        new_stance_json = json.dumps(stance, ensure_ascii=False)

        policy_delta = normalized.get("policy_delta")
        if not isinstance(policy_delta, dict):
            policy_delta = {}
        merged_policy = _merge_policy(old_policy, policy_delta)
        if validation_warnings:
            merged_policy["_shadow_validation_warnings_tail"] = (
                list(merged_policy.get("_shadow_validation_warnings_tail", []))
                + validation_warnings
            )[-24:]
        new_policy_json = json.dumps(merged_policy, ensure_ascii=False)
        hypotheses = list(normalized.get("hypotheses") or [])
        promotions = list(normalized.get("promotion_requests") or [])
        run_summary = _clip_text(normalized.get("run_summary"), 4000)

        response_obj = {
            "output": normalized,
            "meta": {
                **request_meta,
                "validation_warnings": validation_warnings,
                "raw_preview": _clip_text(raw, 4000),
                "json_retry_count": json_retry_count,
            },
        }
        try:
            run_id = db.shadow_global_commit_run(
                SHADOW_GLOBAL_GOAL_ID,
                trigger_kind=trigger_kind,
                summary=run_summary,
                response_obj=response_obj,
                new_stance_json=new_stance_json,
                new_policy_json=new_policy_json,
                hypotheses=hypotheses,
                promotions=promotions,
                goal_text=goal_text,
            )
        except Exception:
            logger.exception("shadow_global_commit_run failed")
            return {"ok": False, "error": "db_commit_failed"}

        return {
            "ok": True,
            "run_id": run_id,
            "hypothesis_count": len(hypotheses),
            "promotion_count": len(promotions),
            "summary": run_summary,
            "validation_warnings": validation_warnings,
            "json_retry_count": json_retry_count,
        }
    finally:
        _GLOBAL_SHADOW_RUN_LOCK = False


async def shadow_global_loop(db: Database) -> None:
    """Autonomous global shadow loop (keeps thinking; suppresses promotions when queue is full)."""
    if not app_config.SHADOW_GLOBAL_AUTO_ENABLED:
        return
    while True:
        try:
            pending = len(
                db.list_shadow_global_promotion_requests(
                    SHADOW_GLOBAL_GOAL_ID, status="pending", limit=500
                )
            )
            promotion_budget = int(app_config.SHADOW_GLOBAL_MAX_PROMOTIONS_PER_RUN)
            experiment_promotion_budget = int(
                app_config.SHADOW_GLOBAL_MAX_EXPERIMENT_PROMOTIONS_PER_RUN
            )
            suppress_promotions_reason = ""
            if pending >= int(app_config.SHADOW_GLOBAL_MAX_PENDING_PROMOTIONS):
                promotion_budget = 0
                experiment_promotion_budget = 0
                suppress_promotions_reason = (
                    f"{pending} promotion(s) are already waiting for review, so this run should"
                    " refine the proof program without adding more live queue pressure."
                )
                db.increment_ops_counter(
                    "shadow_global:auto_run_promotions_suppressed_pending_cap", 1
                )
            res = await run_shadow_global_lab(
                db,
                goal_text=app_config.SHADOW_GLOBAL_GOAL,
                trigger_kind="auto",
                promotion_budget=promotion_budget,
                experiment_promotion_budget=experiment_promotion_budget,
                suppress_promotions_reason=suppress_promotions_reason or None,
            )
            if res.get("ok"):
                db.increment_ops_counter("shadow_global:auto_run_ok", 1)
                if int(res.get("json_retry_count") or 0) > 0:
                    db.increment_ops_counter("shadow_global:auto_run_json_retry_recovered", 1)
            else:
                db.increment_ops_counter("shadow_global:auto_run_fail", 1)
                err = _counter_suffix(res.get("error"))
                db.increment_ops_counter(f"shadow_global:auto_run_fail:{err}", 1)
                if res.get("raw_preview"):
                    logger.warning(
                        "shadow_global_auto_run_failed error=%s preview=%s",
                        err,
                        _clip_text(res.get("raw_preview"), 400),
                    )
                else:
                    logger.warning("shadow_global_auto_run_failed error=%s", err)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Global shadow loop tick failed")
            db.increment_ops_counter("shadow_global:auto_run_exception", 1)
        await asyncio.sleep(max(20, int(app_config.SHADOW_GLOBAL_TICK_INTERVAL_SEC)))
