"""Shadow lab: speculative research agent (writes only shadow_* tables)."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from orchestrator import config as app_config
from orchestrator.db import Database
from orchestrator.llm import invoke_llm
from orchestrator.models import ExperimentStatus

logger = logging.getLogger("orchestrator.shadow")

_STRIP_JSON_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)


def _strip_json_fence(text: str) -> str:
    t = text.strip()
    t = _STRIP_JSON_FENCE.sub("", t).strip()
    return t


def _safe_json_loads(raw: str) -> dict[str, Any]:
    try:
        v = json.loads(_strip_json_fence(raw))
        return v if isinstance(v, dict) else {}
    except json.JSONDecodeError:
        return {}


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
    { "kind": "new_target", "description": "concrete verification target text for Aristotle" },
    { "kind": "new_experiment", "target_id": "must match an existing target id", "objective": "...", "move_kind": "explore|prove|refute|...", "move_note": "shadow:..." }
  ],
  "run_summary": "one paragraph for the run log"
}

Rules:
- hypotheses: 3–12 items; promotion_requests: 0–8 items.
- Never claim the main conjecture is proved unless you are restating a verified fact from the context (prefer not to).
- new_experiment.target_id MUST be one of the ids listed in the user message.
- Keep JSON valid. No markdown fences."""


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
        raw = await invoke_llm(
            SHADOW_SYSTEM,
            user,
            model=model,
            temperature=temp,
            json_object=True,
        )
    except Exception:
        logger.exception("Shadow LLM call failed")
        return {"ok": False, "error": "llm_request_failed"}

    data = _safe_json_loads(raw)
    if not data:
        return {"ok": False, "error": "invalid_json", "raw_preview": raw[:2000]}

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
    }
