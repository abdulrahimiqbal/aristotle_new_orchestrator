from __future__ import annotations

import asyncio
import json
import random
import re
import time
from typing import Any

import httpx

from orchestrator import config as app_config
from orchestrator.models import (
    CampaignState,
    ManagerDecision,
    NewExperiment,
    Target,
    TargetStatus,
    TargetUpdate,
)
from orchestrator.problem_map_util import (
    coerce_llm_problem_map,
    normalize_move_kind,
    parse_problem_map,
    parse_problem_refs,
)


def _strip_json_fence(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```\s*$", "", text)
    return text.strip()


# Serialize all LLM HTTP traffic + enforce min spacing (RPM / burst control).
_llm_http_lock = asyncio.Lock()
_llm_next_allowed_monotonic = 0.0


async def _post_chat_completions(payload: dict[str, Any], *, timeout: float = 120.0) -> dict[str, Any]:
    """POST /chat/completions with global throttle and 429 retry/backoff."""
    global _llm_next_allowed_monotonic
    url = f"{app_config.LLM_BASE_URL}/chat/completions"
    headers = {"Authorization": f"Bearer {app_config.LLM_API_KEY}"}
    spacing = max(0.0, float(app_config.LLM_MIN_SECONDS_BETWEEN_REQUESTS))
    max_attempts = max(1, int(app_config.LLM_MAX_RETRIES_429) + 1)

    async with _llm_http_lock:
        now = time.monotonic()
        wait0 = _llm_next_allowed_monotonic - now
        if wait0 > 0:
            await asyncio.sleep(wait0)

        last_response: httpx.Response | None = None
        for attempt in range(max_attempts):
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, headers=headers, json=payload)
                last_response = response

                if response.status_code == 429:
                    ra = response.headers.get("Retry-After")
                    if ra is not None:
                        try:
                            sleep_s = float(ra)
                        except ValueError:
                            sleep_s = min(120.0, (2**attempt) + random.uniform(0.0, 1.0))
                    else:
                        sleep_s = min(120.0, (2**attempt) + random.uniform(0.0, 1.0))
                    sleep_s = max(1.0, min(180.0, sleep_s))
                    await asyncio.sleep(sleep_s)
                    continue

                response.raise_for_status()
                data = response.json()
                _llm_next_allowed_monotonic = time.monotonic() + spacing
                return data

        if last_response is not None:
            last_response.raise_for_status()
        raise httpx.HTTPError("LLM request failed after retries")


async def _call_llm(system: str, user: str) -> str:
    payload: dict[str, Any] = {
        "model": app_config.LLM_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.3,
    }
    if app_config.LLM_JSON_MODE:
        payload["response_format"] = {"type": "json_object"}
    data = await _post_chat_completions(payload, timeout=120.0)
    return str(data["choices"][0]["message"]["content"])


async def decompose_prompt(prompt: str) -> list[Target]:
    if not app_config.LLM_API_KEY:
        return [
            Target(
                id="",
                campaign_id="",
                description=prompt.strip() or "(empty prompt)",
            )
        ]

    system = """You are a mathematical research assistant that decomposes research problems into verification targets.

Given a research prompt, decompose it into concrete verification targets that can be investigated through formal theorem proving with Lean 4 via Aristotle.

Each target should be:
- A specific claim or sub-problem that Aristotle can attempt to verify
- Independent enough to investigate separately
- Concrete enough to write a Lean theorem statement for

Return JSON:
{
  "targets": [
    {"description": "...specific claim to verify..."}
  ]
}"""

    try:
        raw = await _call_llm(system, prompt)
        data = json.loads(_strip_json_fence(raw))
        items = data.get("targets", [])
        if not isinstance(items, list):
            items = []
        out: list[Target] = []
        for item in items:
            if isinstance(item, dict) and item.get("description"):
                desc = str(item["description"]).strip()
                if desc:
                    out.append(Target(id="", campaign_id="", description=desc))
        if out:
            return out
    except (httpx.HTTPError, json.JSONDecodeError, KeyError, TypeError):
        pass

    return [
        Target(
            id="",
            campaign_id="",
            description=prompt.strip() or "(empty prompt)",
        )
    ]


def _format_state_for_llm(state: CampaignState) -> str:
    lim_ev = app_config.LLM_EVIDENCE_TARGET_TAIL
    sum_chars = app_config.LLM_EXPERIMENT_SUMMARY_CHARS
    tick_chars = app_config.LLM_TICK_REASONING_CHARS

    lines: list[str] = []
    lines.append("## Campaign")
    lines.append(f"prompt: {state.campaign.prompt}")
    lines.append(f"status: {state.campaign.status.value}")
    lines.append(f"workspace_template: {state.campaign.workspace_template}")
    refs = parse_problem_refs(state.campaign.problem_refs_json)
    if refs:
        lines.append("problem_refs (external — do not invent citations beyond this):")
        for k, v in refs.items():
            if v:
                lines.append(f"  {k}: {v}")
    pmap = parse_problem_map(state.campaign.problem_map_json)
    if pmap:
        lines.append("")
        lines.append("## Problem map (structured landscape — align experiments with active_fronts)")
        lines.append(f"summary: {str(pmap.get('summary', ''))[:2000]}")
        nodes = pmap.get("nodes") or []
        if isinstance(nodes, list):
            for n in nodes[:24]:
                if isinstance(n, dict):
                    lines.append(
                        f"- node id={n.get('id')} status={n.get('status')} "
                        f"label={str(n.get('label', ''))[:300]}"
                    )
        fronts = pmap.get("active_fronts") or []
        if isinstance(fronts, list) and fronts:
            lines.append(f"active_fronts: {', '.join(str(x) for x in fronts[:12])}")
    lines.append("")
    lines.append("## Targets")
    for t in state.targets:
        lines.append(f"- id={t.id} status={t.status.value}")
        lines.append(f"  description: {t.description}")
        if t.evidence:
            lines.append("  evidence:")
            for ev in t.evidence[-lim_ev:]:
                lines.append(f"    - {ev}")
    lines.append("")
    lines.append("## Experiments (full list; summaries truncated)")
    for e in state.experiments:
        lines.append(
            f"- id={e.id} target={e.target_id} move_kind={e.move_kind} "
            f"status={e.status.value} verdict={(e.verdict.value if e.verdict else 'n/a')}"
        )
        lines.append(f"  objective: {e.objective}")
        if e.move_note:
            lines.append(f"  move_note: {e.move_note[:500]}")
        if e.result_summary:
            s = e.result_summary
            lines.append(
                f"  summary: {s[:sum_chars]}{'…' if len(s) > sum_chars else ''}"
            )
        if e.parsed_proved_lemmas or e.parsed_blockers:
            lines.append(
                f"  parsed: proved={len(e.parsed_proved_lemmas)} blockers={len(e.parsed_blockers)}"
            )
    lines.append("")
    lines.append("## Recent structured experiment results (retrieved)")
    for row in state.manager_context_experiments:
        lines.append(
            f"- exp={row.get('id')} target={row.get('target_id')} "
            f"move_kind={row.get('move_kind') or 'prove'} verdict={row.get('verdict')}"
        )
        for k in (
            "proved_lemmas",
            "generated_lemmas",
            "unsolved_goals",
            "blockers",
            "counterexamples",
        ):
            vals = row.get(k) or []
            if isinstance(vals, list) and vals:
                lines.append(f"  {k}: {vals[:20]}")
        err = row.get("error_message") or ""
        if err:
            lines.append(f"  error_message: {err[:500]}")
        ps = row.get("parse_source") or ""
        if ps:
            lines.append(f"  parse_source: {ps}")
    lines.append("")
    lines.append("## Structured results by target (recent per target)")
    for t in state.targets:
        rows = state.manager_context_experiments_by_target.get(t.id) or []
        if not rows:
            continue
        lines.append(f"- target={t.id} status={t.status.value}")
        lines.append(f"  description: {t.description[:500]}{'…' if len(t.description) > 500 else ''}")
        for row in rows:
            lines.append(
                f"  exp={row.get('id')} verdict={row.get('verdict')} "
                f"parse_source={row.get('parse_source') or 'n/a'}"
            )
            for k in (
                "proved_lemmas",
                "generated_lemmas",
                "unsolved_goals",
                "blockers",
                "counterexamples",
            ):
                vals = row.get(k) or []
                if isinstance(vals, list) and vals:
                    lines.append(f"    {k}: {vals[:20]}")
            err = row.get("error_message") or ""
            if err:
                lines.append(f"    error_message: {err[:500]}")
    lines.append("")
    lines.append("## Lemma / obligation ledger (recent)")
    for row in state.manager_context_ledger:
        lines.append(
            f"- {row.get('status')}: {str(row.get('label', ''))[:400]} "
            f"(exp={row.get('experiment_id')}, detail={row.get('detail', '')})"
        )
    lines.append("")
    lines.append("## Recent manager ticks (reasoning)")
    for tick in state.recent_ticks[-5:]:
        r = tick.reasoning
        lines.append(
            f"- tick {tick.tick_number}: {r[:tick_chars]}{'…' if len(r) > tick_chars else ''}"
        )
    return "\n".join(lines)


def _parse_manager_decision(data: dict[str, Any]) -> ManagerDecision:
    reasoning = str(data.get("reasoning", "") or "")
    campaign_complete = bool(data.get("campaign_complete", False))
    campaign_complete_reason = str(data.get("campaign_complete_reason", "") or "")

    target_updates: list[TargetUpdate] = []
    for u in data.get("target_updates") or []:
        if not isinstance(u, dict):
            continue
        tid = u.get("target_id")
        ns = u.get("new_status")
        if not tid or not ns:
            continue
        try:
            status = TargetStatus(str(ns).lower())
        except ValueError:
            continue
        target_updates.append(
            TargetUpdate(
                target_id=str(tid),
                new_status=status,
                evidence=str(u.get("evidence", "") or ""),
            )
        )

    new_experiments: list[NewExperiment] = []
    for ne in data.get("new_experiments") or []:
        if not isinstance(ne, dict):
            continue
        tid = ne.get("target_id")
        obj = ne.get("objective")
        if not tid or not obj:
            continue
        mk = normalize_move_kind(str(ne.get("move_kind") or "prove"))
        note = str(ne.get("move_note") or "").strip()
        new_experiments.append(
            NewExperiment(
                target_id=str(tid),
                objective=str(obj).strip(),
                move_kind=mk,
                move_note=note[:2000],
            )
        )

    return ManagerDecision(
        reasoning=reasoning,
        target_updates=target_updates,
        new_experiments=new_experiments,
        campaign_complete=campaign_complete,
        campaign_complete_reason=campaign_complete_reason,
    )


async def reason(state: CampaignState) -> ManagerDecision:
    if not app_config.LLM_API_KEY:
        return ManagerDecision(
            reasoning="LLM_API_KEY not set; skipping automated reasoning this tick.",
        )

    system = """You are an autonomous research manager running a formal verification campaign using Aristotle (Lean 4 prover).

You receive the full campaign state: targets, experiments, and their results. Based on the evidence accumulated so far, decide:

1. Whether any targets should be marked as verified/refuted/blocked based on experiment results
2. What new experiments to submit to Aristotle to make progress
3. Whether the campaign is complete

Key principles:
- Discovery via verification: even failed proofs reveal useful structure (lemmas, blockers, counterexamples)
- Don't repeat the same experiment. Vary the approach if something failed.
- If a proof partially succeeded, build on the lemmas it proved.
- If a counterexample was found, mark the target as refuted.
- A target is "verified" only when Aristotle returns verdict=proved.
- A target is "blocked" if 3+ experiments all fail with infra errors or the approach seems fundamentally stuck.
- The campaign is complete when all targets are verified, refuted, or blocked.

When setting campaign_complete: prefer not to give up while experiments are still submitted/running unless you have waited many ticks with no verdicts; if you do complete anyway, in-flight Aristotle jobs will be marked failed as infrastructure abandonment (consistent DB state).

Use the "Recent structured experiment results" and "Lemma / obligation ledger" sections as authoritative structured memory; do not ignore them in long campaigns.

Hard open problems are rarely solved in one shot: prefer discovery via verification — underspecify, perturb, promote lemmas from partial proofs, reformulate, center on obstructions, or refute subclaims. Name each move with move_kind.

move_kind must be one of: prove, underspecify, perturb, promote, reformulate, center, refute, explore.

Return JSON:
{
  "reasoning": "...your analysis of the current state...",
  "target_updates": [
    {"target_id": "...", "new_status": "verified|refuted|blocked|open", "evidence": "...why..."}
  ],
  "new_experiments": [
    {
      "target_id": "...",
      "objective": "...what to ask Aristotle to prove/explore...",
      "move_kind": "prove|underspecify|perturb|promote|reformulate|center|refute|explore",
      "move_note": "optional short rationale tying to problem map / prior experiment"
    }
  ],
  "campaign_complete": false,
  "campaign_complete_reason": ""
}"""

    user = _format_state_for_llm(state)

    try:
        raw = await _call_llm(system, user)
        data = json.loads(_strip_json_fence(raw))
        if not isinstance(data, dict):
            return ManagerDecision(reasoning="Invalid JSON from LLM.")
        return _parse_manager_decision(data)
    except httpx.HTTPError as e:
        return ManagerDecision(reasoning=f"LLM HTTP error: {e!s}")
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        return ManagerDecision(reasoning=f"LLM parse error: {e!s}")


async def update_problem_map(
    *,
    previous_map_json: str,
    problem_refs_json: str,
    campaign_prompt: str,
    delta_block: str,
    tick_number: int,
    targets_summary: str,
) -> str | None:
    """LLM refresh of problem_map_json. Returns serialized JSON or None on failure/skip."""
    if not app_config.LLM_API_KEY:
        return None

    prev = parse_problem_map(previous_map_json)
    refs = parse_problem_refs(problem_refs_json)
    refs_lines = "\n".join(f"- {k}: {v}" for k, v in refs.items() if v) or "(none)"

    system = """You maintain a structured "problem map" for a formal verification campaign (Lean 4 / Aristotle).

The main theorem may be out of reach; track the landscape: nodes (claims/subproblems), edges (implies / special_case / equivalent / relates), active_fronts (what to push on now), and a short summary of difficulty and strategy.

Discovery via verification: update node statuses from evidence (proved, refuted, blocked, open, active). Align active_fronts with where experiments should focus.

Return JSON only:
{
  "summary": "2–6 sentences",
  "nodes": [ {"id": "stable_id", "label": "short text", "status": "open|active|blocked|proved|refuted"} ],
  "edges": [ {"from": "node_id", "to": "node_id", "kind": "would_imply|special_case|equivalent|relates"} ],
  "active_fronts": ["node_id", ...]
}

Keep nodes concise (max ~20). Reuse stable ids when the same subproblem persists. Do not cite sources that were not given in problem_refs."""

    user = f"""## Campaign prompt
{campaign_prompt[:4000]}

## External problem_refs (authoritative)
{refs_lines}

## Targets (ids and descriptions)
{targets_summary[:8000]}

## Previous problem map (JSON)
{previous_map_json[:12000]}

## New evidence this refresh
{delta_block[:8000]}

## Tick
{tick_number}

Produce an updated full problem map JSON (all keys required). Set node statuses consistently with the evidence."""

    try:
        raw = await _call_llm(system, user)
        data = json.loads(_strip_json_fence(raw))
        if not isinstance(data, dict):
            return None
        coerced = coerce_llm_problem_map(
            data, previous=prev, tick_number=tick_number
        )
        return json.dumps(coerced, ensure_ascii=False)
    except (httpx.HTTPError, json.JSONDecodeError, TypeError, ValueError):
        return None


def _summarize_fallback(raw_output: str) -> str:
    return raw_output[:500] + ("…" if len(raw_output) > 500 else "")


async def summarize_result(raw_output: str, *, use_llm: bool = True) -> str:
    if not raw_output.strip():
        return ""

    cap = app_config.LLM_SUMMARIZE_INPUT_CHARS

    if not app_config.LLM_API_KEY or not use_llm:
        return _summarize_fallback(raw_output)

    system = (
        "Summarize the following Aristotle / Lean verification output in 2-3 clear sentences "
        "for a technical dashboard. Focus on verdict, main lemmas, and blockers."
    )
    user = raw_output[:cap]

    try:
        data = await _post_chat_completions(
            {
                "model": app_config.LLM_MODEL,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": 0.2,
            },
            timeout=60.0,
        )
        text = str(data["choices"][0]["message"]["content"]).strip()
        return text or _summarize_fallback(raw_output)
    except (httpx.HTTPError, KeyError, TypeError):
        return _summarize_fallback(raw_output)
