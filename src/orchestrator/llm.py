from __future__ import annotations

import json
import re
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


def _strip_json_fence(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```\s*$", "", text)
    return text.strip()


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
    async with httpx.AsyncClient(timeout=120) as client:
        response = await client.post(
            f"{app_config.LLM_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {app_config.LLM_API_KEY}"},
            json=payload,
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]


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
            f"- id={e.id} target={e.target_id} status={e.status.value} "
            f"verdict={(e.verdict.value if e.verdict else 'n/a')}"
        )
        lines.append(f"  objective: {e.objective}")
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
            f"- exp={row.get('id')} target={row.get('target_id')} verdict={row.get('verdict')}"
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
        new_experiments.append(
            NewExperiment(target_id=str(tid), objective=str(obj).strip())
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

Return JSON:
{
  "reasoning": "...your analysis of the current state...",
  "target_updates": [
    {"target_id": "...", "new_status": "verified|refuted|blocked|open", "evidence": "...why..."}
  ],
  "new_experiments": [
    {"target_id": "...", "objective": "...what to ask Aristotle to prove/explore..."}
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


async def summarize_result(raw_output: str) -> str:
    if not raw_output.strip():
        return ""

    cap = app_config.LLM_SUMMARIZE_INPUT_CHARS

    if not app_config.LLM_API_KEY:
        return raw_output[:500] + ("…" if len(raw_output) > 500 else "")

    system = (
        "Summarize the following Aristotle / Lean verification output in 2-3 clear sentences "
        "for a technical dashboard. Focus on verdict, main lemmas, and blockers."
    )
    user = raw_output[:cap]

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{app_config.LLM_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {app_config.LLM_API_KEY}"},
                json={
                    "model": app_config.LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    "temperature": 0.2,
                },
            )
            response.raise_for_status()
            text = response.json()["choices"][0]["message"]["content"].strip()
            return text or raw_output[:500]
    except (httpx.HTTPError, KeyError, TypeError):
        return raw_output[:500] + ("…" if len(raw_output) > 500 else "")
