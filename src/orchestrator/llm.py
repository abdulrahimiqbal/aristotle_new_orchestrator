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
    ExperimentStatus,
    ManagerDecision,
    NewExperiment,
    Target,
    TargetStatus,
    TargetUpdate,
)
from orchestrator.problem_map_util import (
    coerce_llm_problem_map,
    normalize_move_kind,
    normalize_node_kind,
    parse_problem_map,
    parse_problem_refs,
)
from orchestrator.research_packets import format_research_packet_markdown, parse_research_packet


def _strip_json_fence(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```\s*$", "", text)
    return text.strip()


# Serialize all LLM HTTP traffic + enforce min spacing (RPM / burst control).
_llm_http_lock = asyncio.Lock()
_llm_next_allowed_monotonic = 0.0


def _extract_message_text(message: Any) -> str:
    if isinstance(message, str):
        return message.strip()
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for part in content:
        if isinstance(part, str):
            text = part.strip()
            if text:
                parts.append(text)
            continue
        if not isinstance(part, dict):
            continue
        text = part.get("text")
        if isinstance(text, str) and text.strip():
            parts.append(text.strip())
            continue
        nested = part.get("content")
        if isinstance(nested, str) and nested.strip():
            parts.append(nested.strip())
    return "\n".join(parts).strip()


async def _post_chat_completions(payload: dict[str, Any], *, timeout: float = 120.0) -> dict[str, Any]:
    """POST /chat/completions with global throttle, per-wave 429 backoff, and extra waves.

    Each *wave* runs up to ``LLM_MAX_RETRIES_429 + 1`` POST attempts; on 429 we sleep
    (Retry-After or exponential backoff) and retry. If every attempt in a wave is 429,
    we sleep ``LLM_429_WAVE_GAP_SEC`` and start another wave, up to ``1 + LLM_EXTRA_429_WAVES`` waves.
    """
    global _llm_next_allowed_monotonic
    url = f"{app_config.LLM_BASE_URL}/chat/completions"
    headers = {"Authorization": f"Bearer {app_config.LLM_API_KEY}"}
    spacing = max(0.0, float(app_config.LLM_MIN_SECONDS_BETWEEN_REQUESTS))
    max_attempts = max(1, int(app_config.LLM_MAX_RETRIES_429) + 1)
    extra_waves = max(0, int(app_config.LLM_EXTRA_429_WAVES))
    total_waves = 1 + extra_waves
    wave_gap = max(0.0, float(app_config.LLM_429_WAVE_GAP_SEC))
    max_5xx_retries = max(0, int(app_config.LLM_MAX_RETRIES_5XX))
    base_5xx_sleep = max(0.5, float(app_config.LLM_5XX_BACKOFF_BASE_SEC))

    async with _llm_http_lock:
        now = time.monotonic()
        wait0 = _llm_next_allowed_monotonic - now
        if wait0 > 0:
            await asyncio.sleep(wait0)

        last_response: httpx.Response | None = None
        for wave in range(total_waves):
            if wave > 0 and wave_gap > 0:
                await asyncio.sleep(wave_gap)

            for attempt in range(max_attempts):
                transport_attempts = 1 + max_5xx_retries
                for transport_attempt in range(transport_attempts):
                    try:
                        async with httpx.AsyncClient(timeout=timeout) as client:
                            response = await client.post(url, headers=headers, json=payload)
                    except httpx.TransportError:
                        if transport_attempt >= max_5xx_retries:
                            raise
                        sleep_s = min(
                            30.0,
                            base_5xx_sleep * (2**transport_attempt) + random.uniform(0.0, 0.5),
                        )
                        await asyncio.sleep(sleep_s)
                        continue

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
                        break

                    if response.status_code in {500, 502, 503, 504} and transport_attempt < max_5xx_retries:
                        sleep_s = min(
                            30.0,
                            base_5xx_sleep * (2**transport_attempt) + random.uniform(0.0, 0.5),
                        )
                        await asyncio.sleep(sleep_s)
                        continue

                    response.raise_for_status()
                    data = response.json()
                    _llm_next_allowed_monotonic = time.monotonic() + spacing
                    return data
                else:
                    continue

        if last_response is not None:
            detail = (
                f" (HTTP {last_response.status_code}: exhausted {max_attempts} attempts "
                f"× {total_waves} wave(s), {wave_gap}s gap between waves; "
                "set LLM_EXTRA_429_WAVES / LLM_429_WAVE_GAP_SEC / LLM_MAX_RETRIES_429 or reduce LLM load)"
            )
            req = last_response.request
            raise httpx.HTTPStatusError(
                f"Client error: {last_response.status_code}{detail}",
                request=req,
                response=last_response,
            )
        raise httpx.HTTPError("LLM request failed after retries")


async def invoke_llm(
    system: str,
    user: str,
    *,
    model: str | None = None,
    temperature: float = 0.3,
    json_object: bool | None = None,
) -> str:
    use_json = app_config.LLM_JSON_MODE if json_object is None else json_object
    payload: dict[str, Any] = {
        "model": model or app_config.LLM_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temperature,
        "max_tokens": max(64, int(app_config.LLM_MAX_TOKENS)),
    }
    if use_json:
        payload["response_format"] = {"type": "json_object"}
    try:
        data = await _post_chat_completions(payload, timeout=120.0)
    except httpx.HTTPStatusError as exc:
        # Some OpenAI-compatible providers reject response_format=json_object even
        # though the rest of the chat completions contract works. Retry once
        # without response_format so provider swaps do not require a hard failure.
        detail = ""
        if exc.response is not None:
            try:
                detail = exc.response.text.lower()
            except Exception:
                detail = ""
        if (
            use_json
            and exc.response is not None
            and exc.response.status_code in (400, 404, 415, 422)
            and (
                "response_format" in detail
                or "json_object" in detail
                or "unsupported response format" in detail
            )
        ):
            payload.pop("response_format", None)
            data = await _post_chat_completions(payload, timeout=120.0)
        else:
            raise
    return _extract_message_text(data["choices"][0]["message"])


async def _call_llm(system: str, user: str) -> str:
    return await invoke_llm(system, user, temperature=0.3)


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
    lines.append(
        "## Mandate (how to think each tick)\n"
        "1) Treat the campaign prompt as the problem to solve — every target and map node serves it.\n"
        "2) Use the problem map as the strategic chart: summary, nodes, edges, active_fronts.\n"
        "3) Run discovery via verification: Aristotle is your experiment lab; Mathlib/LeanSearch hints are leads, not gospel.\n"
        "4) Close the loop: read new results → update targets if justified → pick the next verification that reduces "
        "uncertainty or proves a step toward the main goal → repeat."
    )
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
                    nk = normalize_node_kind(n.get("kind"))
                    lines.append(
                        f"- node id={n.get('id')} kind={nk} status={n.get('status')} "
                        f"label={str(n.get('label', ''))[:300]}"
                    )
        fronts = pmap.get("active_fronts") or []
        if isinstance(fronts, list) and fronts:
            lines.append(f"active_fronts: {', '.join(str(x) for x in fronts[:12])}")
    else:
        fronts = []
    packet = parse_research_packet(state.campaign.research_packet_json)
    packet_md = format_research_packet_markdown(packet, active_fronts=fronts)
    if packet_md:
        lines.append("")
        lines.append(packet_md)
    if (state.mathlib_broad_markdown or "").strip() or (
        state.mathlib_narrow_markdown or ""
    ).strip():
        lines.append("")
        lines.append(
            "## Mathlib / LeanSearch hints (verified library — use for lemma names and imports; "
            "confirm against this campaign's Lake workspace)"
        )
        combo = ""
        if state.mathlib_broad_markdown:
            combo += state.mathlib_broad_markdown.strip() + "\n\n"
        if state.mathlib_narrow_markdown:
            combo += state.mathlib_narrow_markdown.strip()
        cap = app_config.MATHLIB_CONTEXT_MAX_CHARS
        if len(combo) > cap:
            combo = combo[: cap - 1] + "…"
        lines.append(combo)
    n_inflight = sum(
        1
        for e in state.experiments
        if e.status in (ExperimentStatus.SUBMITTED, ExperimentStatus.RUNNING)
    )
    lines.append("")
    lines.append(
        f"## In-flight verification queue: {n_inflight} experiment(s) submitted or running "
        "(Aristotle jobs not yet recorded as completed/failed here). "
        "While this is >0, prefer new discovery moves and waiting for results over campaign_complete. "
        "Slowness is normal for hard goals; partial proofs still count as progress."
    )
    lines.append("")
    lines.append("## Targets")
    lines.append(
        "Use target ids exactly as written below. Do not invent new ids or append suffixes."
    )
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

    system = """You are an autonomous research manager driving a formal verification campaign toward **solving the stated problem** (the campaign prompt). Aristotle runs Lean 4 proof attempts; your job is to steer **map → discovery via verification → interpret → advance**, over and over, until targets reflect real progress or well-justified blockage.

## Operating loop (every tick)
1. **Orient:** Re-read the campaign prompt (what “solved” means). Skim the **problem map** (summary, nodes, edges, `active_fronts`). See which nodes are open/active vs proved and how edges say dependencies (`would_imply`, `special_case`, `equivalent`, `relates`).
2. **Assimilate evidence:** Use experiment summaries, verdicts, parsed lemmas/blockers, and the structured sections as ground truth from the last verification round.
3. **Update targets:** When evidence warrants, change target status (verified / refuted / blocked / open) with explicit citations to experiment ids or lemmas in `evidence`.
4. **Plan the next verification push:** Submit Aristotle experiments that **directly serve the map’s active fronts** or unblock the shortest path to the main goal. Prefer one sharp objective per experiment (clear statement, file/namespace hints if helpful, explicit bounds for finite checks).
5. **Choose tactics via move_kind** (pick the verb that matches intent — do not default everything to `prove`):
   - `prove` — direct attempt at a stated lemma or case toward the goal.
   - `explore` — scout: computations, small search, “what if”, or gathering structure without claiming the full theorem.
   - `promote` — lift or reuse lemmas from a partial proof; formalize a stepping-stone named in a prior summary.
   - `reformulate` — restate the goal (weaker/equivalent variant) to make verification tractable.
   - `center` — focus on the current bottleneck (one blocker, one missing definition, one hard subgoal).
   - `refute` — try to falsify a tempting strengthening or find a counterexample slice (sanity-check).
   - `underspecify` — shrink scope (fixed N, fixed parameter, intermediate statement) to get a verified foothold.
   - `perturb` — nearby variant of a failed or partial attempt (change hypotheses, constants, or statement shape).
   In `move_note`, tie the move to **map node id(s)** or **target id** when possible.

## Mindset: solving, not just logging
- The problem map is a **living plan toward a solution**, not decoration. Rotate or deepen `active_fronts` in your reasoning when the map refresh runs; align experiments with those fronts.
- **Progressive proof strategy:** definitions and formalizations → small/special cases → reusable lemmas → bridges to the flagship target. A single monolithic “prove everything” objective is usually worse than a chain of verifiable steps.
- **Discovery via verification:** failed runs still produce signal (blockers, partial lemmas, counterexamples). Turn that signal into the **next** experiment.
- **Use all tools:** targets, problem map, structured experiment memory, lemma ledger, `problem_refs`, and Mathlib/LeanSearch hints (as orientation — confirm in Lean output).

## Key principles (guardrails)
- Default stance: **keep the campaign running** — each finished experiment updates your picture; propose the next move that most reduces gap to “solved”.
- Don't repeat the same experiment; vary approach after failure or stalemate.
- A target is **verified** only with structured support: verdict=proved, or reconciliation to proved with clear lemma content in summary — not wishful reading of inconclusive runs.
- **finite_check** nodes / targets: state explicit bounds (e.g. n ≤ B, M ≤ …) in objectives.
- **blocked** targets: only after repeated, specific failures (e.g. many infra errors or a principled mathematical obstruction), not because the main conjecture is hard.
- Campaign **complete** only when every target is verified, refuted, or blocked.

## campaign_complete
- If the in-flight queue (submitted/running) is non-empty, set **campaign_complete false** unless every target is already resolved. Do not “give up” while verification is still in flight.
- Only set campaign_complete when stopping is justified: all targets resolved, or empty queue plus sustained lack of actionable progress with reasons stated.

move_kind must be one of: prove, underspecify, perturb, promote, reformulate, center, refute, explore.
Every new_experiments.target_id must exactly match one target id from the state. Do not add suffixes like _general or _extended.

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
      "move_note": "optional: map node id(s), target id, prior experiment id, tactic intent"
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


async def reason_skeptic(
    state: CampaignState, primary: ManagerDecision
) -> list[NewExperiment]:
    """Optional second pass: propose refute/explore experiments to stress-test the primary plan."""
    if not app_config.LLM_API_KEY or not primary.new_experiments:
        return []

    system = """You are a skeptical reviewer for a Lean 4 / Aristotle formal verification campaign.

The primary manager proposed verification moves toward solving the campaign problem. Your job is **adversarial discovery**: add stress-tests that could invalidate hidden assumptions or expose gaps before effort piles onto a wrong path.

- Propose at most 2 new experiments with move_kind "refute" or "explore".
- Target over-stated lemmas, missing hypotheses, or tempting false strengthenings; align with problem map nodes where possible.
- Do not duplicate an objective that is essentially the same as the primary list.
- Each objective must be a single coherent Aristotle-facing instruction.

Return JSON only:
{
  "new_experiments": [
    {
      "target_id": "...",
      "objective": "...",
      "move_kind": "refute|explore",
      "move_note": "skeptic: ..."
    }
  ]
}

If nothing useful to add, return {"new_experiments": []}."""

    primary_objs = json.dumps(
        [e.model_dump(mode="json") for e in primary.new_experiments],
        indent=2,
        ensure_ascii=False,
    )[:12000]
    pmap = parse_problem_map(state.campaign.problem_map_json)
    user = f"""## Primary reasoning (excerpt)
{primary.reasoning[:4000]}

## Primary new_experiments
{primary_objs}

## Problem map summary
{str(pmap.get("summary") or "")[:2500]}

## Open targets (id + description)
""" + "\n".join(
        f"- id={t.id} :: {t.description[:400]}"
        for t in state.targets
        if t.status.value == "open"
    )[:8000]

    try:
        raw = await _call_llm(system, user)
        data = json.loads(_strip_json_fence(raw))
        if not isinstance(data, dict):
            return []
        out: list[NewExperiment] = []
        for ne in data.get("new_experiments") or []:
            if not isinstance(ne, dict):
                continue
            tid = ne.get("target_id")
            obj = ne.get("objective")
            if not tid or not obj:
                continue
            mk = normalize_move_kind(str(ne.get("move_kind") or "explore"))
            if mk not in ("refute", "explore"):
                mk = "explore"
            note = str(ne.get("move_note") or "").strip()
            out.append(
                NewExperiment(
                    target_id=str(tid),
                    objective=str(obj).strip(),
                    move_kind=mk,
                    move_note=(note + " (skeptic_pass)")[:2000],
                )
            )
        return out[: app_config.SKEPTIC_PASS_MAX_EXPERIMENTS]
    except (httpx.HTTPError, json.JSONDecodeError, TypeError, ValueError):
        return []


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

The map is the **shared battle plan** for solving the campaign prompt: nodes are subproblems, edges show how they relate, and `active_fronts` tells the manager where to aim the next Aristotle experiments. The main theorem may be far off — still refine the map so each verification tick can **reduce uncertainty or prove a step** toward the goal.

Track the landscape: nodes (claims/subproblems), edges (implies / special_case / equivalent / relates), `active_fronts` (what to push on now), and a short summary of difficulty and strategy **plus what the next verification passes should try** (one or two sentences).

Each node carries a "kind" (semantic role — orthogonal to status):
- claim: standard lemma or subgoal toward the main result (default).
- hypothesis: explicit provisional assumption ("if we had X…"); not established.
- finite_check: decidable / bounded / computational slice (e.g. n ≤ B).
- literature_anchor: formalization target tied to problem_refs or given external sources.
- obstruction: known hard gate or bottleneck (still a formal target, but tag it as such).
- exploration: scouting / fuzzy node to be refined or split later.
- equivalence: alternate formulation logically key to the main question.

Discovery via verification: update node statuses from evidence (proved, refuted, blocked, open, active). **Align `active_fronts` with the highest-leverage verification steps** (e.g. missing definitions, small cases, lemmas that unlock edges toward the root claim). Use kinds so planners can prioritize finite_check and literature_anchor before unbounded obstructions.

Optional per-node "obligations": short strings (max 5) stating what would validate or falsify that node (e.g. "refute strengthened variant X", "decide for n≤10^6"). Use for hypothesis, obstruction, literature_anchor, finite_check especially.

literature_anchor nodes must align with problem_refs keys above; if refs are empty, do not invent anchors—use exploration or claim instead.

Return JSON only:
{
  "summary": "2–6 sentences",
  "nodes": [ {"id": "stable_id", "label": "short text", "status": "open|active|blocked|proved|refuted", "kind": "claim|hypothesis|finite_check|literature_anchor|obstruction|exploration|equivalence", "obligations": ["optional short string", "..."] } ],
  "edges": [ {"from": "node_id", "to": "node_id", "kind": "would_imply|special_case|equivalent|relates"} ],
  "active_fronts": ["node_id", ...]
}

Keep nodes concise (max ~20). Reuse stable ids when the same subproblem persists. Omit "kind" only when claim is appropriate. Do not cite sources that were not given in problem_refs."""

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
        "Summarize the following Aristotle / Lean verification output in 2-4 clear sentences "
        "for a research manager who will plan the next proof step. Focus on: verdict, main lemmas "
        "or definitions proved, blockers/unsolved goals, and counterexamples if any. "
        "If partial progress exists, name what could be promoted or explored next."
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
                "max_tokens": max(64, int(app_config.LLM_SUMMARIZE_MAX_TOKENS)),
            },
            timeout=60.0,
        )
        text = _extract_message_text(data["choices"][0]["message"])
        return text or _summarize_fallback(raw_output)
    except (httpx.HTTPError, KeyError, TypeError):
        return _summarize_fallback(raw_output)
