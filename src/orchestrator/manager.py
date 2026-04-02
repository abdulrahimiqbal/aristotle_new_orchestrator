from __future__ import annotations

import asyncio
import json
import logging
import re
import time

from orchestrator.aristotle import (
    parse_experiment_result,
    poll,
    submit,
    with_synthesized_json_if_needed,
)
from orchestrator.config import (
    LLM_SUMMARIZE_MAX_LLM_CALLS_PER_TICK,
    MAP_REFRESH_MAX_INTERVAL_TICKS,
    MAX_ACTIVE_EXPERIMENTS,
    MAX_EXPERIMENTS,
    SYNTHESIZE_STRUCTURED_JSON,
    TICK_INTERVAL,
)
from orchestrator import config as app_config
from orchestrator.db import Database
from orchestrator.llm import reason, summarize_result, update_problem_map
from orchestrator.mathlib_knowledge import (
    fetch_broad_reconnaissance,
    fetch_narrow_hints_for_state,
    format_library_anchors_markdown,
)
from orchestrator.models import AristotleParsedResult, ExperimentStatus
from orchestrator.problem_map_util import normalize_move_kind, parse_problem_map

logger = logging.getLogger("orchestrator.manager")

_FAILURE_BRACKET = re.compile(r"^\[([^\]]+)\]")


def _failure_class_from_message(msg: str) -> str:
    m = _FAILURE_BRACKET.match((msg or "").strip())
    if m:
        return m.group(1)
    low = (msg or "").lower()
    if "not set" in low:
        return "config_error"
    return "unknown"


def _ledger_rows_from_parsed(parsed: AristotleParsedResult) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for s in parsed.proved_lemmas:
        rows.append((s, "proved", ""))
    for s in parsed.generated_lemmas:
        rows.append((s, "attempted", "generated"))
    for s in parsed.unsolved_goals:
        rows.append((s, "attempted", "unsolved_goal"))
    for s in parsed.blockers:
        rows.append((s, "blocked", "blocker"))
    for s in parsed.counterexamples:
        rows.append((s, "blocked", "counterexample"))
    return rows


async def manager_loop(db: Database) -> None:
    """Main loop. Runs forever, ticking every TICK_INTERVAL seconds."""
    tick_count = 0
    while True:
        try:
            campaigns = db.get_active_campaigns()
            for campaign in campaigns:
                await tick(db, campaign, tick_count)
            tick_count += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Error in manager tick")
            db.increment_ops_counter("manager:global_tick_error", 1)
        await asyncio.sleep(TICK_INTERVAL)


async def tick(db: Database, campaign: dict, tick_number: int) -> None:
    """One tick of the manager loop for a single campaign."""
    campaign_id = campaign["id"]
    t0 = time.monotonic()
    exp_total = 0
    evidence_delta = False
    finished_snapshots: list[dict[str, str]] = []
    map_refreshed = False
    try:
        db.ensure_problem_map_initialized(campaign_id, campaign.get("prompt") or "")

        if app_config.MATHLIB_KNOWLEDGE_MODE == "leansearch":
            pmap_pre = parse_problem_map(db.get_campaign_problem_map_json(campaign_id))
            if not pmap_pre.get("library_recon_done"):
                tdescs = db.get_target_descriptions(campaign_id)
                if tdescs:
                    try:
                        anchors = await fetch_broad_reconnaissance(
                            campaign.get("prompt") or "",
                            tdescs,
                        )
                        pmap_pre["library_anchors"] = anchors
                        pmap_pre["library_recon_done"] = True
                        db.update_campaign_problem_map(
                            campaign_id,
                            json.dumps(pmap_pre, ensure_ascii=False),
                        )
                        db.increment_ops_counter("mathlib:broad_recon_ok", 1)
                    except Exception:
                        logger.exception(
                            "Mathlib broad reconnaissance failed for campaign %s",
                            campaign_id,
                        )
                        db.increment_ops_counter("mathlib:broad_recon_fail", 1)

        running = db.get_running_experiments(campaign_id)
        exp_total = len(running)
        summarize_llm_remaining = max(0, int(LLM_SUMMARIZE_MAX_LLM_CALLS_PER_TICK))
        for exp in running:
            if not exp["aristotle_job_id"]:
                continue
            status, bundle = await poll(
                exp["aristotle_job_id"],
                campaign["workspace_dir"],
                exp["submitted_at"] or "",
            )
            if status == "running":
                db.update_experiment_running(exp["id"])
            elif status == "completed" and bundle:
                if SYNTHESIZE_STRUCTURED_JSON:
                    bundle = with_synthesized_json_if_needed(bundle)
                md = bundle.markdown or ""
                js = bundle.structured_json_raw
                raw_for_db = md if md.strip() else (js or "")
                parsed = parse_experiment_result(md, js)
                use_llm_sum = summarize_llm_remaining > 0
                if use_llm_sum:
                    summarize_llm_remaining -= 1
                else:
                    db.increment_ops_counter("llm:summarize_truncated_cap", 1)
                summary = await summarize_result(raw_for_db, use_llm=use_llm_sum)
                db.update_experiment_completed(
                    exp["id"],
                    result_raw=raw_for_db,
                    result_summary=summary,
                    verdict=parsed.verdict.value,
                    parsed_proved_lemmas=parsed.proved_lemmas,
                    parsed_generated_lemmas=parsed.generated_lemmas,
                    parsed_unsolved_goals=parsed.unsolved_goals,
                    parsed_blockers=parsed.blockers,
                    parsed_counterexamples=parsed.counterexamples,
                    parsed_error_message=parsed.error_message or "",
                    result_structured_json=js or "",
                    parse_schema_version=parsed.parse_schema_version or 0,
                    parse_source=parsed.parse_source,
                    parse_warnings=parsed.parse_warnings,
                )
                db.increment_ops_counter(
                    f"parse:{parsed.parse_source}", 1
                )
                db.append_ledger_entries(
                    campaign_id,
                    str(exp["target_id"]),
                    str(exp["id"]),
                    _ledger_rows_from_parsed(parsed),
                )
                evidence = f"Experiment {exp['id']}: {parsed.verdict.value}"
                if parsed.proved_lemmas:
                    evidence += f" — proved {len(parsed.proved_lemmas)} lemma(s)"
                if parsed.blockers:
                    evidence += f" — {len(parsed.blockers)} blocker(s)"
                db.append_target_evidence(exp["target_id"], evidence)
                evidence_delta = True
                finished_snapshots.append(
                    {
                        "id": str(exp["id"]),
                        "target_id": str(exp["target_id"]),
                        "move_kind": str(exp.get("move_kind") or "prove"),
                        "outcome": "completed",
                        "verdict": parsed.verdict.value,
                        "summary_snip": (summary or "")[:500],
                    }
                )
            elif status == "failed":
                db.update_experiment_failed(
                    exp["id"], "Aristotle job failed or timed out"
                )
                db.append_target_evidence(
                    exp["target_id"], f"Experiment {exp['id']}: failed"
                )
                db.increment_ops_counter("aristotle:job_failed_or_timeout", 1)
                evidence_delta = True
                finished_snapshots.append(
                    {
                        "id": str(exp["id"]),
                        "target_id": str(exp["target_id"]),
                        "move_kind": str(exp.get("move_kind") or "prove"),
                        "outcome": "failed",
                        "verdict": "n/a",
                        "summary_snip": "Aristotle job failed or timed out",
                    }
                )

        state = db.get_campaign_state(campaign_id)

        total_experiments = len(state.experiments)
        active_count = sum(
            1
            for e in state.experiments
            if e.status in {ExperimentStatus.SUBMITTED, ExperimentStatus.RUNNING}
        )

        if total_experiments >= MAX_EXPERIMENTS:
            logger.info(
                "Campaign %s hit max experiments (%s)",
                campaign_id,
                MAX_EXPERIMENTS,
            )
            db.complete_campaign(campaign_id)
            db.record_tick(
                campaign_id,
                tick_number,
                reasoning=f"Stopped: reached MAX_EXPERIMENTS ({MAX_EXPERIMENTS}).",
                actions={"halt": "max_experiments"},
            )
            return

        pmap_prev = parse_problem_map(state.campaign.problem_map_json)
        last_map_tick = int(pmap_prev.get("last_tick_updated", -1))
        periodic_map = last_map_tick >= 0 and (tick_number - last_map_tick) >= MAP_REFRESH_MAX_INTERVAL_TICKS
        should_refresh_map = (evidence_delta or periodic_map) and bool(app_config.LLM_API_KEY)

        if should_refresh_map:
            delta_lines: list[str] = []
            for s in finished_snapshots:
                delta_lines.append(
                    f"- experiment {s['id']} target={s['target_id']} move_kind={s['move_kind']} "
                    f"outcome={s['outcome']} verdict={s['verdict']} :: {s['summary_snip']}"
                )
            if not delta_lines:
                delta_lines.append(
                    "(No new completed/failed experiments this interval; reassess the map from current knowledge.)"
                )
            targets_summary = "\n".join(
                f"- id={t.id} status={t.status.value}: {t.description[:600]}"
                for t in state.targets
            )
            new_map_json = await update_problem_map(
                previous_map_json=state.campaign.problem_map_json,
                problem_refs_json=state.campaign.problem_refs_json,
                campaign_prompt=state.campaign.prompt,
                delta_block="\n".join(delta_lines),
                tick_number=tick_number,
                targets_summary=targets_summary,
            )
            if new_map_json:
                db.update_campaign_problem_map(campaign_id, new_map_json)
                db.increment_ops_counter("problem_map:refresh_ok", 1)
                map_refreshed = True
                state = db.get_campaign_state(campaign_id)
            else:
                db.increment_ops_counter("problem_map:refresh_fail", 1)

        broad_md = format_library_anchors_markdown(
            parse_problem_map(state.campaign.problem_map_json).get("library_anchors") or []
        )
        narrow_md = ""
        if app_config.MATHLIB_KNOWLEDGE_MODE == "leansearch":
            try:
                narrow_md = await fetch_narrow_hints_for_state(
                    manager_context_experiments=state.manager_context_experiments,
                    manager_context_ledger=state.manager_context_ledger,
                )
                if narrow_md.strip():
                    db.increment_ops_counter("mathlib:narrow_hints_ok", 1)
            except Exception:
                logger.exception("Mathlib narrow hints failed for campaign %s", campaign_id)
                db.increment_ops_counter("mathlib:narrow_hints_fail", 1)

        state = state.model_copy(
            update={
                "mathlib_broad_markdown": broad_md,
                "mathlib_narrow_markdown": narrow_md,
            }
        )

        decision = await reason(state)

        if decision.reasoning.startswith("LLM HTTP error"):
            db.increment_ops_counter("llm:http_error", 1)
        elif decision.reasoning.startswith("LLM parse error"):
            db.increment_ops_counter("llm:parse_error", 1)

        for update in decision.target_updates:
            db.update_target(
                update.target_id,
                update.new_status.value,
                update.evidence,
            )

        done = decision.campaign_complete or db.all_targets_resolved(campaign_id)
        if not done:
            slots_available = MAX_ACTIVE_EXPERIMENTS - active_count
            valid_target_ids = {t.id for t in state.targets}
            for new_exp in decision.new_experiments[: max(0, slots_available)]:
                if new_exp.target_id not in valid_target_ids:
                    logger.warning(
                        "Skipping experiment: unknown target_id %s",
                        new_exp.target_id,
                    )
                    db.increment_ops_counter("manager:skip_unknown_target", 1)
                    continue
                mk = normalize_move_kind(new_exp.move_kind)
                exp_id = db.create_experiment(
                    campaign_id,
                    new_exp.target_id,
                    new_exp.objective,
                    move_kind=mk,
                    move_note=new_exp.move_note or "",
                )
                job_id, error = await submit(
                    new_exp.objective, campaign["workspace_dir"]
                )
                if job_id:
                    db.update_experiment_submitted(exp_id, job_id)
                else:
                    db.update_experiment_failed(exp_id, error)
                    fc = _failure_class_from_message(error)
                    db.increment_ops_counter(f"aristotle:submit:{fc}", 1)

        if decision.campaign_complete or db.all_targets_resolved(campaign_id):
            db.complete_campaign(campaign_id)

        db.record_tick(
            campaign_id,
            tick_number,
            reasoning=decision.reasoning,
            actions={
                "target_updates": [
                    u.model_dump(mode="json") for u in decision.target_updates
                ],
                "new_experiments": [
                    e.model_dump(mode="json") for e in decision.new_experiments
                ],
                "campaign_complete": decision.campaign_complete,
                "campaign_complete_reason": decision.campaign_complete_reason,
                "problem_map_refreshed": map_refreshed,
            },
        )
        db.clear_tick_diagnostic(campaign_id)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.exception("Tick failed for campaign %s", campaign_id)
        db.increment_ops_counter("manager:tick_exception", 1)
        db.set_tick_diagnostic(
            campaign_id,
            last_error_class=type(e).__name__,
            last_error_message=str(e)[:4000],
            last_tick_number=tick_number,
        )
    finally:
        dt_ms = int((time.monotonic() - t0) * 1000)
        logger.info(
            "manager_tick campaign_id=%s tick=%s duration_ms=%s running_experiments_polled=%s",
            campaign_id,
            tick_number,
            dt_ms,
            exp_total,
        )
