from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .cohorts import summarize_cohort
from .db import LimaCoreDB
from .frontier import proof_debt
from .runtime import detect_runtime_status
from .solved import solved_checker


STATUS_STYLES = {
    "booting": {"badge": "bg-sky-400/20 text-sky-200", "banner": "border-sky-400/30 bg-sky-400/10 text-sky-100"},
    "running": {"badge": "bg-algae/20 text-algae", "banner": "border-algae/20 bg-algae/10 text-algae"},
    "blocked": {"badge": "bg-rust/20 text-rust", "banner": "border-rust/40 bg-rust/10 text-rust"},
    "stalled": {"badge": "bg-brass/20 text-brass", "banner": "border-brass/40 bg-brass/10 text-brass"},
    "paused": {"badge": "bg-slate-500/20 text-slate-200", "banner": "border-slate-400/30 bg-slate-400/10 text-slate-100"},
    "solved": {"badge": "bg-algae/20 text-algae", "banner": "border-algae/40 bg-algae/10 text-algae"},
    "failed": {"badge": "bg-red-500/20 text-red-200", "banner": "border-red-400/40 bg-red-400/10 text-red-100"},
}


def get_problem_status_view(db: LimaCoreDB, problem: dict[str, Any], *, events: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    runtime = detect_runtime_status(db, str(problem["id"]))
    style = STATUS_STYLES[runtime.status]
    cta = {
        "booting": "Workspace is normalizing the theorem and seeding the first world line.",
        "running": "Autopilot is active.",
        "blocked": "Inspect blocker",
        "stalled": "Force world rotation",
        "paused": "Start autopilot",
        "solved": "Inspect solved proof graph",
        "failed": "Inspect failure",
    }[runtime.status]
    return {
        "status": runtime.status,
        "reason": runtime.reason,
        "badge_class": style["badge"],
        "banner_class": style["banner"],
        "cta_text": cta,
        "blocked_node_key": runtime.blocked_node_key,
        "blocker_kind": runtime.blocker_kind,
        "blocker_summary": runtime.blocker_summary,
        "exhausted_family_key": runtime.exhausted_family_key,
        "suggested_family_key": runtime.suggested_family_key,
        "replayable_gain_rate": runtime.replayable_gain_rate,
        "last_meaningful_change_at": runtime.last_meaningful_change_at or str(problem.get("updated_at") or ""),
        "stalled_iteration_window": runtime.stalled_iteration_window,
        "stalled_since": runtime.stalled_since,
        "last_gain_at": runtime.last_gain_at,
        "autopilot_enabled": bool(int(problem.get("autopilot_enabled", 1) or 0)),
    }


def get_workspace_alert_banner(
    problem: dict[str, Any],
    status_view: dict[str, Any],
    strongest_world: dict[str, Any] | None,
    solved_report,
) -> dict[str, Any] | None:
    status = status_view["status"]
    if status == "solved":
        return {
            "kind": "solved",
            "title": "Solved: target theorem closed and replay check passed.",
            "summary": [
                "Solved theorem node: target_theorem",
                f"Replay check: {'passed' if solved_report.replay_passed else 'failed'}",
                f"Closure: {'closed' if solved_report.dependency_closure_passed else 'open'}",
            ],
            "actions": ["Inspect solved proof graph"],
            "class": status_view["banner_class"],
        }
    if status == "blocked":
        return {
            "kind": "blocked",
            "title": "Blocked: current frontier cannot advance.",
            "summary": [
                f"Blocking node: {status_view['blocked_node_key'] or 'unknown'}",
                f"Blocker kind: {status_view['blocker_kind'] or 'unknown'}",
                f"Blocker summary: {status_view['blocker_summary'] or status_view['reason']}",
                f"Primary family exhausted: {status_view['exhausted_family_key'] or 'no'}",
                f"Suggested next family: {status_view['suggested_family_key'] or 'unknown'}",
                f"Strongest world: {(strongest_world or {}).get('world_name', 'None yet')}",
            ],
            "actions": ["Inspect blocker", "Rotate world", "Spawn alternative cohort"],
            "class": status_view["banner_class"],
        }
    if status == "stalled":
        return {
            "kind": "stalled",
            "title": f"Stalled: no replayable formal gain in the last {status_view['stalled_iteration_window']} iterations.",
            "summary": [
                f"No replayable gain in last {status_view['stalled_iteration_window']} iterations.",
                f"Stalled since: {status_view['stalled_since'] or 'recently'}",
                f"Last meaningful gain: {status_view['last_gain_at'] or 'none recorded'}",
                f"Strongest world: {(strongest_world or {}).get('world_name', 'None yet')}",
                f"Current family: {status_view['exhausted_family_key'] or 'unknown'}",
            ],
            "actions": ["Inspect fractures", "Force rotation", "Revise program"],
            "class": status_view["banner_class"],
        }
    if status == "failed":
        return {
            "kind": "failed",
            "title": "Failed: internal error or unrecoverable backend state.",
            "summary": [status_view["reason"]],
            "actions": ["Inspect failure"],
            "class": status_view["banner_class"],
        }
    return None


def get_problem_card_summary(card: dict[str, Any]) -> str:
    status = card["status_view"]["status"]
    if status == "solved":
        return "Solved: target theorem closed and replay check passed."
    if status == "blocked":
        family = card["status_view"].get("exhausted_family_key") or "unknown"
        return f"Blocked on {card['status_view']['blocked_node_key'] or 'unknown'}; family {family} exhausted."
    if status == "stalled":
        return f"Stalled after {card['status_view']['stalled_iteration_window']} iterations with zero replayable gain."
    return card["status_view"]["reason"]


def get_autopilot_state(problem: dict[str, Any], status_view: dict[str, Any]) -> dict[str, Any]:
    running = status_view["status"] in {"running", "booting", "blocked", "stalled"} and status_view["autopilot_enabled"]
    return {
        "enabled": status_view["autopilot_enabled"],
        "running": running,
        "label": "Autopilot on" if status_view["autopilot_enabled"] else "Autopilot paused",
        "state_text": status_view["reason"],
        "can_start": status_view["status"] in {"paused", "blocked", "stalled", "running", "booting"},
        "can_pause": status_view["autopilot_enabled"] and status_view["status"] not in {"solved", "failed"},
        "can_iterate": status_view["status"] != "failed",
    }


def _compute_cohort_summary(cohorts: list[dict]) -> dict[str, Any]:
    """Compute honest cohort/yield summary for UI display.

    Since Aristotle jobs are executed inline synchronously, there are never
    truly "running" or "queued" jobs in the traditional sense. This function
    provides an honest summary focused on recent throughput and yields.
    """
    if not cohorts:
        return {
            "latest_cohort": None,
            "latest_cohort_title": None,
            "latest_cohort_completed_at": None,
            "latest_cohort_yield_summary": "No cohorts yet",
            "recent_job_yield": 0,
            "recent_failed_rate": 0.0,
            "has_recent_activity": False,
            "total_cohorts": 0,
            "finished_cohorts": 0,
        }

    # Sort by update time to find latest
    sorted_cohorts = sorted(
        cohorts,
        key=lambda c: str(c.get("updated_at") or c.get("created_at") or ""),
        reverse=True
    )
    latest = sorted_cohorts[0]

    # Compute recent yield (last 3 cohorts)
    recent_cohorts = sorted_cohorts[:3]
    recent_yield = sum(int(c.get("yielded_lemmas", 0)) for c in recent_cohorts)
    recent_total = sum(int(c.get("total_jobs", 0)) for c in recent_cohorts)
    recent_failed = sum(int(c.get("failed_jobs", 0)) for c in recent_cohorts)
    recent_failed_rate = recent_failed / max(1, recent_total)

    # Determine if there's been recent activity (within last 5 minutes for UI purposes)
    has_recent_activity = True  # Simplified - any cohorts count as recent in this context

    # Build yield summary string
    if int(latest.get("yielded_lemmas", 0)) > 0:
        yield_summary = f"Yielded {latest['yielded_lemmas']} lemma(s), {latest['yielded_counterexamples']} counterexample(s)"
    elif int(latest.get("yielded_counterexamples", 0)) > 0:
        yield_summary = f"Yielded {latest['yielded_counterexamples']} counterexample(s)"
    elif int(latest.get("failed_jobs", 0)) >= int(latest.get("total_jobs", 0)):
        yield_summary = "All jobs failed - no yield"
    else:
        yield_summary = f"Completed {latest['succeeded_jobs']}/{latest['total_jobs']} jobs with no yield"

    return {
        "latest_cohort": latest,
        "latest_cohort_title": str(latest.get("title", "Unknown")),
        "latest_cohort_completed_at": str(latest.get("updated_at") or latest.get("created_at") or ""),
        "latest_cohort_yield_summary": yield_summary,
        "recent_job_yield": recent_yield,
        "recent_failed_rate": recent_failed_rate,
        "has_recent_activity": has_recent_activity,
        "total_cohorts": len(cohorts),
        "finished_cohorts": sum(1 for c in cohorts if str(c.get("status")) == "finished"),
    }


def build_index_context(db: LimaCoreDB) -> dict[str, Any]:
    cards = []
    for problem in db.list_problems():
        snapshot = db.snapshot(str(problem["id"]))
        worlds = snapshot["worlds"]
        fractures = snapshot["fractures"]
        jobs = snapshot["jobs"]
        cohorts = [asdict(summarize_cohort(row)) for row in snapshot["cohorts"]]
        solved = solved_checker(db, str(problem["id"]))
        status_view = get_problem_status_view(db, problem, events=snapshot["events"])
        cohort_summary = _compute_cohort_summary(cohorts)

        # Honest active jobs count - since jobs are inline, this is always 0 for live jobs
        # but we report historical throughput via cohort_summary
        active_jobs = sum(1 for job in jobs if str(job["status"]) in {"queued", "running"})

        card = {
            "problem": problem,
            "solved_report": solved,
            "status_view": status_view,
            "frontier_debt": proof_debt(snapshot["frontier"]),
            "strongest_world": worlds[0] if worlds else None,
            "top_blocker": fractures[0] if fractures else None,
            "active_jobs": active_jobs,
            "cohort_summary": cohort_summary,
            "replayable_gain_rate": status_view["replayable_gain_rate"],
            "last_meaningful_change_at": status_view["last_meaningful_change_at"],
        }
        card["summary_text"] = get_problem_card_summary(card)
        cards.append(card)
    return {"cards": cards}


def build_workspace_context(db: LimaCoreDB, problem_slug_or_id: str, *, flash: dict[str, Any] | None = None) -> dict[str, Any]:
    snapshot = db.snapshot(problem_slug_or_id)
    solved = solved_checker(db, str(snapshot["problem"]["id"]))
    frontier = snapshot["frontier"]
    worlds = snapshot["worlds"]
    fractures = snapshot["fractures"]
    events = snapshot["events"]
    cohorts = [asdict(summarize_cohort(row)) for row in snapshot["cohorts"]]
    jobs = snapshot["jobs"]
    open_nodes = [node for node in frontier if node["status"] == "open"]
    blocked_nodes = [node for node in frontier if node["status"] == "blocked"]
    proved_nodes = [node for node in frontier if node["status"] == "proved"]
    recent_accepted = [event for event in events if event["decision"] == "accepted"]
    recent_reverted = [event for event in events if event["decision"] == "reverted"]
    stale_cohorts = [row for row in cohorts if row["status"] == "finished" and row["yielded_lemmas"] == 0]
    strongest_world = worlds[0] if worlds else None
    current_delta = events[-1] if events else None
    status_view = get_problem_status_view(db, snapshot["problem"], events=events)
    autopilot_state = get_autopilot_state(snapshot["problem"], status_view)
    cohort_summary = _compute_cohort_summary(cohorts)

    # Honest job stats - since jobs are executed inline synchronously:
    # - running_jobs and queued_jobs will typically be 0 after iteration completes
    # - succeeded/failed show historical throughput
    # - cohort_summary shows recent yield context
    stats = {
        "proof_debt": proof_debt(frontier),
        # These are honest counts - typically 0 for synchronous inline execution
        "running_jobs": sum(1 for job in jobs if job["status"] == "running"),
        "queued_jobs": sum(1 for job in jobs if job["status"] == "queued"),
        # Historical throughput
        "succeeded_jobs": sum(1 for job in jobs if job["status"] == "succeeded"),
        "failed_jobs": sum(1 for job in jobs if job["status"] == "failed"),
        "yielded_lemmas": sum(int(cohort["yielded_lemmas"]) for cohort in cohorts),
        "yielded_counterexamples": sum(int(cohort["yielded_counterexamples"]) for cohort in cohorts),
        "replayable_gain_rate": status_view["replayable_gain_rate"],
        # Honest active count - will be 0 for inline execution
        "active_jobs": sum(1 for job in jobs if job["status"] in {"queued", "running"}),
        # Cohort summary for UI honesty about inline execution
        "cohort_summary": cohort_summary,
        "latest_cohort_title": cohort_summary.get("latest_cohort_title", "None"),
        "latest_cohort_completed_at": cohort_summary.get("latest_cohort_completed_at", ""),
        "latest_cohort_yield_summary": cohort_summary.get("latest_cohort_yield_summary", "No cohorts"),
        "has_recent_cohort_activity": cohort_summary.get("has_recent_activity", False),
    }
    return {
        "problem": snapshot["problem"],
        "frontier": frontier,
        "worlds": worlds,
        "fractures": fractures,
        "events": events,
        "cohorts": cohorts,
        "jobs": jobs,
        "open_nodes": open_nodes,
        "blocked_nodes": blocked_nodes,
        "proved_nodes": proved_nodes,
        "solved_report": solved,
        "strongest_world": strongest_world,
        "current_delta": current_delta,
        "recent_accepted": recent_accepted[-20:],
        "recent_reverted": recent_reverted[-20:],
        "program": snapshot["program"],
        "stats": stats,
        "stale_cohorts": stale_cohorts,
        "flash": flash or {},
        "status_view": status_view,
        "alert_banner": get_workspace_alert_banner(snapshot["problem"], status_view, strongest_world, solved),
        "autopilot_state": autopilot_state,
    }
