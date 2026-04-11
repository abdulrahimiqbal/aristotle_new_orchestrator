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
                f"Replay status: {'passed' if solved_report.replay_passed else 'failed'}",
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
                f"Strongest world: {(strongest_world or {}).get('world_name', 'None yet')}",
            ],
            "actions": ["Inspect blocker", "Promote alternative world", "Spawn kill cohort"],
            "class": status_view["banner_class"],
        }
    if status == "stalled":
        return {
            "kind": "stalled",
            "title": f"Stalled: no replayable formal gain in the last {status_view['stalled_iteration_window']} iterations.",
            "summary": [
                f"Stale iteration count: {status_view['stalled_iteration_window']}",
                f"Stalled since: {status_view['stalled_since'] or 'recently'}",
                f"Strongest world: {(strongest_world or {}).get('world_name', 'None yet')}",
                f"Last useful frontier gain: {status_view['last_gain_at'] or 'none recorded'}",
            ],
            "actions": ["Force world rotation", "Inspect fractures", "Revise program"],
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
        return card["status_view"]["blocker_summary"] or card["status_view"]["reason"]
    if status == "stalled":
        return card["status_view"]["reason"]
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


def build_index_context(db: LimaCoreDB) -> dict[str, Any]:
    cards = []
    for problem in db.list_problems():
        snapshot = db.snapshot(str(problem["id"]))
        worlds = snapshot["worlds"]
        fractures = snapshot["fractures"]
        jobs = snapshot["jobs"]
        solved = solved_checker(db, str(problem["id"]))
        status_view = get_problem_status_view(db, problem, events=snapshot["events"])
        card = {
            "problem": problem,
            "solved_report": solved,
            "status_view": status_view,
            "frontier_debt": proof_debt(snapshot["frontier"]),
            "strongest_world": worlds[0] if worlds else None,
            "top_blocker": fractures[0] if fractures else None,
            "active_jobs": sum(1 for job in jobs if str(job["status"]) in {"queued", "running"}),
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
    stats = {
        "proof_debt": proof_debt(frontier),
        "running_jobs": sum(1 for job in jobs if job["status"] == "running"),
        "queued_jobs": sum(1 for job in jobs if job["status"] == "queued"),
        "succeeded_jobs": sum(1 for job in jobs if job["status"] == "succeeded"),
        "failed_jobs": sum(1 for job in jobs if job["status"] == "failed"),
        "yielded_lemmas": sum(int(cohort["yielded_lemmas"]) for cohort in cohorts),
        "yielded_counterexamples": sum(int(cohort["yielded_counterexamples"]) for cohort in cohorts),
        "replayable_gain_rate": status_view["replayable_gain_rate"],
        "active_jobs": sum(1 for job in jobs if job["status"] in {"queued", "running"}),
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
