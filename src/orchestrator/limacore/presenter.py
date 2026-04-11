from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .cohorts import summarize_cohort
from .db import LimaCoreDB
from .frontier import proof_debt
from .solved import solved_checker


def build_index_context(db: LimaCoreDB) -> dict[str, Any]:
    cards = []
    for problem in db.list_problems():
        snapshot = db.snapshot(str(problem["id"]))
        worlds = snapshot["worlds"]
        fractures = snapshot["fractures"]
        jobs = snapshot["jobs"]
        solved = solved_checker(db, str(problem["id"]))
        replayable_gain_rate = sum((event.get("score_delta") or {}).get("replayable_gain", 0) for event in snapshot["events"])
        cards.append(
            {
                "problem": problem,
                "solved_report": solved,
                "frontier_debt": proof_debt(snapshot["frontier"]),
                "strongest_world": worlds[0] if worlds else None,
                "top_blocker": fractures[0] if fractures else None,
                "active_jobs": sum(1 for job in jobs if str(job["status"]) in {"queued", "running"}),
                "replayable_gain_rate": replayable_gain_rate,
            }
        )
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
    stats = {
        "proof_debt": proof_debt(frontier),
        "running_jobs": sum(1 for job in jobs if job["status"] == "running"),
        "queued_jobs": sum(1 for job in jobs if job["status"] == "queued"),
        "succeeded_jobs": sum(1 for job in jobs if job["status"] == "succeeded"),
        "failed_jobs": sum(1 for job in jobs if job["status"] == "failed"),
        "yielded_lemmas": sum(1 for cohort in cohorts for _ in [cohort] if cohort["yielded_lemmas"]),
        "yielded_counterexamples": sum(int(cohort["yielded_counterexamples"]) for cohort in cohorts),
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
    }
