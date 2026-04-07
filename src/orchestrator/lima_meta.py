from __future__ import annotations

from typing import Any

from orchestrator import config as app_config
from orchestrator.lima_db import LimaDatabase


def analyze_and_update_policy(
    lima_db: LimaDatabase,
    *,
    problem_id: str,
    from_run_id: str | None = None,
) -> dict[str, Any]:
    """Bounded Meta-Lima strategy update.

    This writes reversible policy rows only; it never mutates code or changes
    Lima's mission.
    """

    families = lima_db.list_family_leaderboard(problem_id, limit=50)
    fractures = lima_db.list_fractures(problem_id, limit=50)
    obligations = lima_db.list_obligations(problem_id, limit=50)
    runs = lima_db.list_runs(problem_id, limit=20)

    formalizable_families = [
        f for f in families if int(f.get("formal_win_count") or 0) > 0
    ]
    repeated_failures: dict[str, int] = {}
    for fracture in fractures:
        kind = str(fracture.get("failure_type") or "unknown")
        repeated_failures[kind] = repeated_failures.get(kind, 0) + 1

    queued_obligations = [o for o in obligations if str(o.get("status")) == "queued"]
    policy_changes = {
        "mission_locked": True,
        "generation": {
            "prefer_family_keys": [
                str(f.get("family_key")) for f in formalizable_families[:5]
            ],
            "avoid_empty_repetitions": True,
            "max_universes_per_run": int(app_config.LIMA_MAX_UNIVERSES_PER_RUN),
        },
        "rupture": {
            "emphasize_failure_types": [
                kind
                for kind, _count in sorted(
                    repeated_failures.items(), key=lambda kv: kv[1], reverse=True
                )[:5]
            ],
            "require_vacuity_and_bridgeability": True,
        },
        "literature": {
            "prior_art_check_required": True,
            "route_cycle_and_completion_families_to_manual_literature": True,
        },
        "formal": {
            "escalate_only_after_human_approval": True,
            "queued_obligation_count": len(queued_obligations),
        },
        "scoring": {
            "falsifiability_weight": 1.25 if repeated_failures else 1.0,
            "bridgeability_weight": 1.2,
            "literature_novelty_weight": 1.1,
            "complexity_penalty": 1.0,
        },
    }
    benchmark = {
        "run_count": len(runs),
        "family_count": len(families),
        "fracture_count": len(fractures),
        "queued_obligation_count": len(queued_obligations),
        "failure_type_histogram": repeated_failures,
    }
    summary = (
        "Meta-Lima updated reversible strategy policy. "
        f"Observed {len(fractures)} fracture(s), {len(families)} family/families, "
        f"and {len(queued_obligations)} queued obligation(s). "
        "Core mission and zero-live-authority constraints remain locked."
    )
    meta_id = lima_db.create_meta_run(
        problem_id=problem_id,
        from_run_id=from_run_id,
        analysis_summary_md=summary,
        policy_changes=policy_changes,
        benchmark=benchmark,
    )
    revision_id = lima_db.create_policy_revision(
        problem_id=problem_id,
        generation_policy=policy_changes["generation"],
        rupture_policy=policy_changes["rupture"],
        literature_policy=policy_changes["literature"],
        formal_policy=policy_changes["formal"],
        scoring_weights=policy_changes["scoring"],
        change_reason_md=summary,
    )
    return {
        "meta_id": meta_id,
        "revision_id": revision_id,
        "analysis_summary_md": summary,
        "policy_changes": policy_changes,
        "benchmark": benchmark,
    }
