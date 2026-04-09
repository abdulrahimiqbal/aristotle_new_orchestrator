from __future__ import annotations

from orchestrator.lima_presenter import build_lima_ui_context
from orchestrator.lima_steward import build_lima_steward_view, problem_ready_for_auto_continue


def test_steward_counts_only_actionable_human_packets_as_escalations() -> None:
    obligations = [
        {
            "id": "queued-human",
            "status": "queued_formal_review",
            "review_status": "not_reviewed",
            "title": "fresh formal review",
            "obligation_kind": "bridge_lemma",
            "estimated_formalization_value": 4.5,
            "estimated_execution_cost": 2.0,
            "prove_or_kill_md": "Needs review",
        },
        {
            "id": "archived-result",
            "status": "inconclusive",
            "review_status": "archived",
            "title": "old archived result",
            "obligation_kind": "bridge_lemma",
            "estimated_formalization_value": 5.0,
            "estimated_execution_cost": 2.0,
            "prove_or_kill_md": "Already decided",
        },
    ]

    steward = build_lima_steward_view(
        pending_handoffs=[],
        obligations=obligations,
        fractures=[],
        top_candidate={"title": "Promising world"},
        top_blocker={
            "title": "Dominant blocker",
            "body": "Still blocked",
            "tone": "risk",
            "label": "risk",
        },
    )

    assert steward["summary"]["escalated_count"] == 1
    assert steward["summary"]["blocked_count"] == 1
    assert [packet["title"] for packet in steward["packets"]] == [
        "fresh formal review",
        "Dominant blocker",
    ]


def test_problem_ready_for_auto_continue_requires_no_true_human_gate() -> None:
    clear_snapshot = {
        "problem": {"status": "active"},
        "handoffs": [],
        "obligations": [
            {"status": "approved_for_formal", "review_status": "approved"},
            {"status": "queued_local", "review_status": "not_reviewed"},
        ],
    }
    blocked_snapshot = {
        "problem": {"status": "active"},
        "handoffs": [],
        "obligations": [
            {"status": "queued_formal_review", "review_status": "not_reviewed"},
        ],
    }

    assert problem_ready_for_auto_continue(clear_snapshot) is True
    assert problem_ready_for_auto_continue(blocked_snapshot) is False


def test_presenter_uses_frontier_milestones_instead_of_row_counts() -> None:
    snapshot = {
        "problem": {"id": "prob1", "slug": "prob1", "title": "Problem 1", "status": "active"},
        "problems": [],
        "state": {},
        "latest_run": {"run_summary_md": "summary", "created_at": "2026-04-09T00:00:00"},
        "runs": [],
        "families": [],
        "universes": [
            {
                "id": "u1",
                "title": "Strong universe",
                "family_key": "fam",
                "universe_status": "formalized",
                "solved_world": "Solved here",
                "why_problem_is_easy_here": "It compresses well",
                "fit_score": 4.0,
                "compression_score": 4.0,
                "formalizability_score": 4.0,
                "falsifiability_score": 2.0,
            }
        ],
        "fractures": [],
        "obligations": [
            {"status": "verified_local", "review_status": "approved"},
            {"status": "verified_formal", "review_status": "approved"},
        ],
        "handoffs": [],
        "literature_sources": [],
        "literature_extracts": [],
        "literature_links": [],
        "formal_reviews": [],
        "artifacts": [],
        "policy_revisions": [],
        "policy_layers": [],
        "transfer_metrics": [],
    }

    context = build_lima_ui_context(snapshot)
    progress = context["lima_workspace_progress"]

    assert progress["label"] == "Frontier milestones cleared"
    assert progress["total"] == 7
    assert progress["resolved"] == 7
    assert "human queue clear yes" in progress["status_line"]
    assert "completion progress" in progress["caption"]
