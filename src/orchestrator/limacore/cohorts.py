from __future__ import annotations

from .models import AristotleAgenda, CohortSummary


def build_aristotle_cohorts(agenda: AristotleAgenda) -> list[dict]:
    return [
        {
            "cohort_kind": "agenda_fanout",
            "title": agenda.title,
            "jobs": agenda.job_specs,
        }
    ]


def summarize_cohort(row: dict) -> CohortSummary:
    return CohortSummary(
        id=str(row["id"]),
        cohort_kind=str(row["cohort_kind"]),
        title=str(row["title"]),
        total_jobs=int(row["total_jobs"]),
        queued_jobs=int(row["queued_jobs"]),
        running_jobs=int(row["running_jobs"]),
        succeeded_jobs=int(row["succeeded_jobs"]),
        failed_jobs=int(row["failed_jobs"]),
        yielded_lemmas=int(row["yielded_lemmas"]),
        yielded_counterexamples=int(row["yielded_counterexamples"]),
        yielded_blockers=int(row["yielded_blockers"]),
        status=str(row["status"]),
    )
