from __future__ import annotations

import re
from typing import Any

from orchestrator.lima_db import LimaDatabase
from orchestrator.lima_rupture import collatz_step, residue_pattern_summary


def _parse_modulus(text: str, *, default: int = 16) -> int:
    match = re.search(r"\b(?:modulo|mod)\s+(\d{1,4})\b", text, flags=re.IGNORECASE)
    if not match:
        return default
    return max(2, min(1024, int(match.group(1))))


def _strict_descent_refuted(text: str, artifact: dict[str, Any]) -> bool:
    lowered = text.lower()
    requires_descent = "descent" in lowered and (
        "every" in lowered or "all" in lowered or "strict" in lowered
    )
    if not requires_descent:
        return False
    return any(
        int(count) == 0
        for residue, count in artifact.get("one_step_descent_counts", {}).items()
        if int(residue) % 2 == 1
    )


def _finite_check(obligation: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    text = " ".join(
        [
            str(obligation.get("title") or ""),
            str(obligation.get("statement_md") or ""),
            str(obligation.get("lean_goal") or ""),
        ]
    )
    modulus = _parse_modulus(text)
    limit = max(512, modulus * 128)
    artifact = residue_pattern_summary(limit=limit, modulus=modulus)
    odd_non_descent = [
        int(residue)
        for residue, count in artifact["one_step_descent_counts"].items()
        if int(residue) % 2 == 1 and int(count) == 0
    ]
    artifact["obligation_id"] = str(obligation.get("id") or "")
    artifact["odd_residue_classes_without_one_step_descent"] = odd_non_descent

    if _strict_descent_refuted(text, artifact):
        for n in range(1, limit + 1, 2):
            nxt = collatz_step(n)
            if nxt > n:
                artifact["smallest_odd_one_step_increase"] = {"n": n, "next": nxt}
                break
        return (
            "falsified",
            (
                f"Exact residue scan modulo {modulus} over n <= {limit} refuted a strict one-step descent reading. "
                f"Odd residue classes {odd_non_descent} have no one-step descent witnesses."
            ),
            artifact,
        )

    return (
        "checked",
        (
            f"Computed exact residue scan modulo {modulus} over n <= {limit}. "
            f"Odd residue classes {odd_non_descent} do not one-step descend; this is evidence for the obligation, not a proof of the quotient transfer."
        ),
        artifact,
    )


def run_queued_obligation_checks(
    lima_db: LimaDatabase,
    *,
    problem_id: str,
    limit: int = 20,
) -> dict[str, Any]:
    """Run bounded Lima-local checks without creating live Aristotle work."""

    obligations = lima_db.list_obligations(problem_id, status="queued", limit=limit)
    checked: list[str] = []
    falsified: list[str] = []
    skipped: list[str] = []
    for obligation in obligations:
        kind = str(obligation.get("obligation_kind") or "")
        if kind not in {"finite_check", "counterexample_search"}:
            skipped.append(str(obligation.get("id") or ""))
            continue

        status, summary, artifact = _finite_check(obligation)
        lima_db.create_artifact(
            problem_id=problem_id,
            universe_id=str(obligation.get("universe_id") or ""),
            artifact_kind="obligation_check",
            content={
                "obligation_id": obligation.get("id"),
                "obligation_kind": kind,
                "title": obligation.get("title"),
                "status": status,
                "summary": summary,
                "artifact": artifact,
                "zero_live_authority": True,
            },
        )
        lima_db.update_obligation_result(
            str(obligation["id"]),
            status=status,
            result_summary_md=summary,
            aristotle_ref={
                "executor": "lima_local_obligation_check",
                "live_aristotle_job_created": False,
                "artifact_kind": "obligation_check",
            },
        )
        (falsified if status == "falsified" else checked).append(str(obligation["id"]))

    return {
        "ok": True,
        "checked": checked,
        "falsified": falsified,
        "skipped": skipped,
        "queued_seen": len(obligations),
    }
