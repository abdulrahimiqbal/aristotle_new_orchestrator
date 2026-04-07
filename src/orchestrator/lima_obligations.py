from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Protocol

from orchestrator import config as app_config
from orchestrator.lima_db import LimaDatabase
from orchestrator.lima_models import LimaObligationSpec, LimaUniverseSpec, safe_json_loads, slugify
from orchestrator.lima_rupture import collatz_step, residue_pattern_summary


LOCAL_CHECK_KINDS = {"finite_check", "counterexample_search", "invariant_check", "consistency"}
FORMAL_REVIEW_KINDS = {"lean_goal", "bridge_lemma", "equivalence"}


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


def canonical_obligation_key(obligation: LimaObligationSpec | dict[str, Any]) -> str:
    if isinstance(obligation, LimaObligationSpec):
        parts = [
            obligation.obligation_kind,
            obligation.title,
            obligation.statement_md,
            obligation.lean_goal,
        ]
    else:
        parts = [
            obligation.get("obligation_kind"),
            obligation.get("title"),
            obligation.get("statement_md"),
            obligation.get("lean_goal"),
        ]
    return slugify(" ".join(str(part or "") for part in parts), fallback="obligation")


def _obligation_scores(kind: str, priority: int) -> tuple[float, float]:
    value_base = {
        "bridge_lemma": 4.5,
        "equivalence": 4.25,
        "lean_goal": 4.0,
        "invariant_check": 3.5,
        "finite_check": 3.0,
        "counterexample_search": 3.0,
        "consistency": 2.75,
        "literature_crosscheck": 2.5,
    }.get(kind, 3.0)
    cost_base = {
        "finite_check": 1.0,
        "counterexample_search": 1.25,
        "literature_crosscheck": 1.5,
        "consistency": 2.0,
        "invariant_check": 2.25,
        "bridge_lemma": 3.5,
        "equivalence": 4.0,
        "lean_goal": 4.0,
    }.get(kind, 2.5)
    boost = max(0, min(5, priority)) * 0.1
    return (min(5.0, value_base + boost), min(5.0, cost_base))


def compile_obligations_for_universe(
    universe: LimaUniverseSpec,
    rupture_report: dict[str, Any] | None = None,
) -> list[LimaObligationSpec]:
    """Compile narrow checks from explicit targets plus high-value claims."""

    out: list[LimaObligationSpec] = []
    seen: set[str] = set()

    def add(obligation: LimaObligationSpec) -> None:
        key = obligation.canonical_key or canonical_obligation_key(obligation)
        if key in seen:
            return
        seen.add(key)
        kind = obligation.obligation_kind
        status = obligation.status
        if status in {"queued", ""}:
            status = "queued_local" if kind in LOCAL_CHECK_KINDS else "queued_formal_review"
        value, cost = _obligation_scores(kind, obligation.priority)
        out.append(
            obligation.model_copy(
                update={
                    "canonical_key": key,
                    "status": status,
                    "estimated_formalization_value": obligation.estimated_formalization_value or value,
                    "estimated_execution_cost": obligation.estimated_execution_cost or cost,
                }
            )
        )

    for target in universe.formalization_targets:
        add(
            target.model_copy(
                update={
                    "why_exists_md": target.why_exists_md
                    or f"Lima emitted this formalization target from universe '{universe.title}'.",
                    "prove_or_kill_md": target.prove_or_kill_md
                    or "A proof supports the universe bridge; a counterexample or formal block fractures it.",
                }
            )
        )

    for claim in sorted(universe.bridge_lemmas, key=lambda c: c.priority, reverse=True)[:3]:
        add(
            LimaObligationSpec(
                obligation_kind="bridge_lemma",
                title=claim.title or f"Bridge lemma for {universe.title}",
                statement_md=claim.statement_md,
                lean_goal=claim.formal_statement,
                status="queued_formal_review",
                priority=max(3, claim.priority),
                why_exists_md="Bridge lemmas are the shortest path from a solved-world story back to the original problem.",
                prove_or_kill_md="Proving it validates the translation; refuting it blocks escalation.",
            )
        )

    if universe.conditional_theorem:
        claim = universe.conditional_theorem
        add(
            LimaObligationSpec(
                obligation_kind="equivalence",
                title=claim.title or f"Conditional theorem for {universe.title}",
                statement_md=claim.statement_md,
                lean_goal=claim.formal_statement,
                status="queued_formal_review",
                priority=max(3, claim.priority),
                why_exists_md="Conditional theorems express what the universe would actually buy if its bridge assumptions hold.",
                prove_or_kill_md="A failed equivalence kills the promised reduction.",
            )
        )

    for kill_test in sorted(universe.kill_tests, key=lambda c: c.priority, reverse=True)[:2]:
        add(
            LimaObligationSpec(
                obligation_kind="counterexample_search",
                title=kill_test.title or f"Kill test for {universe.title}",
                statement_md=kill_test.statement_md,
                status="queued_local",
                priority=max(2, kill_test.priority),
                why_exists_md="Kill tests are cheap falsification pressure before any formal escalation.",
                prove_or_kill_md="A bounded counterexample refutes the universe early; no hit means only that the bounded scan did not kill it.",
            )
        )

    for fracture in (rupture_report or {}).get("fractures") or []:
        failure_type = str(fracture.get("failure_type") or "")
        if failure_type == "prior_art":
            add(
                LimaObligationSpec(
                    obligation_kind="literature_crosscheck",
                    title=f"Novelty crosscheck for {universe.title}",
                    statement_md=str(fracture.get("breakpoint_md") or "Check whether this universe is prior art."),
                    status="queued_formal_review",
                    priority=4,
                    why_exists_md="Rupture found prior-art pressure; novelty must be reviewed before promotion.",
                    prove_or_kill_md="If this is already known, Lima should mark the universe as prior art rather than novelty.",
                )
            )

    return out


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
            "refuted_local",
            (
                f"Exact residue scan modulo {modulus} over n <= {limit} refuted a strict one-step descent reading. "
                f"Odd residue classes {odd_non_descent} have no one-step descent witnesses."
            ),
            artifact,
        )

    return (
        "verified_local",
        (
            f"Computed exact residue scan modulo {modulus} over n <= {limit}. "
            f"Odd residue classes {odd_non_descent} do not one-step descend; this is evidence for the obligation, not a proof of the quotient transfer."
        ),
        artifact,
    )


@dataclass(frozen=True)
class FormalReviewPacket:
    obligation_id: str
    backend_kind: str
    payload: dict[str, Any]


class FormalBackend(Protocol):
    backend_kind: str

    def build_packet(self, obligation: dict[str, Any]) -> FormalReviewPacket:
        ...

    def submit_approved(self, packet: FormalReviewPacket) -> dict[str, Any]:
        ...


class LocalStubFormalBackend:
    backend_kind = "local_stub"

    def build_packet(self, obligation: dict[str, Any]) -> FormalReviewPacket:
        lineage = safe_json_loads(obligation.get("lineage_json"), {})
        payload = {
            "source": "lima",
            "obligation_id": obligation.get("id"),
            "problem_id": obligation.get("problem_id"),
            "universe_id": obligation.get("universe_id"),
            "family_id": obligation.get("family_id"),
            "claim_id": obligation.get("claim_id"),
            "obligation_kind": obligation.get("obligation_kind"),
            "title": obligation.get("title"),
            "statement_md": obligation.get("statement_md"),
            "lean_goal": obligation.get("lean_goal"),
            "why_exists_md": obligation.get("why_exists_md"),
            "prove_or_kill_md": obligation.get("prove_or_kill_md"),
            "estimated_formalization_value": obligation.get("estimated_formalization_value"),
            "estimated_execution_cost": obligation.get("estimated_execution_cost"),
            "lineage": lineage,
            "zero_live_authority": True,
        }
        return FormalReviewPacket(
            obligation_id=str(obligation.get("id") or ""),
            backend_kind=self.backend_kind,
            payload=payload,
        )

    def submit_approved(self, packet: FormalReviewPacket) -> dict[str, Any]:
        return {
            "backend": self.backend_kind,
            "status": "queued_stub_only",
            "submitted_formal": False,
            "live_aristotle_job_created": False,
            "message": "Local stub recorded approval; no remote Lean/Aristotle work was submitted.",
            "packet": packet.payload,
        }


def make_formal_backend(kind: str | None = None) -> FormalBackend:
    selected = (kind or app_config.LIMA_FORMAL_BACKEND or "local_stub").strip().lower()
    # Future Aristotle/Lean/Mathlib adapters must preserve the same approval boundary.
    return LocalStubFormalBackend()


def queue_formal_review(
    lima_db: LimaDatabase,
    *,
    obligation_id: str,
    backend: FormalBackend | None = None,
) -> dict[str, Any]:
    obligation = lima_db.get_obligation(obligation_id)
    if not obligation:
        return {"ok": False, "error": "unknown_obligation"}
    backend = backend or make_formal_backend(str(obligation.get("formal_backend") or ""))
    packet = backend.build_packet(obligation)
    problem_id = str(obligation["problem_id"])
    universe_id = str(obligation.get("universe_id") or "")
    claim_id = str(obligation.get("claim_id") or "")
    lineage = safe_json_loads(obligation.get("lineage_json"), {})
    if isinstance(lineage, dict):
        source_run_id = str(lineage.get("source_run_id") or "")
        rupture_summary = str(lineage.get("rupture_summary") or "")
        claim_ids = [
            str(c)
            for c in (
                lineage.get("claim_ids")
                if isinstance(lineage.get("claim_ids"), list)
                else [lineage.get("source_claim_id") or claim_id]
            )
            if c
        ]
    else:
        lineage = {}
        source_run_id = ""
        rupture_summary = ""
        claim_ids = [claim_id] if claim_id else []
    links = [
        link
        for link in lima_db.list_universe_literature_links(problem_id, limit=50)
        if not universe_id or str(link.get("universe_id") or "") == universe_id
    ][:8]
    policy_revisions = lima_db.list_policy_revisions(problem_id, limit=1)
    policy_revision_id = str(policy_revisions[0].get("id") or "") if policy_revisions else ""
    lineage_payload = {
        **lineage,
        "source_problem_id": problem_id,
        "source_universe_id": universe_id,
        "source_family_id": str(obligation.get("family_id") or ""),
        "source_claim_id": claim_id,
        "source_run_id": source_run_id,
        "zero_live_authority": True,
    }
    review_id = lima_db.create_formal_review_item(
        problem_id=problem_id,
        obligation_id=obligation_id,
        universe_id=universe_id,
        claim_id=claim_id,
        family_id=str(obligation.get("family_id") or ""),
        claim_ids=claim_ids,
        rupture_summary_md=rupture_summary,
        literature_links=links,
        policy_revision_id=policy_revision_id,
        lineage=lineage_payload,
        backend_kind=packet.backend_kind,
        packet=packet.payload,
    )
    lima_db.set_obligation_status(
        obligation_id,
        "queued_formal_review",
        review_status="pending",
        formal_backend=packet.backend_kind,
        formal_payload=packet.payload,
        result_summary_md="Queued for formal review. No live Aristotle job was created.",
    )
    return {
        "ok": True,
        "review_id": review_id,
        "obligation_id": obligation_id,
        "backend": packet.backend_kind,
    }


def approve_formal_review(
    lima_db: LimaDatabase,
    *,
    obligation_id: str,
    backend: FormalBackend | None = None,
) -> dict[str, Any]:
    queued = queue_formal_review(lima_db, obligation_id=obligation_id, backend=backend)
    if not queued.get("ok"):
        return queued
    obligation = lima_db.get_obligation(obligation_id)
    if not obligation:
        return {"ok": False, "error": "unknown_obligation"}
    backend = backend or make_formal_backend(str(obligation.get("formal_backend") or ""))
    packet = backend.build_packet(obligation)
    backend_result = backend.submit_approved(packet)
    status = "submitted_formal" if backend_result.get("submitted_formal") else "approved_for_formal"
    lima_db.set_obligation_status(
        obligation_id,
        status,
        review_status="approved",
        formal_backend=packet.backend_kind,
        formal_payload=packet.payload,
        result_summary_md=str(backend_result.get("message") or "Approved for formal review."),
    )
    lima_db.update_formal_review_item(
        str(queued["review_id"]),
        status=status,
        review_decision="approved",
        backend_result=backend_result,
    )
    lima_db.create_artifact(
        problem_id=str(obligation["problem_id"]),
        universe_id=str(obligation.get("universe_id") or ""),
        artifact_kind="formal_review_packet",
        content=backend_result,
    )
    return {
        "ok": True,
        "review_id": queued["review_id"],
        "obligation_id": obligation_id,
        "status": status,
        "backend_result": backend_result,
    }


def reject_formal_review(lima_db: LimaDatabase, *, obligation_id: str) -> dict[str, Any]:
    obligation = lima_db.get_obligation(obligation_id)
    if not obligation:
        return {"ok": False, "error": "unknown_obligation"}
    lima_db.set_obligation_status(
        obligation_id,
        "inconclusive",
        review_status="rejected",
        result_summary_md="Formal review rejected by operator. Lima should keep the fracture and avoid escalation.",
    )
    for review in lima_db.list_formal_reviews(str(obligation["problem_id"]), limit=100):
        if str(review.get("obligation_id") or "") == obligation_id:
            lima_db.update_formal_review_item(
                str(review["id"]),
                status="inconclusive",
                review_decision="rejected",
                backend_result={
                    "status": "rejected",
                    "submitted_formal": False,
                    "live_aristotle_job_created": False,
                },
            )
            break
    return {"ok": True, "obligation_id": obligation_id, "status": "inconclusive"}


def archive_obligation(lima_db: LimaDatabase, *, obligation_id: str) -> dict[str, Any]:
    obligation = lima_db.get_obligation(obligation_id)
    if not obligation:
        return {"ok": False, "error": "unknown_obligation"}
    lima_db.set_obligation_status(
        obligation_id,
        "archived",
        review_status="archived",
        result_summary_md="Archived by operator. Lima retains lineage but will not route this obligation further.",
    )
    for review in lima_db.list_formal_reviews(str(obligation["problem_id"]), limit=100):
        if str(review.get("obligation_id") or "") == obligation_id:
            lima_db.update_formal_review_item(
                str(review["id"]),
                status="archived",
                review_decision="archived",
                backend_result={
                    "status": "archived",
                    "submitted_formal": False,
                    "live_aristotle_job_created": False,
                },
            )
            break
    return {"ok": True, "obligation_id": obligation_id, "status": "archived"}


def rerun_local_obligation(lima_db: LimaDatabase, *, obligation_id: str) -> dict[str, Any]:
    obligation = lima_db.get_obligation(obligation_id)
    if not obligation:
        return {"ok": False, "error": "unknown_obligation"}
    kind = str(obligation.get("obligation_kind") or "")
    if kind not in LOCAL_CHECK_KINDS:
        return {"ok": False, "error": "not_local_check", "obligation_id": obligation_id}
    problem_id = str(obligation.get("problem_id") or "")
    lima_db.set_obligation_status(
        obligation_id,
        "running_local",
        review_status=str(obligation.get("review_status") or "not_reviewed"),
    )
    refreshed = lima_db.get_obligation(obligation_id) or obligation
    status, summary, artifact = _finite_check(refreshed)
    lima_db.create_artifact(
        problem_id=problem_id,
        universe_id=str(obligation.get("universe_id") or ""),
        artifact_kind="obligation_check",
        content={
            "obligation_id": obligation_id,
            "obligation_kind": kind,
            "title": obligation.get("title"),
            "status": status,
            "summary": summary,
            "artifact": artifact,
            "rerun": True,
            "zero_live_authority": True,
        },
    )
    lima_db.update_obligation_result(
        obligation_id,
        status=status,
        result_summary_md=summary,
        aristotle_ref={
            "executor": "lima_local_obligation_check",
            "rerun": True,
            "live_aristotle_job_created": False,
            "artifact_kind": "obligation_check",
        },
    )
    return {"ok": True, "obligation_id": obligation_id, "status": status, "summary": summary}


def run_queued_obligation_checks(
    lima_db: LimaDatabase,
    *,
    problem_id: str,
    limit: int = 20,
) -> dict[str, Any]:
    """Run bounded Lima-local checks without creating live Aristotle work."""

    obligations = lima_db.list_obligations_by_statuses(
        problem_id, ["queued", "queued_local"], limit=limit
    )
    checked: list[str] = []
    falsified: list[str] = []
    skipped: list[str] = []
    for obligation in obligations:
        kind = str(obligation.get("obligation_kind") or "")
        if kind not in {"finite_check", "counterexample_search"}:
            if kind in FORMAL_REVIEW_KINDS or kind == "literature_crosscheck":
                queue_formal_review(lima_db, obligation_id=str(obligation.get("id") or ""))
            skipped.append(str(obligation.get("id") or ""))
            continue

        lima_db.set_obligation_status(str(obligation["id"]), "running_local")
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
        (falsified if status == "refuted_local" else checked).append(str(obligation["id"]))

    return {
        "ok": True,
        "checked": checked,
        "falsified": falsified,
        "skipped": skipped,
        "queued_seen": len(obligations),
    }
