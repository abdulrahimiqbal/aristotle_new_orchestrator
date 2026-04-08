from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import re
from pathlib import Path
from typing import Any, Protocol

from orchestrator import config as app_config
from orchestrator.aristotle import submit
from orchestrator.db import Database
from orchestrator.lima_db import LimaDatabase
from orchestrator.lima_models import LimaObligationSpec, LimaUniverseSpec, safe_json_loads, slugify
from orchestrator.lima_rupture import collatz_step, residue_pattern_summary
from orchestrator.models import ExperimentStatus, Verdict
from orchestrator.workspace_seed import ensure_workspace


LOCAL_CHECK_KINDS = {"finite_check", "counterexample_search", "invariant_check", "consistency"}
FORMAL_REVIEW_KINDS = {"lean_goal", "bridge_lemma", "equivalence"}
LIMA_ARISTOTLE_PROMPT_PREFIX = "[Lima Formal] Collatz strict-survivor obligations"
LIMA_ARISTOTLE_ACTIVE_STATUSES = [
    ExperimentStatus.PENDING.value,
    ExperimentStatus.SUBMITTED.value,
    ExperimentStatus.RUNNING.value,
]
LIMA_FORMAL_SYNC_STATUSES = {
    "pending": "submitted_formal",
    "submitted": "submitted_formal",
    "running": "submitted_formal",
}


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


FormalBackendProtocol = FormalBackend


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


LocalFormalStubBackend = LocalStubFormalBackend


class FutureAristotleLeanFormalBackend(LocalStubFormalBackend):
    backend_kind = "future_aristotle_lean_stub"

    def submit_approved(self, packet: FormalReviewPacket) -> dict[str, Any]:
        result = super().submit_approved(packet)
        result["backend"] = self.backend_kind
        result["message"] = (
            "Future Aristotle/Lean adapter placeholder recorded approval; "
            "no live work was submitted."
        )
        return result


def _lima_aristotle_prompt_prefix() -> str:
    slug = (app_config.LIMA_ARISTOTLE_CAMPAIGN_SLUG or "collatz-lima-formal").strip()
    threshold = (app_config.LIMA_ARISTOTLE_THRESHOLD or "strict_survivor").strip()
    return f"{LIMA_ARISTOTLE_PROMPT_PREFIX} [{slug}; threshold={threshold}]"


def _parse_submission_ref(obligation: dict[str, Any]) -> dict[str, Any]:
    parsed = safe_json_loads(obligation.get("formal_submission_ref_json"), {})
    return parsed if isinstance(parsed, dict) else {}


def _has_lima_aristotle_submission(obligation: dict[str, Any]) -> bool:
    ref = _parse_submission_ref(obligation)
    return bool(
        ref.get("aristotle_experiment_id")
        or ref.get("aristotle_job_id")
        or ref.get("campaign_id")
    )


def strict_aristotle_eligibility(
    lima_db: LimaDatabase,
    obligation: dict[str, Any],
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Return the strict auto-submit decision for a Lima obligation."""

    reasons: list[str] = []
    warnings: list[str] = []
    kind = str(obligation.get("obligation_kind") or "")
    universe_id = str(obligation.get("universe_id") or obligation.get("source_universe_id") or "")
    universe = lima_db.get_universe(universe_id) if universe_id else None
    universe_status = str((universe or {}).get("universe_status") or "").lower()
    if kind not in FORMAL_REVIEW_KINDS:
        reasons.append(f"kind {kind or 'unknown'} is not a formal Aristotle kind")
    if _has_lima_aristotle_submission(obligation):
        reasons.append("obligation already has a Lima Aristotle submission reference")
    canonical_hash = str(obligation.get("canonical_hash") or "")
    problem_id = str(obligation.get("problem_id") or "")
    if canonical_hash and problem_id:
        for peer in lima_db.list_obligations(problem_id, limit=200):
            if str(peer.get("id") or "") == str(obligation.get("id") or ""):
                continue
            if str(peer.get("canonical_hash") or "") == canonical_hash and _has_lima_aristotle_submission(peer):
                reasons.append("canonical duplicate already has a Lima Aristotle submission reference")
                break
    if not force:
        threshold = (app_config.LIMA_ARISTOTLE_THRESHOLD or "strict_survivor").strip().lower()
        if threshold == "strict_survivor":
            if universe_status not in {"promising", "formalized", "handed_off"}:
                reasons.append(f"universe status {universe_status or 'unknown'} is not a strict survivor")
            fractures = lima_db.list_fractures_for_universe(universe_id, limit=50) if universe_id else []
            prior_art = [
                f
                for f in fractures
                if str(f.get("failure_type") or "").lower() == "prior_art"
                and float(f.get("confidence") or 0) >= 0.25
            ]
            if prior_art:
                reasons.append("universe has active prior-art fracture pressure")
                warnings.extend(str(f.get("breakpoint_md") or "") for f in prior_art[:3])
            local_siblings = lima_db.list_obligations_for_universe(universe_id, limit=100) if universe_id else []
            has_verified_local = any(str(o.get("status") or "") == "verified_local" for o in local_siblings)
            has_refuted_local = any(str(o.get("status") or "") == "refuted_local" for o in local_siblings)
            if not has_verified_local:
                reasons.append("no linked local check is verified_local")
            if has_refuted_local:
                reasons.append("a linked local check is refuted_local")
            try:
                value = float(
                    obligation.get("estimated_formalization_value")
                    or obligation.get("estimated_value")
                    or 0
                )
            except (TypeError, ValueError):
                value = 0.0
            if value < 4.0:
                reasons.append(f"estimated formalization value {value:.2f} is below strict threshold")
    return {
        "eligible": not reasons,
        "force": force,
        "threshold": app_config.LIMA_ARISTOTLE_THRESHOLD,
        "universe_status": universe_status,
        "reasons": reasons,
        "warnings": [w for w in warnings if w],
    }


def _lima_aristotle_objective(obligation: dict[str, Any], packet: FormalReviewPacket) -> str:
    lines = [
        "Lima formal obligation. Work only on this narrow candidate; do not broaden the campaign.",
        f"Obligation: {obligation.get('title') or packet.obligation_id}",
        f"Kind: {obligation.get('obligation_kind') or 'formal'}",
        "",
        "Statement:",
        str(obligation.get("statement_md") or "").strip(),
    ]
    if str(obligation.get("lean_goal") or "").strip():
        lines.extend(["", "Lean goal:", str(obligation.get("lean_goal") or "").strip()])
    if str(obligation.get("why_exists_md") or "").strip():
        lines.extend(["", "Why Lima created this:", str(obligation.get("why_exists_md") or "").strip()])
    if str(obligation.get("prove_or_kill_md") or "").strip():
        lines.extend(["", "What success/failure means:", str(obligation.get("prove_or_kill_md") or "").strip()])
    lines.extend(
        [
            "",
            "Return a narrow verdict: proved, disproved, partial, or inconclusive. Preserve blockers, unsolved goals, and any counterexample signal.",
        ]
    )
    return "\n".join(line for line in lines if line is not None)[:12000]


def _campaign_budget(main_db: Database, campaign_id: str) -> dict[str, Any]:
    active = main_db.count_campaign_experiments_by_statuses(
        campaign_id, LIMA_ARISTOTLE_ACTIVE_STATUSES
    )
    since = (datetime.utcnow() - timedelta(days=1)).isoformat()
    daily = main_db.count_campaign_submissions_since(campaign_id, since)
    return {
        "active": active,
        "daily_submissions": daily,
        "max_active": int(app_config.LIMA_ARISTOTLE_MAX_ACTIVE),
        "max_daily_submissions": int(app_config.LIMA_ARISTOTLE_MAX_DAILY_SUBMISSIONS),
        "within_budget": active < int(app_config.LIMA_ARISTOTLE_MAX_ACTIVE)
        and daily < int(app_config.LIMA_ARISTOTLE_MAX_DAILY_SUBMISSIONS),
    }


class AristotleFormalBackend(LocalStubFormalBackend):
    backend_kind = "aristotle_formal"

    def __init__(
        self,
        *,
        lima_db: LimaDatabase,
        main_db: Database,
        force: bool = False,
    ) -> None:
        self.lima_db = lima_db
        self.main_db = main_db
        self.force = force

    def _campaign(self, obligation: dict[str, Any], packet: FormalReviewPacket) -> dict[str, Any]:
        prefix = _lima_aristotle_prompt_prefix()
        existing = self.main_db.get_campaign_by_prompt_prefix(prefix)
        if existing and str(existing.get("status") or "") == "active":
            workspace_dir = str(
                existing.get("workspace_dir")
                or (Path(app_config.WORKSPACE_ROOT).resolve() / str(existing["id"]))
            )
            ensure_workspace(workspace_dir, app_config.LIMA_ARISTOTLE_WORKSPACE_TEMPLATE)
            return {**existing, "workspace_dir": workspace_dir}
        research_packet = {
            "source": "lima",
            "threshold": app_config.LIMA_ARISTOTLE_THRESHOLD,
            "campaign_slug": app_config.LIMA_ARISTOTLE_CAMPAIGN_SLUG,
            "zero_live_authority": False,
            "operator_approved_escalation_required": True,
            "packet": packet.payload,
        }
        campaign_id = self.main_db.create_campaign(
            prefix,
            workspace_root=app_config.WORKSPACE_ROOT,
            workspace_template=app_config.LIMA_ARISTOTLE_WORKSPACE_TEMPLATE,
            problem_refs_json=json.dumps({"source": "lima", "problem_id": obligation.get("problem_id")}),
            research_packet_json=json.dumps(research_packet),
            mathlib_knowledge=True,
        )
        campaign = self.main_db.get_campaign_row(campaign_id) or {"id": campaign_id}
        workspace_dir = str(
            campaign.get("workspace_dir")
            or (Path(app_config.WORKSPACE_ROOT).resolve() / campaign_id)
        )
        ensure_workspace(workspace_dir, app_config.LIMA_ARISTOTLE_WORKSPACE_TEMPLATE)
        return {**campaign, "workspace_dir": workspace_dir}

    def _budget(self, campaign_id: str) -> dict[str, Any]:
        return _campaign_budget(self.main_db, campaign_id)

    async def submit_approved_async(self, packet: FormalReviewPacket) -> dict[str, Any]:
        obligation = self.lima_db.get_obligation(packet.obligation_id)
        if not obligation:
            return {"ok": False, "error": "unknown_obligation", "submitted_formal": False}
        eligibility = strict_aristotle_eligibility(self.lima_db, obligation, force=self.force)
        if not eligibility["eligible"]:
            return {
                "ok": False,
                "error": "not_eligible",
                "backend": self.backend_kind,
                "status": "blocked_by_strict_threshold",
                "submitted_formal": False,
                "live_aristotle_job_created": False,
                "eligibility": eligibility,
                "message": "Lima strict-survivor threshold blocked Aristotle submission.",
            }
        campaign = self._campaign(obligation, packet)
        campaign_id = str(campaign["id"])
        budget = self._budget(campaign_id)
        if not budget["within_budget"]:
            return {
                "ok": False,
                "error": "budget_exhausted",
                "backend": self.backend_kind,
                "status": "blocked_by_budget",
                "submitted_formal": False,
                "live_aristotle_job_created": False,
                "budget": budget,
                "eligibility": eligibility,
                "message": "Lima Aristotle budget blocked submission.",
            }
        target_description = (
            f"[Lima] {obligation.get('title') or packet.obligation_id}\n\n"
            f"Universe: {obligation.get('universe_title') or obligation.get('universe_id') or 'unknown'}\n"
            f"Lineage: run={obligation.get('source_run_id') or ''} universe={obligation.get('source_universe_id') or obligation.get('universe_id') or ''}"
        )
        target_id = self.main_db.add_targets(campaign_id, [target_description])[0]
        objective = _lima_aristotle_objective(obligation, packet)
        experiment_id = self.main_db.create_experiment(
            campaign_id,
            target_id,
            objective,
            move_kind="prove",
            move_note=f"Lima obligation {packet.obligation_id}; threshold={app_config.LIMA_ARISTOTLE_THRESHOLD}",
        )
        workspace_dir = str(campaign.get("workspace_dir") or "")
        ensure_workspace(workspace_dir, app_config.LIMA_ARISTOTLE_WORKSPACE_TEMPLATE)
        job_id, error = await submit(objective, workspace_dir)
        ref = {
            "ok": not bool(error),
            "backend": self.backend_kind,
            "status": "submitted" if job_id else "submission_failed",
            "submitted_formal": bool(job_id),
            "live_aristotle_job_created": bool(job_id),
            "campaign_id": campaign_id,
            "target_id": target_id,
            "aristotle_experiment_id": experiment_id,
            "aristotle_job_id": job_id,
            "campaign_slug": app_config.LIMA_ARISTOTLE_CAMPAIGN_SLUG,
            "threshold": app_config.LIMA_ARISTOTLE_THRESHOLD,
            "budget": self._budget(campaign_id),
            "eligibility": eligibility,
            "message": "Submitted Lima obligation to dedicated Aristotle campaign."
            if job_id
            else f"Aristotle submission failed: {error}",
            "zero_live_authority": False,
            "operator_approved_escalation_required": True,
        }
        if job_id:
            self.main_db.update_experiment_submitted(experiment_id, job_id)
        else:
            self.main_db.update_experiment_failed(
                experiment_id, error or "Lima Aristotle submission failed", verdict=Verdict.INCONCLUSIVE.value
            )
        return ref


def make_formal_backend(
    kind: str | None = None,
    *,
    lima_db: LimaDatabase | None = None,
    main_db: Database | None = None,
    force_aristotle: bool = False,
) -> FormalBackend:
    selected = (kind or app_config.LIMA_FORMAL_BACKEND or "local_stub").strip().lower()
    # Future Aristotle/Lean/Mathlib adapters must preserve the same approval boundary.
    if selected in {"future_aristotle_lean_stub", "aristotle_lean_stub"}:
        return FutureAristotleLeanFormalBackend()
    if selected in {"aristotle_formal", "aristotle", "lean_aristotle"} and lima_db and main_db:
        return AristotleFormalBackend(
            lima_db=lima_db,
            main_db=main_db,
            force=force_aristotle,
        )
    return LocalStubFormalBackend()


def queue_formal_review(
    lima_db: LimaDatabase,
    *,
    obligation_id: str,
    backend: FormalBackend | None = None,
    main_db: Database | None = None,
) -> dict[str, Any]:
    obligation = lima_db.get_obligation(obligation_id)
    if not obligation:
        return {"ok": False, "error": "unknown_obligation"}
    backend = backend or make_formal_backend(
        str(obligation.get("formal_backend") or ""),
        lima_db=lima_db,
        main_db=main_db,
    )
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
        review_note="Queued for formal review; no live Aristotle job was created.",
        result_summary_md="Queued for formal review. No live Aristotle job was created.",
    )
    return {
        "ok": True,
        "review_id": review_id,
        "obligation_id": obligation_id,
        "backend": packet.backend_kind,
    }


def _record_formal_backend_result(
    lima_db: LimaDatabase,
    *,
    obligation: dict[str, Any],
    review_id: str,
    packet: FormalReviewPacket,
    backend_result: dict[str, Any],
) -> dict[str, Any]:
    if backend_result.get("ok", True) is False:
        lima_db.set_obligation_status(
            str(obligation["id"]),
            str(obligation.get("status") or "queued_formal_review"),
            review_status=str(obligation.get("review_status") or "pending"),
            formal_backend=packet.backend_kind,
            formal_payload=packet.payload,
            formal_submission_ref=backend_result,
            review_note=str(backend_result.get("message") or backend_result.get("error") or "Formal escalation blocked."),
            result_summary_md=str(backend_result.get("message") or "Formal escalation blocked."),
        )
        lima_db.update_formal_review_item(
            review_id,
            status=str(obligation.get("status") or "queued_formal_review"),
            review_decision=str(obligation.get("review_status") or "pending"),
            backend_result=backend_result,
        )
        return {
            "ok": False,
            "review_id": review_id,
            "obligation_id": str(obligation["id"]),
            "status": str(obligation.get("status") or "queued_formal_review"),
            "backend_result": backend_result,
            "error": backend_result.get("error"),
        }

    status = "submitted_formal" if backend_result.get("submitted_formal") else "approved_for_formal"
    lima_db.set_obligation_status(
        str(obligation["id"]),
        status,
        review_status="approved",
        formal_backend=packet.backend_kind,
        formal_payload=packet.payload,
        formal_submission_ref=backend_result,
        review_note=str(backend_result.get("message") or "Approved for formal review."),
        result_summary_md=str(backend_result.get("message") or "Approved for formal review."),
    )
    lima_db.update_formal_review_item(
        review_id,
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
        "review_id": review_id,
        "obligation_id": str(obligation["id"]),
        "status": status,
        "backend_result": backend_result,
    }


def approve_formal_review(
    lima_db: LimaDatabase,
    *,
    obligation_id: str,
    backend: FormalBackend | None = None,
    main_db: Database | None = None,
) -> dict[str, Any]:
    queued = queue_formal_review(
        lima_db,
        obligation_id=obligation_id,
        backend=backend,
        main_db=main_db,
    )
    if not queued.get("ok"):
        return queued
    obligation = lima_db.get_obligation(obligation_id)
    if not obligation:
        return {"ok": False, "error": "unknown_obligation"}
    backend = backend or make_formal_backend(
        str(obligation.get("formal_backend") or ""),
        lima_db=lima_db,
        main_db=main_db,
    )
    packet = backend.build_packet(obligation)
    backend_result = backend.submit_approved(packet)
    return _record_formal_backend_result(
        lima_db,
        obligation=obligation,
        review_id=str(queued["review_id"]),
        packet=packet,
        backend_result=backend_result,
    )


async def approve_formal_review_async(
    lima_db: LimaDatabase,
    *,
    obligation_id: str,
    backend: FormalBackend | None = None,
    main_db: Database | None = None,
    force_aristotle: bool = False,
) -> dict[str, Any]:
    queued = queue_formal_review(
        lima_db,
        obligation_id=obligation_id,
        backend=backend,
        main_db=main_db,
    )
    if not queued.get("ok"):
        return queued
    obligation = lima_db.get_obligation(obligation_id)
    if not obligation:
        return {"ok": False, "error": "unknown_obligation"}
    backend = backend or make_formal_backend(
        str(obligation.get("formal_backend") or ""),
        lima_db=lima_db,
        main_db=main_db,
        force_aristotle=force_aristotle,
    )
    packet = backend.build_packet(obligation)
    async_submit = getattr(backend, "submit_approved_async", None)
    if async_submit:
        backend_result = await async_submit(packet)
    else:
        backend_result = backend.submit_approved(packet)
    return _record_formal_backend_result(
        lima_db,
        obligation=obligation,
        review_id=str(queued["review_id"]),
        packet=packet,
        backend_result=backend_result,
    )


def reject_formal_review(lima_db: LimaDatabase, *, obligation_id: str) -> dict[str, Any]:
    obligation = lima_db.get_obligation(obligation_id)
    if not obligation:
        return {"ok": False, "error": "unknown_obligation"}
    lima_db.set_obligation_status(
        obligation_id,
        "inconclusive",
        review_status="rejected",
        review_note="Formal review rejected by operator.",
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
        review_note="Archived by operator.",
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

    obligations = lima_db.list_obligations_by_statuses(problem_id, ["queued_local"], limit=limit)
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


def lima_aristotle_budget(main_db: Database) -> dict[str, Any]:
    campaign = main_db.get_campaign_by_prompt_prefix(_lima_aristotle_prompt_prefix())
    if not campaign or str(campaign.get("status") or "") != "active":
        return {
            "campaign_id": "",
            "active": 0,
            "daily_submissions": 0,
            "max_active": int(app_config.LIMA_ARISTOTLE_MAX_ACTIVE),
            "max_daily_submissions": int(app_config.LIMA_ARISTOTLE_MAX_DAILY_SUBMISSIONS),
            "within_budget": True,
        }
    budget = _campaign_budget(main_db, str(campaign["id"]))
    return {**budget, "campaign_id": str(campaign["id"])}


async def submit_promising_formal_obligations(
    lima_db: LimaDatabase,
    main_db: Database,
    *,
    problem_id: str,
    limit: int = 5,
) -> dict[str, Any]:
    """Submit only strict-survivor Lima formal obligations to Aristotle."""

    if not (app_config.LIMA_FORMAL_AUTO_SUBMIT and app_config.LIMA_ARISTOTLE_AUTO_SUBMIT):
        return {"ok": True, "enabled": False, "submitted": [], "blocked": []}
    obligations = lima_db.list_obligations_by_statuses(
        problem_id,
        ["queued_formal_review", "approved_for_formal"],
        limit=limit,
    )
    submitted: list[str] = []
    blocked: list[dict[str, Any]] = []
    for obligation in obligations:
        obligation_id = str(obligation.get("id") or "")
        if not obligation_id or str(obligation.get("obligation_kind") or "") not in FORMAL_REVIEW_KINDS:
            continue
        if _has_lima_aristotle_submission(obligation):
            blocked.append({"obligation_id": obligation_id, "reason": "duplicate_submission"})
            continue
        backend = AristotleFormalBackend(lima_db=lima_db, main_db=main_db)
        result = await approve_formal_review_async(
            lima_db,
            obligation_id=obligation_id,
            backend=backend,
            main_db=main_db,
        )
        if result.get("ok") and result.get("status") == "submitted_formal":
            submitted.append(obligation_id)
        else:
            blocked.append(
                {
                    "obligation_id": obligation_id,
                    "reason": result.get("error") or result.get("status") or "not_submitted",
                    "result": result,
                }
            )
    return {
        "ok": True,
        "enabled": True,
        "submitted": submitted,
        "blocked": blocked,
        "budget": lima_aristotle_budget(main_db),
    }


def _sync_status_from_experiment(experiment: dict[str, Any]) -> str:
    status = str(experiment.get("status") or "").lower()
    if status in LIMA_FORMAL_SYNC_STATUSES:
        return LIMA_FORMAL_SYNC_STATUSES[status]
    if status == ExperimentStatus.COMPLETED.value:
        verdict = str(experiment.get("verdict") or "").lower()
        if verdict == Verdict.PROVED.value:
            return "verified_formal"
        if verdict == Verdict.DISPROVED.value:
            return "refuted_formal"
        return "inconclusive"
    if status == ExperimentStatus.FAILED.value:
        return "inconclusive"
    return "inconclusive"


def sync_lima_aristotle_results(
    lima_db: LimaDatabase,
    main_db: Database,
    *,
    problem_id: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """Ingest main Aristotle experiment rows back into Lima formal obligations."""

    problems = [lima_db.get_problem(problem_id)] if problem_id else lima_db.list_problems()
    synced: list[str] = []
    skipped: list[str] = []
    for problem in problems:
        pid = str(problem.get("id") or "")
        if not pid:
            continue
        obligations = lima_db.list_obligations_by_statuses(
            pid,
            ["submitted_formal", "approved_for_formal"],
            limit=limit,
        )
        for obligation in obligations:
            obligation_id = str(obligation.get("id") or "")
            ref = _parse_submission_ref(obligation)
            experiment_id = str(ref.get("aristotle_experiment_id") or "")
            if not obligation_id or not experiment_id:
                skipped.append(obligation_id)
                continue
            experiment = main_db.get_experiment_row(experiment_id)
            if not experiment:
                skipped.append(obligation_id)
                continue
            next_status = _sync_status_from_experiment(experiment)
            merged_ref = {
                **ref,
                "last_sync_at": datetime.utcnow().isoformat(),
                "main_experiment_status": experiment.get("status"),
                "verdict": experiment.get("verdict"),
                "result_summary": experiment.get("result_summary"),
                "parsed_proved_lemmas": safe_json_loads(experiment.get("parsed_proved_lemmas_json"), []),
                "parsed_blockers": safe_json_loads(experiment.get("parsed_blockers_json"), []),
                "parsed_unsolved_goals": safe_json_loads(experiment.get("parsed_unsolved_goals_json"), []),
                "parse_warnings": safe_json_loads(experiment.get("parse_warnings_json"), []),
                "aristotle_job_id": experiment.get("aristotle_job_id") or ref.get("aristotle_job_id"),
            }
            summary = str(experiment.get("result_summary") or "")
            if not summary:
                summary = f"Synced Aristotle experiment {experiment_id}: {experiment.get('status') or 'unknown'}."
            lima_db.update_obligation_result(
                obligation_id,
                status=next_status,
                result_summary_md=summary[:8000],
                aristotle_ref=merged_ref,
            )
            for review in lima_db.list_formal_reviews(pid, limit=100):
                if str(review.get("obligation_id") or "") == obligation_id:
                    lima_db.update_formal_review_item(
                        str(review["id"]),
                        status=next_status,
                        review_decision="approved",
                        backend_result=merged_ref,
                    )
                    break
            lima_db.create_artifact(
                problem_id=pid,
                universe_id=str(obligation.get("universe_id") or ""),
                artifact_kind="aristotle_result_sync",
                content={
                    "obligation_id": obligation_id,
                    "aristotle_experiment_id": experiment_id,
                    "status": next_status,
                    "experiment": merged_ref,
                },
            )
            synced.append(obligation_id)
    return {"ok": True, "synced": synced, "skipped": skipped}
