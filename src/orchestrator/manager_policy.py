"""Manager-side policy: move diversity, map proved-gate, skeptic merge."""

from __future__ import annotations

import json
import logging
from typing import Any

from orchestrator import config as app_config
from orchestrator.db import Database
from orchestrator.models import CampaignState, ManagerDecision, NewExperiment
from orchestrator.problem_map_util import normalize_node_kind, parse_problem_map

logger = logging.getLogger("orchestrator.manager_policy")

_NON_PROVE_KINDS = frozenset(
    {
        "refute",
        "explore",
        "underspecify",
        "perturb",
        "promote",
        "reformulate",
        "center",
    }
)


def ensure_move_kind_diversity(
    experiments: list[NewExperiment],
    state: CampaignState,
    problem_map: dict[str, Any],
) -> list[NewExperiment]:
    """Append a explore move if the batch is all prove and env requests non-prove diversity."""
    min_non = int(app_config.MANAGER_MIN_NON_PROVE_MOVES_PER_BATCH or 0)
    if min_non <= 0 or not experiments:
        return experiments

    has_non_prove = any(
        (e.move_kind or "prove").strip().lower() in _NON_PROVE_KINDS for e in experiments
    )
    if has_non_prove:
        return experiments

    open_targets = [t for t in state.targets if t.status.value == "open"]
    if not open_targets:
        return experiments

    # Prefer a target that appears in active_fronts (node ids) — match by id substring in description is fragile;
    # use first open target as a stable fallback.
    fronts = problem_map.get("active_fronts") or []
    chosen = open_targets[0]
    if isinstance(fronts, list) and fronts:
        fid = str(fronts[0]).strip()
        for t in open_targets:
            if fid and fid in (t.id, t.description[:40]):
                chosen = t
                break

    hint = ""
    nodes = problem_map.get("nodes") or []
    if isinstance(nodes, list) and isinstance(fronts, list) and fronts:
        fid = str(fronts[0])
        for n in nodes:
            if isinstance(n, dict) and str(n.get("id")) == fid:
                hint = str(n.get("label") or "")[:200]
                break

    obj = (
        "Exploratory pass: state a plausible strengthening of the current goal (or of the active front "
        f"\"{hint or chosen.description[:120]}\"). Either sketch why it might be false, or outline "
        "the minimal lemma that would disprove it. Do not claim the full main conjecture."
    )
    extra = NewExperiment(
        target_id=chosen.id,
        objective=obj,
        move_kind="explore",
        move_note="manager_policy:non_prove_diversity",
    )
    out = list(experiments) + [extra]
    logger.info(
        "Injected explore experiment for move-kind diversity (campaign has only prove moves)"
    )
    return out


def resolve_planned_target_id(
    target_id: str, valid_target_ids: set[str]
) -> tuple[str | None, str | None]:
    """Resolve minor LLM target-id drift back to a real target id when safe.

    Returns (resolved_target_id, alias_source). alias_source is the original id when
    we successfully rewrote it, otherwise None.
    """
    candidate = str(target_id or "").strip()
    if not candidate:
        return None, None
    if candidate in valid_target_ids:
        return candidate, None
    if "_" in candidate:
        base = candidate.split("_", 1)[0].strip()
        if base in valid_target_ids:
            return base, candidate
    return None, None


def apply_map_proved_gate(map_json: str, *, campaign_id: str, db: Database) -> str:
    """Downgrade proved → active for gated node kinds unless operator acked (see /admin/map-node-ack)."""
    kinds = app_config.MAP_PROVED_GATE_KINDS
    if not kinds:
        return map_json

    pmap = parse_problem_map(map_json)
    nodes = pmap.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        return map_json

    acked = db.list_map_node_acks(campaign_id)
    changed = False
    new_nodes: list[dict[str, Any]] = []
    for n in nodes:
        if not isinstance(n, dict):
            continue
        d = dict(n)
        st = str(d.get("status", "")).lower()
        nk = normalize_node_kind(d.get("kind"))
        nid = str(d.get("id") or "")
        if st == "proved" and nk in kinds and nid and nid not in acked:
            d["status"] = "active"
            changed = True
            logger.debug(
                "Map gate: node %s kind=%s downgraded proved→active until ack", nid, nk
            )
        new_nodes.append(d)

    if not changed:
        return map_json

    pmap["nodes"] = new_nodes
    return json.dumps(pmap, ensure_ascii=False)


def merge_skeptic_experiments(
    primary: ManagerDecision,
    skeptic_experiments: list[NewExperiment],
    *,
    max_total: int,
) -> ManagerDecision:
    """Append skeptic experiments, capped by max_total additional items."""
    if not skeptic_experiments or max_total <= 0:
        return primary

    merged = list(primary.new_experiments)
    for e in skeptic_experiments[:max_total]:
        merged.append(e)

    return primary.model_copy(update={"new_experiments": merged})
