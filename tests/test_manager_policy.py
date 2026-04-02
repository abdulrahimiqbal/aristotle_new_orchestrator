from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from orchestrator import config as app_config
from orchestrator.manager_policy import apply_map_proved_gate, ensure_move_kind_diversity
from orchestrator.models import (
    Campaign,
    CampaignState,
    CampaignStatus,
    NewExperiment,
    Target,
    TargetStatus,
)


def test_apply_map_proved_gate_downgrades_without_ack(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        app_config,
        "MAP_PROVED_GATE_KINDS",
        frozenset({"obstruction"}),
    )
    db = MagicMock()
    db.list_map_node_acks.return_value = set()
    raw = json.dumps(
        {
            "summary": "x",
            "nodes": [
                {
                    "id": "n1",
                    "label": "hard",
                    "status": "proved",
                    "kind": "obstruction",
                }
            ],
            "edges": [],
            "active_fronts": ["n1"],
        }
    )
    out = apply_map_proved_gate(raw, campaign_id="c1", db=db)
    d = json.loads(out)
    assert d["nodes"][0]["status"] == "active"


def test_apply_map_proved_gate_respects_ack(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        app_config,
        "MAP_PROVED_GATE_KINDS",
        frozenset({"obstruction"}),
    )
    db = MagicMock()
    db.list_map_node_acks.return_value = {"n1"}
    raw = json.dumps(
        {
            "summary": "x",
            "nodes": [
                {
                    "id": "n1",
                    "label": "hard",
                    "status": "proved",
                    "kind": "obstruction",
                }
            ],
            "edges": [],
            "active_fronts": ["n1"],
        }
    )
    out = apply_map_proved_gate(raw, campaign_id="c1", db=db)
    d = json.loads(out)
    assert d["nodes"][0]["status"] == "proved"


def test_ensure_move_kind_diversity_adds_explore(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(app_config, "MANAGER_MIN_NON_PROVE_MOVES_PER_BATCH", 1)
    state = CampaignState(
        campaign=Campaign(
            id="c",
            prompt="p",
            status=CampaignStatus.ACTIVE,
            workspace_dir="/w",
        ),
        targets=[
            Target(id="t1", campaign_id="c", description="first open", status=TargetStatus.OPEN)
        ],
        experiments=[],
    )
    ex = [
        NewExperiment(target_id="t1", objective="prove X", move_kind="prove"),
    ]
    pmap = {"active_fronts": ["t1"], "nodes": []}
    out = ensure_move_kind_diversity(ex, state, pmap)
    assert len(out) == 2
    assert out[1].move_kind == "explore"
    assert "manager_policy" in out[1].move_note
