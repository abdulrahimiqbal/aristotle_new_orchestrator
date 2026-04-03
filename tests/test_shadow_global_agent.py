from __future__ import annotations

from pathlib import Path

from orchestrator.db import Database
from orchestrator.shadow_agent import _normalize_global_response


def test_normalize_global_response_filters_invalid_promotions(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "g.db"))
    db.initialize()
    cid = db.create_campaign("c", workspace_root=str(tmp_path / "ws"))
    tid = db.add_targets(cid, ["t"])[0]
    raw = {
        "run_summary": "s",
        "solved_world": {"claim": "world"},
        "hypotheses": [
            {
                "kind": "new_axiom",
                "title": "A1",
                "body_md": "SPECULATIVE requires new axioms",
                "lean_snippet": "",
                "evidence": [{"campaign_id": cid, "target_id": tid, "note": "x"}],
            }
        ],
        "promotion_requests": [
            {"kind": "new_target", "campaign_id": cid, "description": "ok"},
            {"kind": "new_experiment", "campaign_id": cid, "target_id": tid, "objective": "o"},
            {"kind": "new_experiment", "campaign_id": "bad", "target_id": tid, "objective": "x"},
        ],
    }
    out, warnings = _normalize_global_response(raw, db)
    assert out["solved_world"]["claim"] == "world"
    assert len(out["promotion_requests"]) == 2
    assert out["hypotheses"][0]["score_0_100"] >= 0
    assert out["hypotheses"][0]["groundability_tier"] in ("A", "B", "C")
    assert isinstance(warnings, list)
