"""Regression-style checks for manager tick + DB persistence (no real Aristotle / LLM)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from orchestrator.aristotle import ExtractedArchive
from orchestrator.db import Database
from orchestrator.manager import tick

_MARKDOWN = """# Summary of changes
## Completed
- lemma orch_result : True := by trivial
## Partial
- sorry remaining on subgoal
"""


@pytest.mark.asyncio
async def test_tick_marks_completed_persists_parsed_and_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LLM_API_KEY", "")
    root = tmp_path / "wsroot"
    db_path = tmp_path / "db.sqlite"
    db = Database(str(db_path))
    db.initialize()
    cid = db.create_campaign("benchmark prompt", workspace_root=str(root), workspace_template="minimal")
    (tid,) = db.add_targets(cid, ["first target"])
    eid = db.create_experiment(cid, tid, "prove triviality")
    db.update_experiment_submitted(eid, "00000000-0000-0000-0000-000000000099")

    ws_dir = str((root.resolve() / cid))
    campaign_row = {"id": cid, "workspace_dir": ws_dir}

    async def _poll(*_a, **_k):
        return "completed", ExtractedArchive(markdown=_MARKDOWN)

    async def _submit(*_a, **_k):
        return "", "skip"

    with patch("orchestrator.manager.poll", _poll), patch("orchestrator.manager.submit", _submit):
        await tick(db, campaign_row, tick_number=0)

    state = db.get_campaign_state(cid)
    exp = next(e for e in state.experiments if e.id == eid)
    assert exp.status.value == "completed"
    assert exp.verdict is not None
    assert len(exp.parsed_proved_lemmas) >= 1
    assert exp.parse_source == "markdown_derived"
    assert state.manager_context_experiments
    assert tid in state.manager_context_experiments_by_target
    led = db.get_recent_ledger_entries(cid, 20)
    assert any(row["experiment_id"] == eid for row in led)


@pytest.mark.asyncio
async def test_decompose_fallback_single_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LLM_API_KEY", "")
    from orchestrator.llm import decompose_prompt

    targets = await decompose_prompt("Prove that 2+2=4")
    assert len(targets) == 1
    assert "2+2" in targets[0].description or "Prove" in targets[0].description
