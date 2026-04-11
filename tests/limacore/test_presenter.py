from __future__ import annotations

from pathlib import Path

from orchestrator.limacore.aristotle import LocalAristotleBackend
from orchestrator.limacore.db import LimaCoreDB
from orchestrator.limacore.loop import LimaCoreLoop
from orchestrator.limacore.presenter import build_index_context, build_workspace_context


def test_presenter_returns_panels_and_stable_metrics(tmp_path: Path) -> None:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    loop = LimaCoreLoop(db, backend=LocalAristotleBackend())
    loop.run_iteration("inward-compression-conjecture")
    index_ctx = build_index_context(db)
    assert index_ctx["cards"]
    ws_ctx = build_workspace_context(db, "inward-compression-conjecture")
    assert "stats" in ws_ctx
    assert "frontier" in ws_ctx
    assert ws_ctx["stats"]["proof_debt"] >= 1
