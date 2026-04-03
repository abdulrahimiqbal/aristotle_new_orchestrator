from __future__ import annotations

from pathlib import Path

from orchestrator.aristotle import _stage_project_dir_for_submit


def test_stage_project_dir_for_submit_omits_result_artifacts(tmp_path: Path) -> None:
    project = tmp_path / "campaign"
    (project / "OrchWorkspace").mkdir(parents=True)
    (project / "lean-toolchain").write_text("leanprover/lean4:nightly", encoding="utf-8")
    (project / "lakefile.lean").write_text("package test", encoding="utf-8")
    (project / "OrchWorkspace" / "Main.lean").write_text("theorem x : True := by trivial", encoding="utf-8")
    (project / "aristotle_result_123.bin").write_bytes(b"junk")
    (project / "aristotle_result_123.contents").mkdir()
    (project / "aristotle_result_123.contents" / "ARISTOTLE_SUMMARY.md").write_text(
        "old result",
        encoding="utf-8",
    )
    (project / "__pycache__").mkdir()
    (project / "__pycache__" / "ignored.pyc").write_bytes(b"pyc")

    staging = _stage_project_dir_for_submit(str(project))
    try:
        staged = Path(staging.name) / project.name
        assert (staged / "lean-toolchain").is_file()
        assert (staged / "lakefile.lean").is_file()
        assert (staged / "OrchWorkspace" / "Main.lean").is_file()
        assert not (staged / "aristotle_result_123.bin").exists()
        assert not (staged / "aristotle_result_123.contents").exists()
        assert not (staged / "__pycache__").exists()
    finally:
        staging.cleanup()
