from __future__ import annotations

from pathlib import Path

from orchestrator.workspace_seed import ensure_workspace, template_dir


def test_mathlib_lakefile_requires_mathlib_repo() -> None:
    lf = template_dir("mathlib") / "lakefile.lean"
    text = lf.read_text(encoding="utf-8")
    assert "leanprover-community/mathlib4" in text
    assert "require mathlib" in text


def test_ensure_workspace_copies_mathlib_toolchain(tmp_path: Path) -> None:
    dest = tmp_path / "camp1"
    ensure_workspace(str(dest), "mathlib")
    assert (dest / "lean-toolchain").is_file()
    assert "v4.29" in (dest / "lean-toolchain").read_text(encoding="utf-8")
    assert (dest / "lakefile.lean").is_file()
