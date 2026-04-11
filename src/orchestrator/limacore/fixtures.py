from __future__ import annotations

from pathlib import Path

from .aristotle import LocalAristotleBackend
from .db import LimaCoreDB
from .loop import LimaCoreLoop


def seeded_db(tmp_path: Path) -> LimaCoreDB:
    db = LimaCoreDB(str(tmp_path / "limacore.db"))
    db.initialize()
    return db


def seeded_loop(tmp_path: Path) -> LimaCoreLoop:
    db = seeded_db(tmp_path)
    return LimaCoreLoop(db, backend=LocalAristotleBackend())
