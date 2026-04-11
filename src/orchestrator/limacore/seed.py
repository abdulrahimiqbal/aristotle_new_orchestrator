from __future__ import annotations

from .artifacts import utc_now
from .frontier import ensure_target_frontier
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .db import LimaCoreDB


def ensure_seed_data(db: LimaCoreDB) -> None:
    collatz_id, _ = db.create_problem(
        slug="collatz",
        title="Collatz conjecture",
        statement_md="For every positive integer n, repeated application of the Collatz map reaches 1.",
        domain="number theory",
        target_theorem="Every positive integer Collatz orbit reaches 1.",
    )
    inward_id, _ = db.create_problem(
        slug="inward-compression-conjecture",
        title="Inward Compression Conjecture",
        statement_md=(
            "A state is a finite strictly increasing sequence of integers with fixed length. "
            "A legal move increments one interior-low coordinate and decrements a later coordinate when the resulting sequence remains strictly increasing. "
            "Conjecture: every legal sequence terminates and the final stable state depends only on length and total sum."
        ),
        domain="discrete dynamics",
        target_theorem="For every fixed length and total sum, every legal inward-compression sequence terminates at a unique stable state.",
    )
    ensure_target_frontier(db, collatz_id, target_statement="Every positive integer Collatz orbit reaches 1.")
    ensure_target_frontier(
        db,
        inward_id,
        target_statement="Every legal inward-compression sequence terminates at a unique stable state determined by length and sum.",
    )
