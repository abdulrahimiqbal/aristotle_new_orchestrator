from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from orchestrator import config as app_config
from orchestrator.lima_db import LimaDatabase
from orchestrator.lima_models import LimaUniverseSpec, slugify


@dataclass(frozen=True)
class LiteratureRecord:
    source_type: str
    title: str
    authors: list[str] = field(default_factory=list)
    year: int | None = None
    venue: str = ""
    doi: str = ""
    arxiv_id: str = ""
    url: str = ""
    abstract_md: str = ""
    bibtex: dict[str, Any] = field(default_factory=dict)
    extracts: list[dict[str, Any]] = field(default_factory=list)


class LiteratureBackend(Protocol):
    def search(self, *, problem: dict[str, Any], queries: list[str], limit: int) -> list[LiteratureRecord]:
        ...


class LocalManualLiteratureBackend:
    """Deterministic local/manual literature seed.

    This avoids hardcoding network-only assumptions. Future arXiv, Semantic
    Scholar, Crossref, or local-file backends can implement the same protocol.
    """

    def search(
        self, *, problem: dict[str, Any], queries: list[str], limit: int
    ) -> list[LiteratureRecord]:
        slug = str(problem.get("slug") or "").lower()
        query_blob = " ".join(queries).lower()
        if "collatz" not in slug and "collatz" not in query_blob and "3n" not in query_blob:
            return []
        seeds = [
            LiteratureRecord(
                source_type="manual",
                title="The 3x+1 problem and its generalizations",
                authors=["Jeffrey C. Lagarias"],
                year=1985,
                venue="American Mathematical Monthly",
                abstract_md=(
                    "A survey-style reference point for the Collatz problem, useful for "
                    "terminology, known reductions, and avoiding fake novelty."
                ),
                extracts=[
                    {
                        "extract_kind": "terminology",
                        "title": "Collatz survey baseline",
                        "body_md": "Use the survey baseline to label rediscovered 3x+1 routes as prior art instead of novelty.",
                        "formal_hint": "Treat survey facts as literature constraints, not Lean obligations.",
                        "tags": ["collatz", "survey", "prior_art"],
                        "relevance_score": 0.82,
                    }
                ],
            ),
            LiteratureRecord(
                source_type="manual",
                title="The set of rational cycles for the 3x+1 problem",
                authors=["Jeffrey C. Lagarias"],
                year=1990,
                venue="Acta Arithmetica",
                abstract_md=(
                    "A reference point around cycle structure and rational extensions, "
                    "useful when Lima invents completion or cycle-obstruction worlds."
                ),
                extracts=[
                    {
                        "extract_kind": "warning",
                        "title": "Cycle language is not automatically new",
                        "body_md": "Cycle and extension-based universes should be checked against known rational or generalized cycle literature before claiming novelty.",
                        "formal_hint": "Compile only narrow cycle-obstruction claims.",
                        "tags": ["collatz", "cycles", "prior_art"],
                        "relevance_score": 0.68,
                    }
                ],
            ),
            LiteratureRecord(
                source_type="mathlib",
                title="Mathlib number theory and dynamics primitives",
                authors=["mathlib community"],
                venue="Lean/mathlib",
                url="https://github.com/leanprover-community/mathlib4",
                abstract_md=(
                    "Local formalization landing zone for finite checks, parity lemmas, "
                    "integer arithmetic, and exact algebraic obligations."
                ),
                extracts=[
                    {
                        "extract_kind": "method",
                        "title": "Prefer narrow Lean obligations",
                        "body_md": "Bridge lemmas should be small enough to state as integer arithmetic, parity, residue, quotient, or recurrence obligations.",
                        "formal_hint": "Use Nat/Int arithmetic and modular residues before encoding a whole universe.",
                        "tags": ["lean", "mathlib", "formalization"],
                        "relevance_score": 0.74,
                    }
                ],
            ),
        ]
        return seeds[: max(0, limit)]


def build_literature_queries(
    problem: dict[str, Any],
    pressure_map: dict[str, Any],
    universes: list[LimaUniverseSpec] | None = None,
) -> list[str]:
    queries: list[str] = []
    title = str(problem.get("title") or problem.get("slug") or "")
    if title:
        queries.append(title)
    tensions = pressure_map.get("tensions") if isinstance(pressure_map, dict) else []
    if isinstance(tensions, list):
        for tension in tensions[:6]:
            queries.append(f"{title} {tension}")
    for universe in universes or []:
        queries.append(f"{title} {universe.branch_of_math} {universe.family_key}")
        queries.extend(universe.literature_queries[:4])
    unique: list[str] = []
    seen: set[str] = set()
    for query in queries:
        q = " ".join(str(query).split())
        key = q.lower()
        if q and key not in seen:
            unique.append(q[:300])
            seen.add(key)
    return unique[:12]


def refresh_literature(
    lima_db: LimaDatabase,
    *,
    problem: dict[str, Any],
    pressure_map: dict[str, Any],
    universes: list[LimaUniverseSpec] | None = None,
    backend: LiteratureBackend | None = None,
) -> dict[str, Any]:
    backend = backend or LocalManualLiteratureBackend()
    queries = build_literature_queries(problem, pressure_map, universes)
    records = backend.search(
        problem=problem,
        queries=queries,
        limit=int(app_config.LIMA_MAX_LITERATURE_RESULTS),
    )
    inserted: list[str] = []
    for record in records:
        inserted.append(
            lima_db.insert_literature_source(
                problem_id=str(problem["id"]),
                source_type=record.source_type,
                title=record.title,
                authors=record.authors,
                year=record.year,
                venue=record.venue,
                doi=record.doi,
                arxiv_id=record.arxiv_id,
                url=record.url,
                abstract_md=record.abstract_md,
                bibtex=record.bibtex,
                extracts=record.extracts,
            )
        )
    return {
        "queries": queries,
        "inserted_source_ids": inserted,
        "source_count": len(inserted),
        "backend": backend.__class__.__name__,
    }


def infer_literature_relation(universe: LimaUniverseSpec, source: dict[str, Any]) -> str:
    blob = " ".join(
        [
            universe.title,
            universe.family_key,
            universe.branch_of_math,
            universe.solved_world,
            str(source.get("title") or ""),
            str(source.get("abstract_md") or ""),
        ]
    ).lower()
    if "prior" in blob or "survey" in blob or "generalization" in blob:
        return "prior_art"
    if "lean" in blob or "mathlib" in blob:
        return "bridge_tool"
    if slugify(universe.family_key) and slugify(universe.family_key).replace("_", " ") in blob:
        return "support"
    return "terminology"
