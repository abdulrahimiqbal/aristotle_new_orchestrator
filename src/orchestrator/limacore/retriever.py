from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .db import LimaCoreDB
from .models import DeltaProposal, GroundingBundle, ProblemSpec


class LiteratureProvider(Protocol):
    def search(self, problem: ProblemSpec, delta: DeltaProposal) -> list[dict]:
        ...


@dataclass(slots=True)
class LocalLiteratureProvider:
    corpus: dict[str, list[dict]]

    def search(self, problem: ProblemSpec, delta: DeltaProposal) -> list[dict]:
        rows = list(self.corpus.get(problem.slug, []))
        if delta.world_packet:
            rows.extend(self.corpus.get(delta.world_packet.family_key, []))
        return rows[:3]


class Retriever:
    def __init__(self, db: LimaCoreDB, *, literature_provider: LiteratureProvider | None = None) -> None:
        self.db = db
        self.literature_provider = literature_provider or LocalLiteratureProvider(
            corpus={
                "inward-compression-conjecture": [
                    {"title": "Discrete convex descent notes", "note": "Local averaging and canonical profiles"},
                    {"title": "Termination via energy on rewrite systems", "note": "Convex measures certify termination"},
                ],
                "balancing_world": [
                    {"title": "Balancing and abelian terminal forms", "note": "Fixed sum implies unique balanced limit"},
                ],
                "collatz": [
                    {"title": "Accelerated Collatz maps", "note": "Odd-step quotients can hide failure modes"},
                ],
            }
        )

    def build_grounding_bundle(self, problem: ProblemSpec, delta: DeltaProposal) -> GroundingBundle:
        formal = self._formal_analogs(problem, delta)[:3]
        literature = self.literature_provider.search(problem, delta)[:3]
        internal = self._internal_analogs(problem, delta)[:3]
        return GroundingBundle(
            formal_analogs=formal,
            literature_analogs=literature,
            internal_analogs=internal,
        )

    def _formal_analogs(self, problem: ProblemSpec, delta: DeltaProposal) -> list[dict]:
        frontier = self.db.get_frontier_nodes(problem.id)
        analogs = [
            {
                "source": "internal_frontier",
                "node_key": node["node_key"],
                "statement": node["statement_md"],
            }
            for node in frontier
            if node["node_key"] != delta.target_node_key
        ]
        if problem.slug == "inward-compression-conjecture":
            analogs.append(
                {
                    "source": "local_formal_corpus",
                    "node_key": "measure_descent",
                    "statement": "Strict descent of a natural-valued energy proves termination.",
                }
            )
            analogs.append(
                {
                    "source": "local_formal_corpus",
                    "node_key": "unique_normal_form",
                    "statement": "Unique normal form follows from invariant + descent + balanced characterization.",
                }
            )
        return analogs[:3]

    def _internal_analogs(self, problem: ProblemSpec, delta: DeltaProposal) -> list[dict]:
        worlds = self.db.list_world_heads(problem.id)
        fractures = self.db.list_fracture_heads(problem.id)
        recent = self.db.list_events(problem.id, limit=6)
        analogs: list[dict] = []
        analogs.extend({"kind": "world", "family_key": row["family_key"], "world_name": row["world_name"]} for row in worlds[:2])
        analogs.extend({"kind": "fracture", "family_key": row["family_key"], "failure_type": row["failure_type"]} for row in fractures[:2])
        analogs.extend({"kind": "event", "event_type": row["event_type"], "decision": row["decision"]} for row in recent[:2])
        return analogs[:3]
