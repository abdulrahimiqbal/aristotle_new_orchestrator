"""Problem map seeding, parsing, move-kind normalization, and progress summaries."""

from __future__ import annotations

import json
from typing import Any

ALLOWED_MOVE_KINDS: frozenset[str] = frozenset(
    {
        "prove",
        "underspecify",
        "perturb",
        "promote",
        "reformulate",
        "center",
        "refute",
        "explore",
    }
)

# Semantic category for each problem-map node (cartography, not proof status).
ALLOWED_NODE_KINDS: frozenset[str] = frozenset(
    {
        "claim",
        "hypothesis",
        "finite_check",
        "literature_anchor",
        "obstruction",
        "exploration",
        "equivalence",
    }
)


def normalize_move_kind(value: str | None) -> str:
    v = (value or "prove").strip().lower()
    return v if v in ALLOWED_MOVE_KINDS else "explore"


def normalize_node_kind(value: str | None) -> str:
    v = (value or "claim").strip().lower()
    return v if v in ALLOWED_NODE_KINDS else "claim"


def seed_problem_map_json(prompt: str) -> str:
    summary = (prompt or "").strip()[:800]
    if not summary:
        summary = "Research campaign; map will refine as experiments complete."
    obj = {
        "summary": summary,
        "nodes": [
            {
                "id": "root",
                "label": "Main prompt / conjecture",
                "status": "open",
                "kind": "claim",
            }
        ],
        "edges": [],
        "active_fronts": ["root"],
        "last_tick_updated": -1,
    }
    return json.dumps(obj, ensure_ascii=False)


def parse_problem_map(raw: str | None) -> dict[str, Any]:
    if not raw or not str(raw).strip():
        return {}
    try:
        d = json.loads(raw)
        return d if isinstance(d, dict) else {}
    except json.JSONDecodeError:
        return {}


def parse_problem_refs(raw: str | None) -> dict[str, Any]:
    if not raw or not str(raw).strip():
        return {}
    try:
        d = json.loads(raw)
        return d if isinstance(d, dict) else {}
    except json.JSONDecodeError:
        return {}


def problem_refs_to_json(
    *,
    erdos_id: str = "",
    source_url: str = "",
    formal_lean_path: str = "",
    notes: str = "",
) -> str:
    obj: dict[str, Any] = {}
    if erdos_id.strip():
        obj["erdos_id"] = erdos_id.strip()
    if source_url.strip():
        obj["source_url"] = source_url.strip()
    if formal_lean_path.strip():
        obj["formal_lean_path"] = formal_lean_path.strip()
    if notes.strip():
        obj["notes"] = notes.strip()
    return json.dumps(obj, ensure_ascii=False)


def map_needs_init(parsed: dict[str, Any]) -> bool:
    nodes = parsed.get("nodes")
    if not isinstance(nodes, list) or len(nodes) == 0:
        return True
    return False


def map_progress_stats(parsed: dict[str, Any]) -> dict[str, Any]:
    nodes = parsed.get("nodes") or []
    if not isinstance(nodes, list):
        nodes = []
    counts: dict[str, int] = {
        "proved": 0,
        "refuted": 0,
        "blocked": 0,
        "open": 0,
        "active": 0,
    }
    known = set(counts.keys())
    for n in nodes:
        if not isinstance(n, dict):
            continue
        st = str(n.get("status", "open")).lower()
        if st in known:
            counts[st] += 1
        else:
            counts["open"] += 1
    total = sum(1 for n in nodes if isinstance(n, dict))
    kind_counts: dict[str, int] = {k: 0 for k in sorted(ALLOWED_NODE_KINDS)}
    for n in nodes:
        if not isinstance(n, dict):
            continue
        nk = normalize_node_kind(n.get("kind"))
        kind_counts[nk] = kind_counts.get(nk, 0) + 1
    fronts = parsed.get("active_fronts") or []
    if not isinstance(fronts, list):
        fronts = []
    resolved = counts["proved"] + counts["refuted"]
    progress_pct = int(100 * resolved / total) if total else 0
    return {
        "counts": counts,
        "kind_counts": kind_counts,
        "total_nodes": total,
        "active_fronts": [str(x) for x in fronts if x is not None],
        "summary": str(parsed.get("summary") or "")[:2000],
        "resolved_nodes": resolved,
        "progress_percent": min(100, progress_pct),
        "last_tick_updated": parsed.get("last_tick_updated", -1),
    }


def coerce_llm_problem_map(
    data: dict[str, Any],
    *,
    previous: dict[str, Any],
    tick_number: int,
) -> dict[str, Any]:
    """Merge model output into a safe problem_map dict."""
    prev_summary = str(previous.get("summary") or "")
    summary = str(data.get("summary") or prev_summary)[:4000]

    nodes_in = data.get("nodes")
    if not isinstance(nodes_in, list):
        nodes_in = previous.get("nodes") if isinstance(previous.get("nodes"), list) else []
    nodes: list[dict[str, Any]] = []
    for n in nodes_in[:40]:
        if not isinstance(n, dict):
            continue
        nid = str(n.get("id") or "").strip()[:120]
        if not nid:
            continue
        label = str(n.get("label") or nid)[:500]
        st = str(n.get("status", "open")).lower()[:32]
        kind = normalize_node_kind(n.get("kind"))
        nodes.append({"id": nid, "label": label, "status": st, "kind": kind})
    if not nodes and isinstance(previous.get("nodes"), list):
        for n in previous["nodes"][:40]:
            if isinstance(n, dict) and n.get("id"):
                nodes.append(
                    {
                        "id": str(n["id"])[:120],
                        "label": str(n.get("label", n["id"]))[:500],
                        "status": str(n.get("status", "open")).lower()[:32],
                        "kind": normalize_node_kind(n.get("kind")),
                    }
                )
    if not nodes:
        nodes = [
            {
                "id": "root",
                "label": "Main prompt / conjecture",
                "status": "open",
                "kind": "claim",
            }
        ]

    edges_in = data.get("edges")
    if not isinstance(edges_in, list):
        edges_in = previous.get("edges") if isinstance(previous.get("edges"), list) else []
    edges: list[dict[str, Any]] = []
    for e in edges_in[:80]:
        if not isinstance(e, dict):
            continue
        a, b = str(e.get("from", "")).strip()[:120], str(e.get("to", "")).strip()[:120]
        if not a or not b:
            continue
        k = str(e.get("kind", "relates")).strip()[:64]
        edges.append({"from": a, "to": b, "kind": k})

    fronts_in = data.get("active_fronts")
    if not isinstance(fronts_in, list):
        fronts_in = (
            previous.get("active_fronts")
            if isinstance(previous.get("active_fronts"), list)
            else ["root"]
        )
    active_fronts = [str(x).strip()[:120] for x in fronts_in[:12] if x is not None]
    if not active_fronts:
        active_fronts = ["root"]

    # Preserve Mathlib reconnaissance payload unless the model explicitly replaces it.
    lib_anchors = data.get("library_anchors")
    if not isinstance(lib_anchors, list):
        prev_la = previous.get("library_anchors")
        lib_anchors = prev_la if isinstance(prev_la, list) else []
    lib_done = data.get("library_recon_done")
    if not isinstance(lib_done, bool):
        prev_ld = previous.get("library_recon_done")
        lib_done = prev_ld if isinstance(prev_ld, bool) else False

    out: dict[str, Any] = {
        "summary": summary,
        "nodes": nodes,
        "edges": edges,
        "active_fronts": active_fronts,
        "last_tick_updated": int(tick_number),
        "library_anchors": lib_anchors,
        "library_recon_done": lib_done,
    }
    return out
