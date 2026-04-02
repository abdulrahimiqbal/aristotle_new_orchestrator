"""Mathlib / LeanSearch knowledge adapter: broad reconnaissance + narrow symbol search."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

import httpx

from orchestrator import config as app_config

logger = logging.getLogger("orchestrator.mathlib_knowledge")

# Dotted Lean-like names (e.g. Mathlib.Topology.ContinuousMap, Foo.bar)
_DOT_NAME = re.compile(
    r"(?<![\w.])([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)+)(?![\w.])"
)
# Backtick-delimited fragments in errors
_BACKTICK = re.compile(r"`([^`\n]{2,120})`")


@dataclass(frozen=True)
class LeanSearchHit:
    """One result row from leansearch.net (shape aligned with LeanSearchClient)."""

    name: str
    module: str
    kind: str
    type_text: str
    informal_name: str
    informal_description: str
    doc_url: str
    distance: float


def extract_lean_symbols(text: str, *, max_symbols: int = 32) -> list[str]:
    """Pull candidate declaration paths / symbols from Aristotle errors and structured text."""
    if max_symbols <= 0:
        return []
    if not text or not text.strip():
        return []
    seen: set[str] = set()
    out: list[str] = []
    for m in _BACKTICK.finditer(text):
        s = m.group(1).strip()
        if "." in s and 2 < len(s) < 200:
            if s not in seen:
                seen.add(s)
                out.append(s)
        if len(out) >= max_symbols:
            return out
    for m in _DOT_NAME.finditer(text):
        s = m.group(1).strip()
        if len(s) < 4 or s.count(".") < 1:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
        if len(out) >= max_symbols:
            break
    return out


def _join_name(name_val: Any) -> str:
    if isinstance(name_val, list):
        return ".".join(str(x) for x in name_val if x is not None)
    if isinstance(name_val, str):
        return name_val
    return ""


def _join_module(mod: Any) -> str:
    if isinstance(mod, list):
        return ".".join(str(x) for x in mod if x is not None)
    if isinstance(mod, str):
        return mod
    return ""


def _parse_leansearch_response(body: Any) -> list[LeanSearchHit]:
    """Parse JSON from POST https://leansearch.net/search (LeanSearchClient-compatible)."""
    hits: list[LeanSearchHit] = []
    if not isinstance(body, list) or not body:
        return hits
    inner = body[0] if isinstance(body[0], list) else body
    if not isinstance(inner, list):
        return hits
    for item in inner:
        if not isinstance(item, dict):
            continue
        dist = float(item.get("distance", 0.0) or 0.0)
        res = item.get("result")
        if not isinstance(res, dict):
            continue
        name = _join_name(res.get("name"))
        mod = _join_module(res.get("module_name"))
        kind = str(res.get("kind") or "")
        type_text = str(res.get("type") or res.get("signature") or "")
        inf_name = str(res.get("informal_name") or "")
        inf_desc = str(res.get("informal_description") or "")
        doc_url = str(res.get("doc_url") or "")
        if not name and not inf_name:
            continue
        hits.append(
            LeanSearchHit(
                name=name or inf_name,
                module=mod,
                kind=kind,
                type_text=type_text[:1200],
                informal_name=inf_name,
                informal_description=inf_desc[:1500],
                doc_url=doc_url,
                distance=dist,
            )
        )
    return hits


async def leansearch_query(
    query: str,
    *,
    num_results: int = 6,
    timeout: float = 45.0,
) -> list[LeanSearchHit]:
    """Call LeanSearch HTTP API (same contract as LeanSearchClient)."""
    url = app_config.LEANSEARCH_API_URL.strip()
    if not url:
        return []
    q = query.strip()
    if not q:
        return []
    payload = {"query": [q], "num_results": max(1, min(20, num_results))}
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": app_config.LEANSEARCH_USER_AGENT,
    }
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.post(url, json=payload, headers=headers)
            r.raise_for_status()
            data = r.json()
    except (httpx.HTTPError, json.JSONDecodeError, TypeError) as e:
        logger.warning("LeanSearch request failed: %s", e)
        return []
    return _parse_leansearch_response(data)


def hits_to_markdown(
    hits: list[LeanSearchHit],
    *,
    heading: str,
    max_chars: int,
) -> str:
    if not hits:
        return ""
    lines: list[str] = [heading, f"(source: LeanSearch; toolchain hint: {app_config.LEAN_TOOLCHAIN_HINT or 'unknown'})"]
    used = 0
    for h in hits:
        block = f"- **{h.name}**"
        if h.module:
            block += f" — `{h.module}`"
        if h.kind:
            block += f" [{h.kind}]"
        block += "\n"
        if h.informal_name:
            block += f"  - informal: {h.informal_name[:400]}\n"
        if h.informal_description:
            block += f"  - {h.informal_description[:600]}\n"
        elif h.type_text:
            block += f"  - type: {h.type_text[:500]}\n"
        if h.doc_url:
            block += f"  - doc: {h.doc_url}\n"
        if used + len(block) > max_chars:
            break
        lines.append(block.rstrip())
        used += len(block)
    return "\n".join(lines) + "\n"


def build_broad_queries(campaign_prompt: str, target_descriptions: list[str]) -> list[str]:
    """1–3 natural-language queries from prompt + target blurbs."""
    n = max(1, min(3, app_config.MATHLIB_BROAD_QUERIES_COUNT))
    parts: list[str] = []
    p = (campaign_prompt or "").strip()
    if p:
        parts.append(p[:500])
    for d in target_descriptions[:4]:
        ds = (d or "").strip()
        if ds:
            parts.append(ds[:350])
    if not parts:
        return []
    queries: list[str] = []
    if n >= 1 and parts:
        queries.append(parts[0][:400])
    if n >= 2 and len(parts) > 1:
        queries.append(parts[1][:400])
    if n >= 3 and len(parts) > 2:
        queries.append(parts[2][:400])
    # Dedupe while preserving order
    seen: set[str] = set()
    out: list[str] = []
    for q in queries:
        k = q.lower().strip()
        if k and k not in seen:
            seen.add(k)
            out.append(q.strip())
    return out[:n]


async def fetch_broad_reconnaissance(
    campaign_prompt: str,
    target_descriptions: list[str],
) -> list[dict[str, Any]]:
    """Run broad LeanSearch queries; return structured anchors for problem_map_json."""
    queries = build_broad_queries(campaign_prompt, target_descriptions)
    per_q = max(1, min(8, app_config.MATHLIB_BROAD_RESULTS_PER_QUERY))
    anchors: list[dict[str, Any]] = []
    for q in queries:
        hits = await leansearch_query(q, num_results=per_q)
        anchors.append(
            {
                "query": q,
                "hits": [
                    {
                        "name": h.name,
                        "module": h.module,
                        "kind": h.kind,
                        "informal_name": h.informal_name,
                        "informal_description": h.informal_description[:800],
                        "doc_url": h.doc_url,
                    }
                    for h in hits
                ],
            }
        )
    return anchors


async def fetch_narrow_hints_for_state(
    *,
    manager_context_experiments: list[dict[str, Any]],
    manager_context_ledger: list[dict[str, Any]],
) -> str:
    """Symbol-first LeanSearch from recent structured experiments + ledger labels."""
    if app_config.MATHLIB_NARROW_MAX_SYMBOLS <= 0:
        return ""
    blob_parts: list[str] = []
    for row in manager_context_experiments:
        for k in (
            "blockers",
            "unsolved_goals",
            "generated_lemmas",
            "error_message",
        ):
            v = row.get(k)
            if isinstance(v, str) and v.strip():
                blob_parts.append(v)
            elif isinstance(v, list):
                blob_parts.extend(str(x) for x in v[:12] if x)
    for row in manager_context_ledger[:30]:
        lab = row.get("label")
        if isinstance(lab, str) and lab.strip():
            blob_parts.append(lab)
    blob = "\n".join(blob_parts)
    symbols = extract_lean_symbols(blob, max_symbols=app_config.MATHLIB_NARROW_MAX_SYMBOLS)
    if not symbols:
        return ""
    per = max(1, min(5, app_config.MATHLIB_NARROW_RESULTS_PER_SYMBOL))
    max_chars = max(500, app_config.MATHLIB_CONTEXT_MAX_CHARS // 2)
    sections: list[str] = []
    for sym in symbols[: app_config.MATHLIB_NARROW_MAX_SYMBOLS]:
        hits = await leansearch_query(sym, num_results=per)
        md = hits_to_markdown(
            hits,
            heading=f"### Narrow search: `{sym[:80]}`",
            max_chars=max_chars,
        )
        if md:
            sections.append(md)
    return "\n".join(sections).strip()


def format_library_anchors_markdown(anchors: list[dict[str, Any]]) -> str:
    """Turn stored library_anchors JSON into a short markdown section."""
    if not anchors:
        return ""
    lines: list[str] = [
        "### Mathlib library anchors (broad reconnaissance — LeanSearch)",
        f"(toolchain hint: {app_config.LEAN_TOOLCHAIN_HINT or 'set LEAN_TOOLCHAIN_HINT to match your Lake pin'})",
    ]
    for block in anchors[:6]:
        if not isinstance(block, dict):
            continue
        q = str(block.get("query") or "")
        if q:
            lines.append(f"- Query: {q[:400]}")
        for h in block.get("hits") or []:
            if not isinstance(h, dict):
                continue
            nm = str(h.get("name") or h.get("informal_name") or "")
            inf = str(h.get("informal_name") or "")
            desc = str(h.get("informal_description") or "")[:400]
            mod = str(h.get("module") or "")
            if nm:
                line = f"  - {nm}"
                if mod:
                    line += f" (`{mod}`)"
                lines.append(line)
                if desc:
                    lines.append(f"    {desc}")
        lines.append("")
    text = "\n".join(lines).strip()
    cap = app_config.MATHLIB_CONTEXT_MAX_CHARS
    if len(text) > cap:
        return text[: cap - 1] + "…"
    return text
