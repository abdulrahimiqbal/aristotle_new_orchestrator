from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import re
from typing import Any, Protocol
import xml.etree.ElementTree as ET

import httpx

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


class LocalFileLiteratureBackend:
    source_name = "local_file"

    def __init__(self, root: str | None = None) -> None:
        self.root = Path(root or app_config.LIMA_LITERATURE_LOCAL_DIR)

    def search(
        self, *, problem: dict[str, Any], queries: list[str], limit: int
    ) -> list[LiteratureRecord]:
        if not self.root or not self.root.exists() or not self.root.is_dir():
            return []
        query_blob = " ".join([str(problem.get("slug") or ""), str(problem.get("title") or ""), *queries]).lower()
        records: list[LiteratureRecord] = []
        for path in sorted(self.root.rglob("*")):
            if len(records) >= limit:
                break
            if path.suffix.lower() not in {".json", ".md", ".markdown", ".txt"}:
                continue
            try:
                text = path.read_text(encoding="utf-8")[:20000]
            except OSError:
                continue
            if query_blob and not _rough_match(query_blob, text.lower() + " " + path.name.lower()):
                continue
            record = _record_from_local_file(path, text)
            if record:
                records.append(record)
        return records[:limit]


class CompositeLiteratureBackend:
    def __init__(self, backends: list[LiteratureBackend]) -> None:
        self.backends = backends or [LocalManualLiteratureBackend()]

    def search(
        self, *, problem: dict[str, Any], queries: list[str], limit: int
    ) -> list[LiteratureRecord]:
        out: list[LiteratureRecord] = []
        seen: set[str] = set()
        for backend in self.backends:
            if len(out) >= limit:
                break
            try:
                records = backend.search(
                    problem=problem, queries=queries, limit=max(0, limit - len(out))
                )
            except Exception:
                records = []
            for record in records:
                key = (
                    record.doi.lower()
                    or record.arxiv_id.lower()
                    or re.sub(r"\s+", " ", record.title.lower()).strip()
                )
                if not key or key in seen:
                    continue
                out.append(record)
                seen.add(key)
                if len(out) >= limit:
                    break
        return out[:limit]


class HttpLiteratureBackend:
    source_name = "http"

    def __init__(self, *, timeout: float | None = None) -> None:
        self.timeout = timeout or float(app_config.LIMA_LITERATURE_HTTP_TIMEOUT_SEC)

    def _get(self, url: str, **kwargs: Any) -> httpx.Response:
        response = httpx.get(url, timeout=self.timeout, **kwargs)
        response.raise_for_status()
        return response


class ArxivLiteratureBackend(HttpLiteratureBackend):
    source_name = "arxiv"
    endpoint = "https://export.arxiv.org/api/query"

    def search(
        self, *, problem: dict[str, Any], queries: list[str], limit: int
    ) -> list[LiteratureRecord]:
        records: list[LiteratureRecord] = []
        for query in queries[:3]:
            if len(records) >= limit:
                break
            response = self._get(
                self.endpoint,
                params={
                    "search_query": f"all:{query}",
                    "start": 0,
                    "max_results": max(1, min(limit - len(records), 5)),
                    "sortBy": "relevance",
                    "sortOrder": "descending",
                },
                headers={"User-Agent": _user_agent()},
            )
            records.extend(_parse_arxiv_atom(response.text, limit - len(records)))
        return records[:limit]


class SemanticScholarLiteratureBackend(HttpLiteratureBackend):
    source_name = "semantic_scholar"
    endpoint = "https://api.semanticscholar.org/graph/v1/paper/search"

    def search(
        self, *, problem: dict[str, Any], queries: list[str], limit: int
    ) -> list[LiteratureRecord]:
        records: list[LiteratureRecord] = []
        headers = {"User-Agent": _user_agent()}
        if app_config.LIMA_SEMANTIC_SCHOLAR_API_KEY:
            headers["x-api-key"] = app_config.LIMA_SEMANTIC_SCHOLAR_API_KEY
        for query in queries[:3]:
            if len(records) >= limit:
                break
            response = self._get(
                self.endpoint,
                params={
                    "query": query,
                    "limit": max(1, min(limit - len(records), 10)),
                    "fields": "title,authors,year,venue,abstract,url,externalIds",
                },
                headers=headers,
            )
            payload = response.json()
            for item in payload.get("data") or []:
                if not isinstance(item, dict):
                    continue
                external = item.get("externalIds") if isinstance(item.get("externalIds"), dict) else {}
                title = str(item.get("title") or "").strip()
                if not title:
                    continue
                records.append(
                    LiteratureRecord(
                        source_type="semantic_scholar",
                        title=title,
                        authors=[
                            str(author.get("name") or "")
                            for author in item.get("authors") or []
                            if isinstance(author, dict) and author.get("name")
                        ],
                        year=_int_or_none(item.get("year")),
                        venue=str(item.get("venue") or ""),
                        doi=str(external.get("DOI") or ""),
                        arxiv_id=str(external.get("ArXiv") or ""),
                        url=str(item.get("url") or ""),
                        abstract_md=str(item.get("abstract") or ""),
                        bibtex={"externalIds": external, "paperId": item.get("paperId")},
                        extracts=[_method_extract(title, item.get("abstract"), "semantic_scholar")],
                    )
                )
                if len(records) >= limit:
                    break
        return records[:limit]


class CrossrefLiteratureBackend(HttpLiteratureBackend):
    source_name = "crossref"
    endpoint = "https://api.crossref.org/works"

    def search(
        self, *, problem: dict[str, Any], queries: list[str], limit: int
    ) -> list[LiteratureRecord]:
        records: list[LiteratureRecord] = []
        headers = {"User-Agent": _user_agent()}
        for query in queries[:3]:
            if len(records) >= limit:
                break
            response = self._get(
                self.endpoint,
                params={"query": query, "rows": max(1, min(limit - len(records), 10))},
                headers=headers,
            )
            payload = response.json()
            message = payload.get("message") if isinstance(payload, dict) else {}
            for item in message.get("items") or []:
                if not isinstance(item, dict):
                    continue
                titles = item.get("title") or []
                title = str(titles[0] if titles else "").strip()
                if not title:
                    continue
                published = item.get("published-print") or item.get("published-online") or {}
                records.append(
                    LiteratureRecord(
                        source_type="crossref",
                        title=title,
                        authors=_crossref_authors(item.get("author") or []),
                        year=_crossref_year(published),
                        venue=str((item.get("container-title") or [""])[0] or ""),
                        doi=str(item.get("DOI") or ""),
                        url=str(item.get("URL") or ""),
                        abstract_md=_strip_markup(str(item.get("abstract") or "")),
                        bibtex={
                            "publisher": item.get("publisher"),
                            "type": item.get("type"),
                            "score": item.get("score"),
                        },
                        extracts=[_method_extract(title, item.get("abstract"), "crossref")],
                    )
                )
                if len(records) >= limit:
                    break
        return records[:limit]


def make_literature_backend(selection: str | None = None) -> LiteratureBackend:
    raw = (selection or app_config.LIMA_LITERATURE_BACKENDS or "local").strip().lower()
    if raw in {"configured", "default"}:
        raw = app_config.LIMA_LITERATURE_BACKENDS.strip().lower() or "local"
    names = [name.strip() for name in raw.split(",") if name.strip()]
    if "all" in names:
        names = ["local", "local_file", "arxiv", "semantic_scholar", "crossref"]
    backends: list[LiteratureBackend] = []
    for name in names:
        if name == "local":
            backends.append(LocalManualLiteratureBackend())
        elif name == "arxiv":
            if app_config.LIMA_ENABLE_ARXIV_BACKEND:
                backends.append(ArxivLiteratureBackend())
        elif name in {"semantic", "semantic_scholar", "semanticscholar"}:
            if app_config.LIMA_ENABLE_SEMANTIC_SCHOLAR_BACKEND:
                backends.append(SemanticScholarLiteratureBackend())
        elif name == "crossref":
            if app_config.LIMA_ENABLE_CROSSREF_BACKEND:
                backends.append(CrossrefLiteratureBackend())
        elif name in {"local_file", "file", "files"}:
            backends.append(LocalFileLiteratureBackend())
    return CompositeLiteratureBackend(backends or [LocalManualLiteratureBackend()])


def build_literature_queries(
    problem: dict[str, Any],
    pressure_map: dict[str, Any],
    universes: list[LimaUniverseSpec] | None = None,
) -> list[str]:
    queries: list[str] = []
    title = str(problem.get("title") or problem.get("slug") or "")
    seed = _load_seed(problem)
    routing = seed.get("routing_policy") if isinstance(seed.get("routing_policy"), dict) else {}
    for keyword in routing.get("retrieval_keywords") or seed.get("retrieval_keywords") or []:
        queries.append(str(keyword))
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
    backend_selection: str | None = None,
) -> dict[str, Any]:
    backend = backend or make_literature_backend(backend_selection)
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
        "backend_selection": backend_selection or app_config.LIMA_LITERATURE_BACKENDS,
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


def score_literature_novelty(universe: LimaUniverseSpec, source: dict[str, Any]) -> dict[str, Any]:
    blob = " ".join(
        [
            universe.title,
            universe.family_key,
            universe.branch_of_math,
            universe.solved_world,
        ]
    ).lower()
    source_blob = " ".join(
        [
            str(source.get("title") or ""),
            str(source.get("abstract_md") or ""),
            str(source.get("venue") or ""),
        ]
    ).lower()
    overlap_terms = [
        term
        for term in {"cycle", "quotient", "residue", "parity", "completion", "3x+1", "collatz"}
        if term in blob and term in source_blob
    ]
    score = min(1.0, 0.25 + 0.15 * len(overlap_terms))
    relation = "prior_art" if score >= 0.55 or "survey" in source_blob else infer_literature_relation(universe, source)
    return {
        "relation_kind": relation,
        "prior_art_score": score if relation == "prior_art" else min(score, 0.5),
        "overlap_terms": overlap_terms,
    }


def _user_agent() -> str:
    base = "aristotle-orchestrator-lima/0.1"
    if app_config.LIMA_CROSSREF_MAILTO:
        return f"{base} (mailto:{app_config.LIMA_CROSSREF_MAILTO})"
    return base


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _strip_markup(value: str) -> str:
    return re.sub(r"<[^>]+>", "", value or "").strip()


def _method_extract(title: str, body: Any, source: str) -> dict[str, Any]:
    return {
        "extract_kind": "method",
        "title": f"{source}: {title}"[:500],
        "body_md": _strip_markup(str(body or ""))[:4000],
        "formal_hint": "Use as literature grounding or prior-art pressure, not as a direct formal obligation.",
        "tags": [source, "literature"],
        "relevance_score": 0.5,
    }


def _load_seed(problem: dict[str, Any]) -> dict[str, Any]:
    raw = problem.get("seed_packet_json")
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _rough_match(query_blob: str, haystack: str) -> bool:
    terms = [term for term in re.split(r"[^a-z0-9+]+", query_blob.lower()) if len(term) >= 4]
    if not terms:
        return True
    return any(term in haystack for term in terms[:20])


def _record_from_local_file(path: Path, text: str) -> LiteratureRecord | None:
    if path.suffix.lower() == ".json":
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        extracts = payload.get("extracts") if isinstance(payload.get("extracts"), list) else []
        return LiteratureRecord(
            source_type="local_file",
            title=str(payload.get("title") or path.stem),
            authors=[str(a) for a in payload.get("authors") or []],
            year=_int_or_none(payload.get("year")),
            venue=str(payload.get("venue") or "local notes"),
            doi=str(payload.get("doi") or ""),
            arxiv_id=str(payload.get("arxiv_id") or ""),
            url=str(payload.get("url") or path.as_posix()),
            abstract_md=str(payload.get("abstract_md") or payload.get("abstract") or ""),
            bibtex={"path": path.as_posix(), "local_file": True},
            extracts=extracts or [_extract_from_text(path.stem, str(payload.get("abstract_md") or ""), "local_file")],
        )
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    title = lines[0].lstrip("# ").strip() if lines else path.stem
    body = "\n".join(lines[1:])[:8000] if len(lines) > 1 else text[:8000]
    return LiteratureRecord(
        source_type="local_file",
        title=title or path.stem,
        venue="local notes",
        url=path.as_posix(),
        abstract_md=body,
        bibtex={"path": path.as_posix(), "local_file": True},
        extracts=[_extract_from_text(title or path.stem, body, "local_file")],
    )


def _extract_from_text(title: str, body: str, source: str) -> dict[str, Any]:
    lowered = body.lower()
    if "counterexample" in lowered or "cannot" in lowered:
        kind = "negative_result"
    elif "theorem" in lowered:
        kind = "theorem"
    elif "lemma" in lowered:
        kind = "lemma"
    elif "terminology" in lowered or "definition" in lowered:
        kind = "terminology"
    elif "open" in lowered:
        kind = "open_problem"
    else:
        kind = "method"
    return {
        "extract_kind": kind,
        "title": f"{source}: {title}"[:500],
        "body_md": body[:4000],
        "formal_hint": "Use this local note as grounding, prior-art pressure, or terminology.",
        "tags": [source, kind],
        "relevance_score": 0.55,
    }


def _parse_arxiv_atom(text: str, limit: int) -> list[LiteratureRecord]:
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "arxiv": "http://arxiv.org/schemas/atom",
    }
    root = ET.fromstring(text)
    records: list[LiteratureRecord] = []
    for entry in root.findall("atom:entry", ns):
        title = " ".join((entry.findtext("atom:title", default="", namespaces=ns) or "").split())
        if not title:
            continue
        authors = [
            " ".join((author.findtext("atom:name", default="", namespaces=ns) or "").split())
            for author in entry.findall("atom:author", ns)
        ]
        url = entry.findtext("atom:id", default="", namespaces=ns) or ""
        arxiv_id = url.rsplit("/", 1)[-1] if url else ""
        published = entry.findtext("atom:published", default="", namespaces=ns) or ""
        records.append(
            LiteratureRecord(
                source_type="arxiv",
                title=title,
                authors=[a for a in authors if a],
                year=_int_or_none(published[:4]),
                venue="arXiv",
                arxiv_id=arxiv_id,
                url=url,
                abstract_md=" ".join(
                    (entry.findtext("atom:summary", default="", namespaces=ns) or "").split()
                ),
                bibtex={"published": published},
                extracts=[_method_extract(title, entry.findtext("atom:summary", default="", namespaces=ns), "arxiv")],
            )
        )
        if len(records) >= limit:
            break
    return records


def _crossref_authors(authors: list[Any]) -> list[str]:
    out: list[str] = []
    for author in authors:
        if not isinstance(author, dict):
            continue
        name = " ".join(
            part
            for part in (str(author.get("given") or ""), str(author.get("family") or ""))
            if part
        ).strip()
        if name:
            out.append(name)
    return out


def _crossref_year(published: dict[str, Any]) -> int | None:
    parts = published.get("date-parts") if isinstance(published, dict) else None
    if isinstance(parts, list) and parts and isinstance(parts[0], list) and parts[0]:
        return _int_or_none(parts[0][0])
    return None
