from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(slots=True)
class PromptParse:
    title: str
    slug: str
    statement_md: str
    normalized_statement_md: str
    domain: str
    research_brief_md: str
    preferences: list[str]


def slugify(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return cleaned or "lima-core-problem"


def infer_domain(prompt: str) -> str:
    lowered = prompt.lower()
    if any(word in lowered for word in ("collatz", "integer", "number theory", "prime", "divisor")):
        return "number theory"
    if any(word in lowered for word in ("graph", "rewrite", "state", "dynamics", "confluence", "sequence")):
        return "discrete dynamics"
    if any(word in lowered for word in ("operator", "convex", "energy", "analysis")):
        return "analysis"
    if any(word in lowered for word in ("geometry", "manifold", "metric")):
        return "geometry"
    return "mathematics"


def infer_title(prompt: str) -> str:
    first_sentence = re.split(r"(?<=[.!?])\s+", prompt.strip(), maxsplit=1)[0].strip()
    if not first_sentence:
        return "Untitled problem"
    trimmed = re.sub(r"^(prove|show|study|benchmark|attack)\s+", "", first_sentence, flags=re.I).strip()
    if len(trimmed) > 88:
        trimmed = trimmed[:85].rstrip(" ,.;:") + "..."
    if trimmed and trimmed[0].islower():
        trimmed = trimmed[0].upper() + trimmed[1:]
    return trimmed


def normalize_statement(prompt: str) -> str:
    text = " ".join(prompt.strip().split())
    if not text:
        return "Research problem."
    if not text.endswith((".", "!", "?")):
        text += "."
    return text


def extract_preferences(prompt: str) -> list[str]:
    lowered = prompt.lower()
    prefs: list[str] = []
    for token in (
        "prefer quotient worlds",
        "prefer odd-step",
        "prefer hidden-state worlds",
        "prefer coordinate lifts",
        "prefer balancing worlds",
        "prefer rewrite worlds",
    ):
        if token in lowered:
            prefs.append(token)
    for match in re.findall(r"prefer ([^.;]+)", lowered):
        pref = f"prefer {match.strip()}"
        if pref not in prefs:
            prefs.append(pref)
    return prefs[:5]


def normalize_problem_prompt(prompt: str) -> PromptParse:
    clean = prompt.strip()
    title = infer_title(clean)
    normalized = normalize_statement(clean)
    slug = slugify(title)
    research_brief = ""
    parts = re.split(r"(?<=[.!?])\s+", clean, maxsplit=1)
    if len(parts) > 1:
        research_brief = parts[1].strip()
    return PromptParse(
        title=title,
        slug=slug,
        statement_md=clean,
        normalized_statement_md=normalized,
        domain=infer_domain(clean),
        research_brief_md=research_brief,
        preferences=extract_preferences(clean),
    )
