from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from orchestrator.models import AristotleParsedResult, Verdict

PROJECT_ID_PATTERN = re.compile(
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
)
PROJECT_LINE_PATTERN = re.compile(
    r"^([0-9a-f-]{36})\s+([A-Z_]+)\s+",
    re.MULTILINE,
)

SUBPROCESS_TIMEOUT = 20 * 60  # 20 minutes
GHOST_JOB_TIMEOUT = 2 * 3600  # 2 hours
STALE_PROGRESS_TIMEOUT = 6 * 3600  # 6 hours

# Machine-readable result artifact (preferred over markdown heuristics).
STRUCTURED_JSON_NAMES = frozenset(
    {
        "aristotle_result.json",
        "aristotle-result.json",
    }
)

# Embedded in synthesized JSON so parse_result_json sets parse_source=markdown_derived.
RESULT_ORIGIN_ORCHESTRATOR_MARKDOWN_V1 = "orchestrator_markdown_v1"
_SUBMIT_EXCLUDE_PREFIXES = ("aristotle_result_",)
_SUBMIT_EXCLUDE_NAMES = frozenset({"__pycache__", ".pytest_cache"})


@dataclass(frozen=True)
class ExtractedArchive:
    """Content extracted from an Aristotle result archive."""

    markdown: str = ""
    structured_json_raw: str | None = None
    # Harmonic may return COMPLETE_WITH_ERRORS (e.g. objective parse trouble) while still shipping Lean.
    complete_with_errors: bool = False


def classify_failure(stdout: str, stderr: str) -> tuple[str, str]:
    text = (stdout + "\n" + stderr).lower()

    if "invalid api key" in text or "check your api key" in text:
        return "auth_error", "Aristotle rejected the API key"
    if "nodename nor servname" in text or "could not resolve host" in text:
        return "dns_failure", "Cannot resolve aristotle.harmonic.fun"
    if (
        "connecterror" in text
        or "connection refused" in text
        or "network is unreachable" in text
    ):
        return "network_error", "Cannot reach Aristotle API"
    if "lean-toolchain" in text or "no lean files" in text:
        return "workspace_error", "Missing Lean toolchain in workspace"
    if "permission denied" in text or "not found" in text:
        return "path_error", "CLI or required files not accessible"
    if "traceback" in text or "api error" in text:
        return "api_error", "Aristotle returned an error"

    return "unknown", "Non-zero exit code from Aristotle CLI"


def _clip_command_output(stdout: str, stderr: str, limit: int = 500) -> str:
    chunks = []
    for raw in (stderr, stdout):
        text = " ".join((raw or "").split()).strip()
        if text:
            chunks.append(text)
    merged = " | ".join(chunks)
    if len(merged) > limit:
        return merged[: limit - 1] + "…"
    return merged


def _ignore_submit_artifacts(_src: str, names: list[str]) -> set[str]:
    ignored: set[str] = set()
    for name in names:
        if name in _SUBMIT_EXCLUDE_NAMES:
            ignored.add(name)
            continue
        if any(name.startswith(prefix) for prefix in _SUBMIT_EXCLUDE_PREFIXES):
            ignored.add(name)
    return ignored


def _stage_project_dir_for_submit(project_dir: str) -> tempfile.TemporaryDirectory[str]:
    src = Path(project_dir).resolve()
    tmp = tempfile.TemporaryDirectory(prefix=f"aristotle-submit-{src.name}-")
    dst = Path(tmp.name) / src.name
    shutil.copytree(
        src,
        dst,
        ignore=_ignore_submit_artifacts,
        symlinks=False,
    )
    return tmp


def _age_seconds(submitted_at: str) -> float | None:
    if not submitted_at:
        return None
    try:
        dt = datetime.fromisoformat(submitted_at)
        return (datetime.utcnow() - dt).total_seconds()
    except ValueError:
        return None


def _safe_under_base(path: Path, base: Path) -> bool:
    try:
        return str(path.resolve()).startswith(str(base.resolve()))
    except OSError:
        return False


def _extract_archive_to_dir(archive_path: Path, extract_dir: Path) -> bool:
    if not archive_path.is_file():
        return False
    extract_dir.mkdir(parents=True, exist_ok=True)
    base = extract_dir.resolve()

    if zipfile.is_zipfile(archive_path):
        with zipfile.ZipFile(archive_path) as zf:
            for member in zf.infolist():
                if member.is_dir():
                    continue
                dest = (extract_dir / member.filename).resolve()
                if not str(dest).startswith(str(base)):
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(member, "r") as src, open(dest, "wb") as out:
                    out.write(src.read())
        return True
    if tarfile.is_tarfile(archive_path):
        with tarfile.open(archive_path) as tf:
            for member in tf.getmembers():
                if member.isdir():
                    continue
                dest = (extract_dir / member.name).resolve()
                if not str(dest).startswith(str(base)):
                    continue
                tf.extract(member, extract_dir)
        return True
    return False


def _read_summary_md(extract_dir: Path, base: Path) -> str:
    for path in extract_dir.rglob("*"):
        if not path.is_file():
            continue
        if not _safe_under_base(path, base):
            continue
        if "ARISTOTLE_SUMMARY" in path.name and path.suffix.lower() == ".md":
            return path.read_text(encoding="utf-8", errors="replace")
    return ""


def _read_structured_json(extract_dir: Path, base: Path) -> str | None:
    for path in extract_dir.rglob("*.json"):
        if not path.is_file():
            continue
        if not _safe_under_base(path, base):
            continue
        if path.name.lower() in STRUCTURED_JSON_NAMES:
            return path.read_text(encoding="utf-8", errors="replace")
    return None


def extract_archive(archive_path: Path) -> ExtractedArchive:
    """Extract result archive and read summary markdown + optional ARISTOTLE_RESULT JSON."""
    extract_dir = archive_path.with_suffix(".contents")
    if not _extract_archive_to_dir(archive_path, extract_dir):
        return ExtractedArchive()

    base = extract_dir.resolve()
    md = _read_summary_md(extract_dir, base)
    js = _read_structured_json(extract_dir, base)
    return ExtractedArchive(markdown=md, structured_json_raw=js)


def extract_and_read_summary(archive_path: Path) -> str:
    """Return ARISTOTLE_SUMMARY.md content only (legacy helper)."""
    return extract_archive(archive_path).markdown


def _normalize_str_list(val: Any) -> list[str]:
    if val is None:
        return []
    if isinstance(val, str):
        s = val.strip()
        return [s] if s else []
    if not isinstance(val, list):
        return []
    out: list[str] = []
    for item in val:
        if isinstance(item, str):
            s = item.strip()
            if s:
                out.append(s)
        elif isinstance(item, dict):
            chunk = None
            for key in ("label", "text", "name", "statement", "detail"):
                v = item.get(key)
                if v is not None and str(v).strip():
                    chunk = str(v).strip()
                    break
            if chunk:
                out.append(chunk)
    return out


def _parse_verdict_str(raw: str | None) -> Verdict | None:
    if not raw or not isinstance(raw, str):
        return None
    key = raw.strip().lower().replace("-", "_")
    mapping = {
        "proved": Verdict.PROVED,
        "partial": Verdict.PARTIAL,
        "disproved": Verdict.DISPROVED,
        "inconclusive": Verdict.INCONCLUSIVE,
        "inconclusive_result": Verdict.INCONCLUSIVE,
        "infra_error": Verdict.INFRA_ERROR,
        "infra": Verdict.INFRA_ERROR,
    }
    return mapping.get(key)


def parse_result_json(data: dict[str, Any]) -> tuple[AristotleParsedResult, list[str]]:
    """Parse schema v1 `aristotle_result.json` into AristotleParsedResult."""
    warnings: list[str] = []
    schema_version = data.get("schema_version")
    if schema_version is None:
        warnings.append("structured JSON missing schema_version; assuming 1")
        sv = 1
    else:
        try:
            sv = int(schema_version)
        except (TypeError, ValueError):
            warnings.append("structured JSON schema_version not an integer; assuming 1")
            sv = 1
    if sv > 1:
        warnings.append(
            f"structured JSON schema_version={sv} newer than supported (1); using v1 field mapping"
        )

    verdict = _parse_verdict_str(data.get("verdict"))
    if verdict is None and data.get("verdict") is not None:
        warnings.append(f"unknown verdict in JSON: {data.get('verdict')!r}")
        verdict = Verdict.INCONCLUSIVE

    proved = _normalize_str_list(data.get("proved_lemmas"))
    generated = _normalize_str_list(data.get("generated_lemmas"))
    unsolved = _normalize_str_list(data.get("unsolved_goals"))
    blockers = _normalize_str_list(data.get("blockers"))
    counterexamples = _normalize_str_list(data.get("counterexamples"))

    err = data.get("error") or data.get("error_message") or ""
    error_message = str(err).strip() if err is not None else ""

    summary_bits: list[str] = []
    if isinstance(data.get("summary"), str) and data["summary"].strip():
        summary_bits.append(data["summary"].strip()[:8000])

    origin = data.get("result_origin")
    if origin == RESULT_ORIGIN_ORCHESTRATOR_MARKDOWN_V1:
        parse_source = "markdown_derived"
    elif origin:
        warnings.append(f"unknown result_origin in JSON: {origin!r}")
        parse_source = "json"
    else:
        parse_source = "json"

    result = AristotleParsedResult(
        verdict=verdict or Verdict.INCONCLUSIVE,
        proved_lemmas=proved,
        generated_lemmas=generated,
        unsolved_goals=unsolved,
        blockers=blockers,
        counterexamples=counterexamples,
        error_message=error_message,
        summary_text="\n".join(summary_bits),
        parse_source=parse_source,
        parse_schema_version=sv,
        parse_warnings=warnings,
    )
    return result, warnings


def parse_experiment_result(
    markdown: str, structured_json_raw: str | None
) -> AristotleParsedResult:
    """Prefer machine-readable JSON; fall back to markdown heuristics."""
    md = markdown or ""
    if not (structured_json_raw and structured_json_raw.strip()):
        return parse_result(md)

    extra_warnings: list[str] = []
    try:
        parsed_obj = json.loads(structured_json_raw)
    except json.JSONDecodeError as e:
        extra_warnings.append(f"structured JSON decode error: {e}")
        fb = parse_result(md)
        fb.parse_warnings = list(fb.parse_warnings) + extra_warnings
        return fb

    if not isinstance(parsed_obj, dict):
        extra_warnings.append("structured JSON root is not an object; using markdown")
        fb = parse_result(md)
        fb.parse_warnings = list(fb.parse_warnings) + extra_warnings
        return fb

    parsed, _w = parse_result_json(parsed_obj)
    if md.strip():
        parsed.summary_text = md[:8000]
    elif not parsed.summary_text.strip():
        parsed.summary_text = structured_json_raw[:8000]
    return parsed


async def submit(objective: str, project_dir: str) -> tuple[str, str]:
    """Submit job to Aristotle. Returns (job_id, error_message). job_id is empty on failure."""
    if not os.environ.get("ARISTOTLE_API_KEY"):
        return "", "ARISTOTLE_API_KEY not set"

    project_dir = str(Path(project_dir).resolve())
    try:
        staging = _stage_project_dir_for_submit(project_dir)
    except (OSError, shutil.Error) as e:
        return "", f"Failed to prepare clean submit workspace: {e!s}"
    staged_project_dir = str((Path(staging.name) / Path(project_dir).name).resolve())
    command = ["aristotle", "submit", objective, "--project-dir", staged_project_dir]

    try:
        try:
            completed = subprocess.run(
                command,
                cwd=staged_project_dir,
                capture_output=True,
                text=True,
                check=False,
                timeout=SUBPROCESS_TIMEOUT,
            )
        except FileNotFoundError:
            return "", "aristotle CLI not found on PATH"
        except subprocess.TimeoutExpired:
            return "", "submission timed out"

        if completed.returncode != 0:
            error_type, error_msg = classify_failure(completed.stdout, completed.stderr)
            detail = _clip_command_output(completed.stdout, completed.stderr)
            if detail:
                return "", f"[{error_type}] {error_msg}: {detail}"
            return "", f"[{error_type}] {error_msg}"

        match = PROJECT_ID_PATTERN.search(completed.stdout + "\n" + completed.stderr)
        if not match:
            detail = _clip_command_output(completed.stdout, completed.stderr)
            if detail:
                return "", f"No job UUID found in Aristotle output: {detail}"
            return "", "No job UUID found in Aristotle output"

        return match.group(1), ""
    finally:
        staging.cleanup()


async def poll(
    job_id: str, project_dir: str, submitted_at: str
) -> tuple[str, ExtractedArchive | None]:
    """Check job status.

    Returns (status, archive_payload_or_none). status is running|completed|failed.
    Payload is set only when status is completed and the archive yielded content.
    """
    project_dir = str(Path(project_dir).resolve())
    try:
        completed = subprocess.run(
            ["aristotle", "list", "--limit", "100"],
            cwd=project_dir,
            capture_output=True,
            text=True,
            check=False,
            timeout=SUBPROCESS_TIMEOUT,
        )
    except FileNotFoundError:
        return "failed", None
    except subprocess.TimeoutExpired:
        return "failed", None

    remote_status = "UNKNOWN"
    for match in PROJECT_LINE_PATTERN.finditer(completed.stdout):
        if match.group(1) == job_id:
            remote_status = match.group(2)
            break

    if remote_status in {"NOT_STARTED", "QUEUED", "PENDING_RETRY", "IN_PROGRESS"}:
        age = _age_seconds(submitted_at)
        if remote_status == "IN_PROGRESS" and age is not None and age > STALE_PROGRESS_TIMEOUT:
            return "failed", None
        return "running", None

    if remote_status in {"FAILED", "OUT_OF_BUDGET", "CANCELED"}:
        return "failed", None

    if remote_status == "UNKNOWN":
        age = _age_seconds(submitted_at)
        if age is not None and age > GHOST_JOB_TIMEOUT:
            return "failed", None
        return "running", None

    if remote_status not in {"COMPLETE", "COMPLETE_WITH_ERRORS"}:
        return "failed", None

    destination = Path(project_dir) / f"aristotle_result_{job_id}.bin"
    result_cmd = subprocess.run(
        ["aristotle", "result", job_id, "--destination", str(destination)],
        cwd=project_dir,
        capture_output=True,
        text=True,
        check=False,
        timeout=SUBPROCESS_TIMEOUT,
    )

    if result_cmd.returncode != 0:
        return "failed", None

    bundle = extract_archive(destination)
    if not bundle.markdown.strip() and not (bundle.structured_json_raw or "").strip():
        return "completed", None
    if remote_status == "COMPLETE_WITH_ERRORS":
        bundle = ExtractedArchive(
            markdown=bundle.markdown,
            structured_json_raw=bundle.structured_json_raw,
            complete_with_errors=True,
        )
    return "completed", bundle


def parse_result(markdown: str) -> AristotleParsedResult:
    """Parse ARISTOTLE_SUMMARY.md-style content into structured fields."""
    result = AristotleParsedResult(
        summary_text=(markdown or "")[:8000],
        parse_source="markdown",
        parse_schema_version=None,
    )

    if not (markdown or "").strip():
        result.verdict = Verdict.INCONCLUSIVE
        return result

    lines = markdown.splitlines()
    current: str | None = None
    completed_lines: list[str] = []
    partial_lines: list[str] = []
    failed_lines: list[str] = []
    has_completed_heading = False
    has_partial_heading = False

    heading_re = re.compile(r"^#+\s*(.+)$")

    for line in lines:
        stripped = line.strip()
        hm = heading_re.match(stripped)
        if hm:
            title = hm.group(1).strip().lower()
            if "completed" in title:
                current = "completed"
                has_completed_heading = True
            elif "partial" in title:
                current = "partial"
                has_partial_heading = True
            elif "failed" in title:
                current = "failed"
            else:
                current = None
            continue

        if current is None:
            continue

        if not stripped.startswith(("-", "*", "•")):
            continue
        content = stripped.lstrip("-*•").strip()
        if not content:
            continue
        if current == "completed":
            completed_lines.append(content)
        elif current == "partial":
            partial_lines.append(content)
        elif current == "failed":
            failed_lines.append(content)

    def scan_line(content: str, from_completed: bool) -> None:
        low = content.lower()
        if "counterexample" in low or "witness" in low:
            result.counterexamples.append(content)
        if "blocker" in low or "stuck" in low:
            result.blockers.append(content)
        if "sorry" in low or "unsolved" in low or "goal" in low:
            result.unsolved_goals.append(content)
        if "theorem" in low or "lemma" in low:
            if from_completed:
                result.proved_lemmas.append(content)
            else:
                result.generated_lemmas.append(content)

    for cl in completed_lines:
        scan_line(cl, True)
    for pl in partial_lines:
        scan_line(pl, False)
    for fl in failed_lines:
        scan_line(fl, False)
        if "error" in fl.lower():
            result.error_message = result.error_message or fl

    if result.counterexamples:
        result.verdict = Verdict.DISPROVED
    elif has_completed_heading and (
        result.proved_lemmas
        or any("theorem" in x.lower() or "lemma" in x.lower() for x in completed_lines)
    ):
        result.verdict = Verdict.PROVED
    elif has_partial_heading or partial_lines:
        result.verdict = Verdict.PARTIAL
    else:
        result.verdict = Verdict.INCONCLUSIVE

    return result


def synthesize_structured_json_from_markdown(markdown: str) -> str:
    """Build schema-v1 JSON from markdown summary parsing (local shim for consistency)."""
    r = parse_result(markdown)
    payload: dict[str, Any] = {
        "schema_version": 1,
        "result_origin": RESULT_ORIGIN_ORCHESTRATOR_MARKDOWN_V1,
        "verdict": r.verdict.value,
        "proved_lemmas": r.proved_lemmas,
        "generated_lemmas": r.generated_lemmas,
        "unsolved_goals": r.unsolved_goals,
        "blockers": r.blockers,
        "counterexamples": r.counterexamples,
    }
    if r.error_message.strip():
        payload["error_message"] = r.error_message
    return json.dumps(payload, ensure_ascii=False)


def with_synthesized_json_if_needed(bundle: ExtractedArchive) -> ExtractedArchive:
    """If the archive had summary markdown but no JSON file, attach synthesized v1 JSON."""
    if not bundle.markdown.strip():
        return bundle
    if (bundle.structured_json_raw or "").strip():
        return bundle
    return ExtractedArchive(
        markdown=bundle.markdown,
        structured_json_raw=synthesize_structured_json_from_markdown(bundle.markdown),
        complete_with_errors=bundle.complete_with_errors,
    )
