from __future__ import annotations

import os
import re
import subprocess
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path

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


def _age_seconds(submitted_at: str) -> float | None:
    if not submitted_at:
        return None
    try:
        dt = datetime.fromisoformat(submitted_at)
        return (datetime.utcnow() - dt).total_seconds()
    except ValueError:
        return None


def extract_and_read_summary(archive_path: Path) -> str:
    """Extract archive and return ARISTOTLE_SUMMARY.md content."""
    extract_dir = archive_path.with_suffix(".contents")
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
    elif tarfile.is_tarfile(archive_path):
        with tarfile.open(archive_path) as tf:
            for member in tf.getmembers():
                if member.isdir():
                    continue
                dest = (extract_dir / member.name).resolve()
                if not str(dest).startswith(str(base)):
                    continue
                tf.extract(member, extract_dir)
    else:
        return ""

    for path in extract_dir.rglob("*"):
        if path.is_file() and "ARISTOTLE_SUMMARY" in path.name and path.suffix == ".md":
            return path.read_text(encoding="utf-8", errors="replace")

    return ""


async def submit(objective: str, project_dir: str) -> tuple[str, str]:
    """Submit job to Aristotle. Returns (job_id, error_message). job_id is empty on failure."""
    if not os.environ.get("ARISTOTLE_API_KEY"):
        return "", "ARISTOTLE_API_KEY not set"

    project_dir = str(Path(project_dir).resolve())
    command = ["aristotle", "submit", objective, "--project-dir", project_dir]

    try:
        completed = subprocess.run(
            command,
            cwd=project_dir,
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
        return "", f"[{error_type}] {error_msg}"

    match = PROJECT_ID_PATTERN.search(completed.stdout + "\n" + completed.stderr)
    if not match:
        return "", "No job UUID found in Aristotle output"

    return match.group(1), ""


async def poll(job_id: str, project_dir: str, submitted_at: str) -> tuple[str, str | None]:
    """Check job status. Returns (status, raw_output_or_none).

    status is one of: "running", "completed", "failed"
    raw_output is only set when status is "completed"
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

    if remote_status != "COMPLETE":
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

    raw_output = extract_and_read_summary(destination)
    return "completed", raw_output if raw_output else None


def parse_result(markdown: str) -> AristotleParsedResult:
    """Parse ARISTOTLE_SUMMARY.md-style content into structured fields."""
    result = AristotleParsedResult(summary_text=(markdown or "")[:8000])

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
