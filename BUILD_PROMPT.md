# Build Prompt: Aristotle Research Orchestrator (v2)

Build the entire project from scratch in this folder. This is a simplified rebuild of a research orchestrator that uses the Aristotle CLI (a Lean 4 theorem prover service) to drive discovery-through-verification campaigns.

## Philosophy

The core idea: take a research prompt, decompose it into verification targets, then autonomously run Aristotle experiments until all targets are resolved. The LLM reasons about what to try next based on accumulated evidence. "Discovery via verification" — proof attempts reveal structure (lemmas, blockers, counterexamples) even when they fail.

## File Structure (create ALL of these)

```
aristotle_new_orchestrator/
├── Dockerfile
├── pyproject.toml
├── requirements.txt
├── README.md
├── .gitignore
├── src/
│   └── orchestrator/
│       ├── __init__.py
│       ├── app.py              # FastAPI app + lifespan (background manager loop)
│       ├── manager.py           # The tick loop + LLM reasoning
│       ├── aristotle.py         # Aristotle CLI: submit, poll, parse results
│       ├── llm.py               # LLM client (OpenAI-compatible API)
│       ├── db.py                # SQLite schema + all queries
│       ├── models.py            # Pydantic models
│       └── templates/
│           └── dashboard.html   # Single-page HTMX dashboard
```

---

## 1. `pyproject.toml`

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "aristotle-orchestrator"
version = "0.1.0"
description = "LLM-driven research orchestrator using Aristotle for discovery via formal verification"
requires-python = ">=3.11"
dependencies = [
  "fastapi>=0.115.0",
  "uvicorn>=0.30.0",
  "jinja2>=3.1.0",
  "httpx>=0.27.0",
]

[tool.setuptools]
package-dir = {"" = "src"}

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools.package-data]
orchestrator = ["templates/*.html"]
```

## 2. `requirements.txt`

```
fastapi>=0.115.0
uvicorn>=0.30.0
jinja2>=3.1.0
httpx>=0.27.0
```

## 3. `.gitignore`

```
__pycache__/
*.pyc
*.db
*.sqlite
.env
dist/
*.egg-info/
.venv/
```

---

## 4. `src/orchestrator/models.py` — Pydantic Models

All data flows through these models. Keep it flat.

```python
from __future__ import annotations
from pydantic import BaseModel, Field
from enum import Enum
from typing import Optional
from datetime import datetime


class CampaignStatus(str, Enum):
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


class TargetStatus(str, Enum):
    OPEN = "open"
    VERIFIED = "verified"
    REFUTED = "refuted"
    BLOCKED = "blocked"


class ExperimentStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Verdict(str, Enum):
    PROVED = "proved"
    PARTIAL = "partial"
    DISPROVED = "disproved"
    INCONCLUSIVE = "inconclusive"
    INFRA_ERROR = "infra_error"


class Campaign(BaseModel):
    id: str
    prompt: str
    status: CampaignStatus = CampaignStatus.ACTIVE
    workspace_dir: str = ""
    created_at: datetime = Field(default_factory=datetime.utcnow)

class Target(BaseModel):
    id: str
    campaign_id: str
    description: str
    status: TargetStatus = TargetStatus.OPEN
    evidence: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)

class Experiment(BaseModel):
    id: str
    campaign_id: str
    target_id: str
    objective: str
    status: ExperimentStatus = ExperimentStatus.PENDING
    aristotle_job_id: Optional[str] = None
    result_raw: Optional[str] = None
    result_summary: Optional[str] = None
    verdict: Optional[Verdict] = None
    submitted_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

class Tick(BaseModel):
    id: Optional[int] = None
    campaign_id: str
    tick_number: int
    reasoning: str
    actions: dict = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)

class ManagerDecision(BaseModel):
    """What the LLM decides to do each tick."""
    reasoning: str
    target_updates: list[TargetUpdate] = Field(default_factory=list)
    new_experiments: list[NewExperiment] = Field(default_factory=list)
    campaign_complete: bool = False
    campaign_complete_reason: str = ""

class TargetUpdate(BaseModel):
    target_id: str
    new_status: TargetStatus
    evidence: str = ""

class NewExperiment(BaseModel):
    target_id: str
    objective: str

class AristotleParsedResult(BaseModel):
    """Structured result parsed from Aristotle output."""
    verdict: Verdict = Verdict.INCONCLUSIVE
    proved_lemmas: list[str] = Field(default_factory=list)
    generated_lemmas: list[str] = Field(default_factory=list)
    unsolved_goals: list[str] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    counterexamples: list[str] = Field(default_factory=list)
    error_message: str = ""
    summary_text: str = ""

class CampaignState(BaseModel):
    """Full state passed to LLM for reasoning."""
    campaign: Campaign
    targets: list[Target]
    experiments: list[Experiment]
    recent_ticks: list[Tick] = Field(default_factory=list)
```

Note: `ManagerDecision` references `TargetUpdate` and `NewExperiment`, so define those BEFORE `ManagerDecision` in the actual file. The order above is for readability.

---

## 5. `src/orchestrator/db.py` — SQLite Database

4 tables: `campaigns`, `targets`, `experiments`, `ticks`. WAL mode. All queries in one file.

Key operations:
- `initialize()` — create tables
- `create_campaign(prompt) -> campaign_id`
- `add_targets(campaign_id, targets)`
- `get_campaign_state(campaign_id) -> CampaignState`
- `create_experiment(campaign_id, target_id, objective) -> experiment_id`
- `update_experiment_submitted(experiment_id, aristotle_job_id)`
- `update_experiment_completed(experiment_id, result_raw, result_summary, verdict)`
- `update_experiment_failed(experiment_id, error)`
- `update_target(target_id, status, evidence)`
- `complete_campaign(campaign_id)`
- `get_running_experiments(campaign_id) -> list`
- `all_targets_resolved(campaign_id) -> bool`
- `record_tick(campaign_id, tick_number, reasoning, actions)`
- `get_active_campaigns() -> list`
- `get_all_campaigns() -> list` (for dashboard)
- `get_campaign_dashboard(campaign_id) -> dict` (aggregated view for dashboard)

Use `sqlite3.Row` for dict-like access. Use `datetime.utcnow().isoformat()` for timestamps.
Generate UUIDs with `uuid4().hex[:12]` for readable IDs.

---

## 6. `src/orchestrator/aristotle.py` — Aristotle CLI Integration

This is the most important file to get right. It wraps the `aristotle` CLI tool.

### Aristotle CLI Commands

```bash
# Submit a job (returns immediately with a project UUID)
aristotle submit "{objective}" --project-dir {project_dir}

# List jobs and their statuses
aristotle list --limit 100

# Download result when job is COMPLETE
aristotle result {job_uuid} --destination {output_path}
```

### Job Lifecycle
1. `submit` → returns stdout containing a UUID like `a1b2c3d4-e5f6-7890-abcd-ef1234567890`
2. `list` → shows status: `NOT_STARTED`, `QUEUED`, `PENDING_RETRY`, `IN_PROGRESS`, `COMPLETE`, `FAILED`, `OUT_OF_BUDGET`, `CANCELED`
3. When `COMPLETE` → `result {uuid} --destination {path}` downloads a zip/tar archive
4. Archive contains `ARISTOTLE_SUMMARY.md` with structured results

### Submit Function

```python
import subprocess
import re
import os
from pathlib import Path

PROJECT_ID_PATTERN = re.compile(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})")
SUBPROCESS_TIMEOUT = 20 * 60  # 20 minutes

async def submit(objective: str, project_dir: str) -> tuple[str, str]:
    """Submit job to Aristotle. Returns (job_id, error_message).
    job_id is empty string on failure.
    """
    if not os.environ.get("ARISTOTLE_API_KEY"):
        return "", "ARISTOTLE_API_KEY not set"

    project_dir = str(Path(project_dir).resolve())
    command = ["aristotle", "submit", objective, "--project-dir", project_dir]

    try:
        completed = subprocess.run(
            command, cwd=project_dir,
            capture_output=True, text=True, check=False,
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
```

### Poll Function

```python
PROJECT_LINE_PATTERN = re.compile(
    r"^([0-9a-f-]{36})\s+([A-Z_]+)\s+", re.MULTILINE
)

GHOST_JOB_TIMEOUT = 2 * 3600      # 2 hours
STALE_PROGRESS_TIMEOUT = 6 * 3600  # 6 hours

async def poll(job_id: str, project_dir: str, submitted_at: str) -> tuple[str, str | None]:
    """Check job status. Returns (status, raw_output_or_none).
    
    status is one of: "running", "completed", "failed"
    raw_output is only set when status is "completed"
    """
    # Check status via `aristotle list`
    completed = subprocess.run(
        ["aristotle", "list", "--limit", "100"],
        cwd=project_dir, capture_output=True, text=True, check=False,
        timeout=SUBPROCESS_TIMEOUT,
    )
    
    remote_status = "UNKNOWN"
    for match in PROJECT_LINE_PATTERN.finditer(completed.stdout):
        if match.group(1) == job_id:
            remote_status = match.group(2)
            break
    
    # Still queued or running
    if remote_status in {"NOT_STARTED", "QUEUED", "PENDING_RETRY", "IN_PROGRESS"}:
        # Check for stale timeout on IN_PROGRESS
        age = _age_seconds(submitted_at)
        if remote_status == "IN_PROGRESS" and age and age > STALE_PROGRESS_TIMEOUT:
            return "failed", None  # stalled too long
        return "running", None
    
    # Failed remotely
    if remote_status in {"FAILED", "OUT_OF_BUDGET", "CANCELED"}:
        return "failed", None
    
    # Unknown — check ghost timeout
    if remote_status == "UNKNOWN":
        age = _age_seconds(submitted_at)
        if age and age > GHOST_JOB_TIMEOUT:
            return "failed", None  # ghost job
        return "running", None
    
    # COMPLETE — download result
    destination = Path(project_dir) / f"aristotle_result_{job_id}.bin"
    result_cmd = subprocess.run(
        ["aristotle", "result", job_id, "--destination", str(destination)],
        cwd=project_dir, capture_output=True, text=True, check=False,
        timeout=SUBPROCESS_TIMEOUT,
    )
    
    if result_cmd.returncode != 0:
        return "failed", None
    
    # Extract archive and find ARISTOTLE_SUMMARY.md
    raw_output = extract_and_read_summary(destination)
    return "completed", raw_output
```

### Result Parsing (from ARISTOTLE_SUMMARY.md)

The `ARISTOTLE_SUMMARY.md` file has sections like:
```markdown
# Summary of changes
## Completed
- theorem name : type := proof
## Partial
- ...
## Failed
- Error: ...
```

Parse it to extract:
- **Proved lemmas**: lines with `theorem` or `lemma` keywords under `## Completed`
- **Unsolved goals**: lines with `sorry`, `unsolved`, `goal` keywords
- **Blockers**: lines with `blocker`, `stuck` keywords
- **Counterexamples**: lines with `counterexample`, `witness` keywords
- **Overall verdict**: `proved` if `## Completed`, `partial` if `## Partial`, `disproved` if counterexample found, `inconclusive` otherwise

### Failure Classification

Classify CLI failures by scanning stdout+stderr for patterns:

```python
def classify_failure(stdout: str, stderr: str) -> tuple[str, str]:
    text = (stdout + "\n" + stderr).lower()
    
    if "invalid api key" in text or "check your api key" in text:
        return "auth_error", "Aristotle rejected the API key"
    if "nodename nor servname" in text or "could not resolve host" in text:
        return "dns_failure", "Cannot resolve aristotle.harmonic.fun"
    if "connecterror" in text or "connection refused" in text or "network is unreachable" in text:
        return "network_error", "Cannot reach Aristotle API"
    if "lean-toolchain" in text or "no lean files" in text:
        return "workspace_error", "Missing Lean toolchain in workspace"
    if "permission denied" in text or "not found" in text:
        return "path_error", "CLI or required files not accessible"
    if "traceback" in text or "api error" in text:
        return "api_error", "Aristotle returned an error"
    
    return "unknown", "Non-zero exit code from Aristotle CLI"
```

### Archive Extraction

Handle both zip and tar.gz archives safely:

```python
import zipfile, tarfile

def extract_and_read_summary(archive_path: Path) -> str:
    """Extract archive and return ARISTOTLE_SUMMARY.md content."""
    extract_dir = archive_path.with_suffix(".contents")
    extract_dir.mkdir(exist_ok=True)
    
    if zipfile.is_zipfile(archive_path):
        with zipfile.ZipFile(archive_path) as zf:
            # Safe extraction (check for path traversal)
            for member in zf.infolist():
                member_path = (extract_dir / member.filename).resolve()
                if not str(member_path).startswith(str(extract_dir.resolve())):
                    continue
            zf.extractall(extract_dir)
    elif tarfile.is_tarfile(archive_path):
        with tarfile.open(archive_path) as tf:
            for member in tf.getmembers():
                member_path = (extract_dir / member.name).resolve()
                if not str(member_path).startswith(str(extract_dir.resolve())):
                    continue
            tf.extractall(extract_dir)
    
    # Find ARISTOTLE_SUMMARY.md
    for path in extract_dir.rglob("*"):
        if path.is_file() and "ARISTOTLE_SUMMARY" in path.name and path.suffix == ".md":
            return path.read_text(encoding="utf-8")
    
    return ""
```

---

## 7. `src/orchestrator/llm.py` — LLM Client

Three functions, all returning structured Pydantic models. Use `httpx` for async HTTP to an OpenAI-compatible API.

### Configuration

```python
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o")
```

### Function 1: `decompose_prompt(prompt) -> list[Target]`

System prompt:
```
You are a mathematical research assistant that decomposes research problems into verification targets.

Given a research prompt, decompose it into concrete verification targets that can be investigated through formal theorem proving with Lean 4 via Aristotle.

Each target should be:
- A specific claim or sub-problem that Aristotle can attempt to verify
- Independent enough to investigate separately  
- Concrete enough to write a Lean theorem statement for

Return JSON:
{
  "targets": [
    {"description": "...specific claim to verify..."},
    ...
  ]
}
```

### Function 2: `reason(state: CampaignState) -> ManagerDecision`

This is the core LLM call. System prompt:

```
You are an autonomous research manager running a formal verification campaign using Aristotle (Lean 4 prover).

You receive the full campaign state: targets, experiments, and their results. Based on the evidence accumulated so far, decide:

1. Whether any targets should be marked as verified/refuted/blocked based on experiment results
2. What new experiments to submit to Aristotle to make progress
3. Whether the campaign is complete

Key principles:
- Discovery via verification: even failed proofs reveal useful structure (lemmas, blockers, counterexamples)
- Don't repeat the same experiment. Vary the approach if something failed.
- If a proof partially succeeded, build on the lemmas it proved.
- If a counterexample was found, mark the target as refuted.
- A target is "verified" only when Aristotle returns verdict=proved.
- A target is "blocked" if 3+ experiments all fail with infra errors or the approach seems fundamentally stuck.
- The campaign is complete when all targets are verified, refuted, or blocked.

Return JSON:
{
  "reasoning": "...your analysis of the current state...",
  "target_updates": [
    {"target_id": "...", "new_status": "verified|refuted|blocked", "evidence": "...why..."}
  ],
  "new_experiments": [
    {"target_id": "...", "objective": "...what to ask Aristotle to prove/explore..."}
  ],
  "campaign_complete": false,
  "campaign_complete_reason": ""
}
```

The user message should be a formatted dump of the CampaignState including:
- Campaign prompt
- Each target with status and evidence
- Each experiment with objective, status, verdict, summary
- Last 5 tick reasonings (so the LLM can see its own prior reasoning)

### Function 3: `summarize_result(raw_output: str) -> str`

Simple summarization of raw Aristotle output into 2-3 sentences. This is used for display in the dashboard alongside the structured parsing.

### Implementation Pattern

```python
import httpx
import json

async def _call_llm(system: str, user: str) -> str:
    async with httpx.AsyncClient(timeout=120) as client:
        response = await client.post(
            f"{LLM_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {LLM_API_KEY}"},
            json={
                "model": LLM_MODEL,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": 0.3,
                "response_format": {"type": "json_object"},
            },
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
```

If no `LLM_API_KEY` is set, fall back gracefully:
- `decompose_prompt` creates a single target with the full prompt as description
- `reason` returns empty decision (no updates, no new experiments)
- `summarize_result` returns the first 500 chars of raw output

---

## 8. `src/orchestrator/manager.py` — The Core Loop

This is the brain. One async function `manager_loop` and one `tick` function.

```python
import asyncio
import logging
from orchestrator.db import Database
from orchestrator.aristotle import submit, poll, parse_result
from orchestrator.llm import decompose_prompt, reason, summarize_result
from orchestrator.models import ExperimentStatus, CampaignStatus

logger = logging.getLogger("orchestrator.manager")

MAX_ACTIVE_EXPERIMENTS = int(os.environ.get("MAX_ACTIVE_EXPERIMENTS", "5"))
TICK_INTERVAL = int(os.environ.get("TICK_INTERVAL", "30"))
MAX_EXPERIMENTS_PER_CAMPAIGN = int(os.environ.get("MAX_EXPERIMENTS", "50"))

async def manager_loop(db: Database):
    """Main loop. Runs forever, ticking every TICK_INTERVAL seconds."""
    tick_count = 0
    while True:
        try:
            campaigns = db.get_active_campaigns()
            for campaign in campaigns:
                await tick(db, campaign, tick_count)
            tick_count += 1
        except Exception:
            logger.exception("Error in manager tick")
        await asyncio.sleep(TICK_INTERVAL)


async def tick(db: Database, campaign: dict, tick_number: int):
    """One tick of the manager loop for a single campaign."""
    campaign_id = campaign["id"]
    
    # 1. Sync: poll Aristotle for completed jobs
    running = db.get_running_experiments(campaign_id)
    for exp in running:
        if not exp["aristotle_job_id"]:
            continue
        status, raw_output = await poll(
            exp["aristotle_job_id"],
            campaign["workspace_dir"],
            exp["submitted_at"] or "",
        )
        if status == "completed" and raw_output:
            parsed = parse_result(raw_output)
            summary = await summarize_result(raw_output)
            db.update_experiment_completed(
                exp["id"],
                result_raw=raw_output,
                result_summary=summary,
                verdict=parsed.verdict.value,
            )
            # Add evidence to the target
            evidence = f"Experiment {exp['id']}: {parsed.verdict.value}"
            if parsed.proved_lemmas:
                evidence += f" — proved {len(parsed.proved_lemmas)} lemma(s)"
            if parsed.blockers:
                evidence += f" — {len(parsed.blockers)} blocker(s)"
            db.append_target_evidence(exp["target_id"], evidence)
            
        elif status == "failed":
            db.update_experiment_failed(exp["id"], "Aristotle job failed or timed out")
            db.append_target_evidence(exp["target_id"], f"Experiment {exp['id']}: failed")
    
    # 2. Reason: LLM decides what to do next
    state = db.get_campaign_state(campaign_id)
    
    # Don't submit more if we've hit limits
    total_experiments = len(state.experiments)
    active_count = sum(1 for e in state.experiments if e.status in {ExperimentStatus.SUBMITTED, ExperimentStatus.RUNNING})
    
    if total_experiments >= MAX_EXPERIMENTS_PER_CAMPAIGN:
        logger.info(f"Campaign {campaign_id} hit max experiments ({MAX_EXPERIMENTS_PER_CAMPAIGN})")
        db.complete_campaign(campaign_id)
        return
    
    decision = await reason(state)
    
    # 3. Apply target updates
    for update in decision.target_updates:
        db.update_target(update.target_id, update.new_status.value, update.evidence)
    
    # 4. Submit new experiments (respect active cap)
    slots_available = MAX_ACTIVE_EXPERIMENTS - active_count
    for new_exp in decision.new_experiments[:max(0, slots_available)]:
        exp_id = db.create_experiment(campaign_id, new_exp.target_id, new_exp.objective)
        
        # Submit to Aristotle
        job_id, error = await submit(new_exp.objective, campaign["workspace_dir"])
        if job_id:
            db.update_experiment_submitted(exp_id, job_id)
        else:
            db.update_experiment_failed(exp_id, error)
    
    # 5. Check campaign completion
    if decision.campaign_complete or db.all_targets_resolved(campaign_id):
        db.complete_campaign(campaign_id)
    
    # 6. Record tick
    db.record_tick(
        campaign_id, tick_number,
        reasoning=decision.reasoning,
        actions={
            "target_updates": [u.model_dump() for u in decision.target_updates],
            "new_experiments": [e.model_dump() for e in decision.new_experiments],
            "campaign_complete": decision.campaign_complete,
        },
    )
```

---

## 9. `src/orchestrator/app.py` — FastAPI Application

Single FastAPI app that:
1. Starts the manager loop as a background task on startup
2. Serves the dashboard at `/`
3. Serves API endpoints for dashboard data
4. Serves health check at `/health`
5. Has a POST endpoint to start new campaigns

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import asyncio
import os
from pathlib import Path

from orchestrator.db import Database
from orchestrator.manager import manager_loop
from orchestrator.llm import decompose_prompt
from orchestrator.models import CampaignStatus

DATABASE_PATH = os.environ.get("DATABASE_PATH", "orchestrator.db")
WORKSPACE_DIR = os.environ.get("WORKSPACE_DIR", "/data/workspace")

db = Database(DATABASE_PATH)

@asynccontextmanager
async def lifespan(app: FastAPI):
    db.initialize()
    Path(WORKSPACE_DIR).mkdir(parents=True, exist_ok=True)
    task = asyncio.create_task(manager_loop(db))
    yield
    task.cancel()

app = FastAPI(title="Aristotle Orchestrator", lifespan=lifespan)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


# --- Health ---
@app.get("/health")
async def health():
    campaigns = db.get_active_campaigns()
    return {"status": "healthy", "active_campaigns": len(campaigns)}


# --- Dashboard ---
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    campaigns = db.get_all_campaigns()
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "campaigns": campaigns,
        "selected": None,
        "state": None,
    })

@app.get("/campaign/{campaign_id}", response_class=HTMLResponse)
async def campaign_detail(request: Request, campaign_id: str):
    campaigns = db.get_all_campaigns()
    state = db.get_campaign_state(campaign_id)
    ticks = db.get_recent_ticks(campaign_id, limit=20)
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "campaigns": campaigns,
        "selected": campaign_id,
        "state": state,
        "ticks": ticks,
    })


# --- API: Start Campaign ---
@app.post("/api/campaign")
async def start_campaign(prompt: str = Form(...)):
    """Start a new campaign from a prompt."""
    # Create campaign
    campaign_id = db.create_campaign(prompt, workspace_dir=WORKSPACE_DIR)
    
    # Decompose into targets via LLM
    targets = await decompose_prompt(prompt)
    db.add_targets(campaign_id, [t.description for t in targets])
    
    return RedirectResponse(f"/campaign/{campaign_id}", status_code=303)


# --- API: Campaign State (for HTMX polling) ---
@app.get("/api/campaign/{campaign_id}/state", response_class=HTMLResponse)
async def campaign_state_fragment(request: Request, campaign_id: str):
    """Returns HTML fragment for HTMX polling."""
    state = db.get_campaign_state(campaign_id)
    ticks = db.get_recent_ticks(campaign_id, limit=20)
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "campaigns": db.get_all_campaigns(),
        "selected": campaign_id,
        "state": state,
        "ticks": ticks,
    }, block_name="campaign_content")
    # NOTE: If block_name doesn't work with your Jinja2 version, 
    # create a separate partial template for the campaign content fragment.


# --- API: Pause/Resume ---
@app.post("/api/campaign/{campaign_id}/pause")
async def pause_campaign(campaign_id: str):
    db.update_campaign_status(campaign_id, CampaignStatus.PAUSED.value)
    return RedirectResponse(f"/campaign/{campaign_id}", status_code=303)

@app.post("/api/campaign/{campaign_id}/resume")  
async def resume_campaign(campaign_id: str):
    db.update_campaign_status(campaign_id, CampaignStatus.ACTIVE.value)
    return RedirectResponse(f"/campaign/{campaign_id}", status_code=303)
```

---

## 10. `src/orchestrator/templates/dashboard.html` — Dashboard UI

Single-page dashboard using **Tailwind CSS** (CDN) and **HTMX** (CDN). No build step.

Features:
- Left sidebar: list of campaigns with status badges
- Top: form to start new campaign (text input + submit)
- Main area (when campaign selected):
  - Campaign prompt and status
  - Progress bar (% of targets resolved)
  - **Targets** section: cards with status badges (open/verified/refuted/blocked), evidence list
  - **Experiments** section: table with columns: objective, status, verdict, timing
  - **Manager Log** section: tick-by-tick reasoning trace (collapsible)
- HTMX: poll `/api/campaign/{id}/state` every 5 seconds to update the main area
- Dark theme, clean typography
- Color coding: verified=green, refuted=red, blocked=amber, open=blue, running=purple

Make it genuinely beautiful. This is the main interface.

---

## 11. `Dockerfile`

```dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    sqlite3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

EXPOSE 8000

CMD ["uvicorn", "orchestrator.app:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

## 12. `README.md`

Write a clear README covering:
- What this is (1 paragraph)
- The core loop diagram (ASCII art)
- Environment variables table:
  - `DATABASE_PATH` — path to SQLite DB (default: `orchestrator.db`)
  - `WORKSPACE_DIR` — workspace for Aristotle projects (default: `/data/workspace`)
  - `ARISTOTLE_API_KEY` — API key for Aristotle
  - `LLM_API_KEY` — API key for LLM (OpenAI-compatible)
  - `LLM_BASE_URL` — LLM API base URL (default: `https://api.openai.com/v1`)
  - `LLM_MODEL` — model name (default: `gpt-4o`)
  - `MAX_ACTIVE_EXPERIMENTS` — max concurrent Aristotle jobs (default: `5`)
  - `TICK_INTERVAL` — seconds between ticks (default: `30`)
  - `MAX_EXPERIMENTS` — max total experiments per campaign (default: `50`)
- Local development: `pip install -e .` then `uvicorn orchestrator.app:app --reload`
- Railway deployment: push to GitHub, connect to Railway, add volume at `/data`, set env vars

---

## Key Design Decisions

1. **One process, one service**: FastAPI serves both the dashboard and the manager loop. No separate services, no inter-service communication.

2. **LLM-driven, not heuristic-driven**: The old system had complex frontier ranking, move families, candidate auditing. The new system lets the LLM see full state and decide. The LLM IS the manager.

3. **4 tables, not 30+**: campaigns, targets, experiments, ticks. That's it. Evidence is stored as JSON arrays on targets. Tick reasoning is stored as text.

4. **Aristotle integration preserved exactly**: The submit/poll/parse pattern is proven. Keep the exact CLI commands, status patterns, failure classification, and ARISTOTLE_SUMMARY.md parsing.

5. **Dashboard is server-rendered**: HTMX + Tailwind. No React, no build step, no separate service. The dashboard is just another set of routes on the same FastAPI app.

6. **Railway-native**: One Dockerfile, one service, one volume. Health check on `/health`.

---

## What NOT to Build

- No provider registry (only Aristotle)
- No multi-tenancy
- No prompt linter
- No evaluator/scoring system
- No semantic memory / canonical IDs
- No lemma ledger / proof obligations DAG
- No discovery graph
- No operator commands system
- No incident tracking
- No separate health server thread
- No campaign planner heuristics (LLM does decomposition)
- No experiment generator / move families
- No replay system

All of these were complexity in the old system. The LLM reasoning + structured Aristotle parsing gives us the same value with 10% of the code.

---

## Build Order

1. `models.py` (everything depends on these)
2. `db.py` (needs models)
3. `aristotle.py` (standalone, needs models)
4. `llm.py` (standalone, needs models)
5. `manager.py` (needs db, aristotle, llm)
6. `app.py` (needs everything)
7. `templates/dashboard.html` (needs app routes to exist)
8. `Dockerfile`, `pyproject.toml`, `requirements.txt`, `.gitignore`, `README.md`

Build all files. Make them complete, working, and production-ready. No stubs, no TODOs.
