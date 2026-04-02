# Aristotle Research Orchestrator (v2)

A small **LLM-driven research orchestrator** that turns a natural-language research prompt into verification targets, then runs **Aristotle** (Lean 4) experiments in a loop until targets are resolved or limits are hit. Proof attempts surface structure—lemmas, blockers, counterexamples—even when they fail (“discovery via verification”).

One **FastAPI** process serves the **HTMX + Tailwind** dashboard and a **background manager** that ticks on an interval: poll Aristotle jobs, parse `ARISTOTLE_SUMMARY.md`, call the LLM for the next moves, and enqueue new experiments.

## Core loop

```
  ┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
  │   Prompt    │────▶│  LLM decompose   │────▶│    Targets      │
  └─────────────┘     └──────────────────┘     └────────┬────────┘
                                                        │
  ┌─────────────┐     ┌──────────────────┐              ▼
  │  Dashboard  │◀───▶│  SQLite state    │◀────┌─────────────────┐
  └─────────────┘     └──────────────────┘     │  Manager tick:    │
                                 ▲             │  poll Aristotle  │
                                 │             │  → LLM reason    │
                                 └─────────────│  → submit jobs   │
                                               └─────────────────┘
```

## Workspaces and Mathlib

- **Per-campaign directories**: each campaign has its own Lake project at `WORKSPACE_ROOT/<campaign_id>/` (isolated `lean-toolchain`, `lakefile.lean`, and `OrchWorkspace/`). Aristotle uses that path as `--project-dir`.
- **Templates** (pick in the “New campaign” form, `DEFAULT_WORKSPACE_TEMPLATE`, or JSON `workspace_template`):
  - **minimal** — small Lake library, fast cold start (Lean pin in-repo).
  - **mathlib** — `require mathlib from git "https://github.com/leanprover-community/mathlib4"` with a `lean-toolchain` aligned to current Mathlib (see [Using mathlib4 as a dependency](https://github.com/leanprover-community/mathlib4/wiki/Using-mathlib4-as-a-dependency)).
- **First Mathlib build**: downloading and building Mathlib can take a long time. After the template is copied into a campaign directory, run **`lake exe cache get`** inside that directory (local shell or `docker exec`) to fetch precompiled artifacts when available. Plan Docker/Railway image layers or a warm volume accordingly—the UI labels the Mathlib option as expensive on first use.

### Migrating from a single shared workspace

Older deployments stored every campaign under one `WORKSPACE_DIR`. Set **`WORKSPACE_LEGACY_DIR`** to that absolute path (and **`WORKSPACE_ROOT`** to the new parent). On startup, any campaign whose `workspace_dir` still equals the legacy path is copied (or symlinked if `WORKSPACE_MIGRATION_SYMLINK=1`) into `WORKSPACE_ROOT/<campaign_id>/` and the database row is updated. Safe to run repeatedly.

## Environment variables

| Variable | Description |
|----------|-------------|
| `DATABASE_PATH` | SQLite database file (default: `orchestrator.db` in the CWD) |
| `WORKSPACE_ROOT` | Parent directory for per-campaign workspaces (default: `./workspace_root` locally; Docker/Railway: `/data/workspaces`) |
| `WORKSPACE_LEGACY_DIR` | Optional legacy shared workspace path to migrate from (default: falls back to `WORKSPACE_DIR` if set) |
| `WORKSPACE_DIR` | **Legacy.** Used only as a default for `WORKSPACE_LEGACY_DIR` when the latter is unset |
| `WORKSPACE_MIGRATION_SYMLINK` | If `1`/`true`/`yes`, migration tries a symlink to the legacy dir before copying |
| `DEFAULT_WORKSPACE_TEMPLATE` | `minimal` or `mathlib` when the form/API does not specify (default: `minimal`) |
| `ARISTOTLE_API_KEY` | API key for the Aristotle CLI |
| `LLM_API_KEY` | API key for an OpenAI-compatible chat API |
| `LLM_BASE_URL` | API base URL (default: `https://api.openai.com/v1`) |
| `LLM_MODEL` | Model name (default: `gpt-4o`) |
| `MAX_ACTIVE_EXPERIMENTS` | Max concurrent in-flight Aristotle jobs per campaign (default: `5`) |
| `TICK_INTERVAL` | Seconds between manager ticks (default: `30`) |
| `MAX_EXPERIMENTS` | Max total experiments per campaign (default: `100`) |
| `LLM_JSON_MODE` | Set to `0` to disable `response_format: json_object` if your API rejects it (default: on) |
| `LLM_EVIDENCE_TARGET_TAIL` | Max target evidence lines shown to the LLM (default: `24`) |
| `LLM_EXPERIMENT_SUMMARY_CHARS` | Truncation for experiment summaries in the LLM user payload (default: `4000`) |
| `LLM_TICK_REASONING_CHARS` | Truncation for prior tick reasoning (default: `4000`) |
| `LLM_RECENT_STRUCTURED_EXPERIMENTS` | Recent completed experiments with parsed fields injected into the LLM context (default: `12`) |
| `LLM_LEDGER_ENTRIES_LIMIT` | Recent lemma-ledger rows injected into the LLM context (default: `40`) |
| `LLM_SUMMARIZE_INPUT_CHARS` | Max raw Aristotle text sent to the summarizer call (default: `50000`) |
| `ADMIN_TOKEN` | If set, enables `/admin/status`, `/admin/config`, `/admin/ui` (use `Authorization: Bearer`, `X-Admin-Token`, or `?admin_token=`; prefer headers) |

## HTTP API (selected)

| Method | Path | Notes |
|--------|------|--------|
| POST | `/api/campaign` | Form: `prompt`, optional `workspace_template` (`minimal` \| `mathlib`) |
| POST | `/api/campaign/start` | JSON: `{"prompt":"...","workspace_template":"minimal"}` → `201` with `campaign_id`, `workspace_dir` |
| GET | `/api/campaign/{id}/ledger` | Read-only ledger JSON (`limit` query, capped) |

## Admin / observability

With **`ADMIN_TOKEN`** set:

- **`GET /admin/status`** — database connectivity, workspace root existence, disk usage for `WORKSPACE_ROOT`, last per-campaign tick exception metadata, `ops_counters` (Aristotle submit failure classes, LLM errors, etc.).
- **`GET /admin/config`** — effective caps and LLM context limits (no secrets).
- **`GET /admin/ui`** — short HTML pointer and curl example.

Structured logs: the manager emits `manager_tick` lines with `campaign_id`, `tick`, `duration_ms`, and `running_experiments_polled`.

`GET /health` includes `database_ok` / `database_message` for shallow checks.

## Local development

```bash
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -e .
export ARISTOTLE_API_KEY=...   # optional for UI-only exploration
export LLM_API_KEY=...         # optional; without it, decomposition uses one target and the manager skips LLM reasoning
uvicorn orchestrator.app:app --reload --app-dir src
```

Open `http://127.0.0.1:8000`. Health: `GET /health`.

### Tests / benchmarks

```bash
pip install -e '.[dev]'
PYTHONPATH=src pytest tests/ -q
```

Regression tests under `tests/benchmarks/` mock Aristotle and assert DB persistence (parsed fields, lemma ledger). Mathlib coverage is **file-level** (template contains the git dependency); **no network** is required for CI. A full `lake build` for Mathlib is optional and documented above.

## Docker

```bash
docker build -t aristotle-orchestrator .
docker run -p 8000:8000 \
  -e ARISTOTLE_API_KEY=... \
  -e LLM_API_KEY=... \
  -v aristotle-data:/data \
  aristotle-orchestrator
```

Mount a volume at `/data`. The image sets `WORKSPACE_ROOT=/data/workspaces`, `WORKSPACE_LEGACY_DIR=/data/workspace`, and `DATABASE_PATH=/data/orchestrator.db`. Install the `aristotle` CLI in your image if you need it inside the container; the host `PATH` is not available unless you add a layer that installs it.

**Mathlib in Docker**: expect a long first build unless you pre-run `lake exe cache get` in a derived image or persistent volume layer.

## Railway (full deploy)

The container listens on **`PORT`** (Railway sets this automatically). SQLite and Lean workspaces should live on a **persistent volume** mounted at **`/data`**.

### One-time setup

1. **Push this repo to GitHub** (Railway deploys from git).
2. In [Railway](https://railway.app): **New project** → **Deploy from GitHub** → select the repo. Use **one** production service with a **`/data`** volume (avoid a second empty duplicate service—see **`GITHUB_DEPLOYMENT.md`**).
3. **Add a volume** (persists SQLite + workspaces across deploys):
   - **Dashboard**: service → **Settings** → **Volumes** → mount path **`/data`**, or  
   - **CLI** (from a linked repo): `railway volume add --mount-path /data`
4. **Link the CLI** (from your laptop, in the repo root):

   ```bash
   railway link -p <your-project-id>   # or: railway link  (interactive)
   ```

5. **Secrets** — do **not** commit keys. Copy `.env.example` → `.env`, fill in `LLM_API_KEY`, `ARISTOTLE_API_KEY`, and Moonshot settings, then:

   ```bash
   ./scripts/sync-railway-env.sh
   ```

   Or set the same variables in the Railway dashboard (**Variables** tab).

6. **Generate a public URL**: service → **Settings** → **Networking** → **Generate domain**. Open that URL in a browser; the dashboard is at **`/`**, health at **`/health`**.

### Required variables on Railway

| Variable | Example / note |
|----------|----------------|
| `LLM_API_KEY` | Moonshot API key |
| `LLM_BASE_URL` | `https://api.moonshot.ai/v1` (or `https://api.moonshot.cn/v1`) |
| `LLM_MODEL` | e.g. `kimi-k2-turbo-preview` (use the exact id from Moonshot) |
| `ARISTOTLE_API_KEY` | Your Aristotle key |
| `DATABASE_PATH` | `/data/orchestrator.db` (default in Docker image) |
| `WORKSPACE_ROOT` | `/data/workspaces` (default in Docker image) |
| `WORKSPACE_LEGACY_DIR` | `/data/workspace` if upgrading from the old single-workspace layout |

### Aristotle CLI and Lean workspace in the container

The Docker image installs **`aristotlelib`** from PyPI (provides the `aristotle` CLI on `PATH`). Each **campaign** gets its own seeded Lake project under `WORKSPACE_ROOT/<campaign_id>/` when the campaign is created.

### Moonshot note

This app sends `response_format: { "type": "json_object" }` on decomposition and reasoning calls. If Moonshot returns an error for that field, set `LLM_JSON_MODE=0` or open an issue.

## Database schema notes (migrations)

SQLite **`PRAGMA user_version`** drives migrations on startup.

- **`campaigns.workspace_template`** — `minimal` or `mathlib`.
- **`experiments`**: `parsed_*_json` columns and `parsed_error_message` mirror `AristotleParsedResult`.
- **`lemma_ledger`** — append-only rows keyed by campaign / target / experiment / label / status (`proved` \| `attempted` \| `blocked`).
- **`ops_counters`** — failure and error class tallies for admin.
- **`manager_tick_diagnostics`** — last exception metadata per campaign.

Existing `orchestrator.db` files pick up new columns and tables automatically on next process start.

## Operator checklist (“hard campaigns”)

1. **Isolation**: confirm each active campaign has a distinct directory under `WORKSPACE_ROOT` (shown on the campaign page).
2. **Mathlib**: if using `mathlib`, run `lake exe cache get` in that campaign directory once; watch disk and logs for first-build time.
3. **Caps**: verify `MAX_EXPERIMENTS` / `MAX_ACTIVE_EXPERIMENTS` / `TICK_INTERVAL` via the dashboard footer, `/admin/config`, or env.
4. **Evidence**: completed experiments should show parsed lists and ledger entries; the LLM user payload includes “Recent structured experiment results” and “Lemma / obligation ledger”.
5. **Health**: `/health` and (with token) `/admin/status` show DB and disk; structured logs include `duration_ms` per tick.
6. **Regression**: `pytest tests/` passes after changes.

## Templates

- `templates/dashboard.html` — full shell (sidebar, new-campaign form with template, HTMX polling container).
- `templates/campaign_panel.html` — main campaign view; returned by `GET /api/campaign/{id}/state` for HTMX `innerHTML` swaps.

## License

Use and modify per your project policy.
