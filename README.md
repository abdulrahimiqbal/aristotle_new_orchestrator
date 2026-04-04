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

## Problem map (JSON structure)

Each campaign stores **`campaigns.problem_map_json`**: a single JSON object the manager refreshes on a schedule (see `MAP_REFRESH_MAX_INTERVAL_TICKS` / manager tick). It is **not** the same as verification **targets** (those are separate rows); the map is a **landscape graph** for the LLM and dashboard.

| Field | Role |
|--------|------|
| **`summary`** | Short narrative: difficulty, strategy, what changed. |
| **`nodes`** | List of `{ "id", "label", "status", "kind", "obligations"? }`. Optional **`obligations`**: up to **5** short strings (what would check or falsify the node). Max **40** nodes after `coerce_llm_problem_map` (`problem_map_util.py`). |
| **`edges`** | List of `{ "from", "to", "kind" }` between node ids (`would_imply`, `special_case`, `equivalent`, `relates`, …). |
| **`active_fronts`** | Node ids where the planner should focus experiments next. |
| **`last_tick_updated`** | Manager tick number when the map was last written. |
| **`library_anchors`** / **`library_recon_done`** | Optional Mathlib / LeanSearch payload from broad recon (see `mathlib_knowledge.py`). |

**Node `status`:** `open`, `active`, `blocked`, `proved`, `refuted` (drives progress counts on the dashboard).

**Node `kind`:** semantic **role** of the node (orthogonal to status). Allowed values are normalized in `problem_map_util.normalize_node_kind`:

| `kind` | Meaning |
|--------|---------|
| `claim` | Default — standard lemma or subgoal toward the main result. |
| `hypothesis` | Explicit provisional assumption; not yet established. |
| `finite_check` | Decidable / bounded / computational slice. |
| `literature_anchor` | Tied to external refs (`problem_refs`) or given literature. |
| `obstruction` | Known hard gate or bottleneck. |
| `exploration` | Scouting node; expect refinement or split. |
| `equivalence` | Alternate formulation key to the main question. |

New campaigns get a seed map (`seed_problem_map_json`) with a single **`root`** node (`kind: claim`). The **map refresh** LLM (`update_problem_map` in `llm.py`) is instructed to set `kind` on each node; invalid values fall back to **`claim`**.

**Experiment `move_kind`** (on each Aristotle run) is separate: it classifies the **tactic** for that job (`prove`, `refute`, `explore`, …). See `ALLOWED_MOVE_KINDS` in `problem_map_util.py` and the manager prompt in `llm.py`.

## Research methodology (where it lives in code)

These pieces support **serious open-problem** campaigns (cartography, mixed tactics, safer “proved” signals). Each row points to the primary implementation.

| Capability | Behavior | Primary files |
|------------|----------|----------------|
| **Verdict ↔ summary reconciliation** | After LLM summarization, if structured verdict is still `inconclusive` but the summary strongly indicates success *and* there are proved lemmas (or similar), promote to **`proved`** and append `verdict_reconciled:summary_heuristic` to **`parse_warnings`**. Disable with env. | `verdict_reconcile.py`, `manager.py` (after `summarize_result`), `config.py` (`VERDICT_RECONCILE_FROM_SUMMARY`) |
| **Map node obligations** | Cartographer may attach **`obligations`** strings per node; coerced and shown under each node on the dashboard. | `problem_map_util.py` (`coerce_obligations`), `llm.py` (`update_problem_map` prompt), `campaign_panel.html` |
| **Literature anchors** | Prompt instructs: do not invent `literature_anchor` nodes without **`problem_refs`**. | `llm.py` |
| **Research packet injection** | Campaign-scoped operator packet stores frontier state, attack families, anti-goals, finite checks, and references; the manager sees only that campaign’s packet and the dashboard exposes an in-place JSON editor. | `research_packets.py`, `db.py` (`research_packet_json`), `llm.py`, `app.py`, `campaign_panel.html` |
| **Move-kind diversity** | If every new experiment in a batch is `prove` and **`MANAGER_MIN_NON_PROVE_MOVES_PER_BATCH` ≥ 1**, append one **`explore`** experiment (policy note in `move_note`). | `manager_policy.py` (`ensure_move_kind_diversity`), `manager.py`, `config.py` |
| **Skeptic pass** | Optional **second** LLM call adds up to **`SKEPTIC_PASS_MAX_EXPERIMENTS`** extra jobs with `refute` / `explore` to stress-test the primary plan. | `llm.py` (`reason_skeptic`), `manager.py`, `config.py` |
| **Proved gate on map** | Nodes whose **`kind`** is in **`MAP_PROVED_GATE_KINDS`** cannot remain **`proved`** in stored JSON until **`POST /admin/map-node-ack`** records an ack (otherwise coerced to **`active`** after each map refresh). | `manager_policy.py` (`apply_map_proved_gate`), `db.py` (`campaign_map_node_acks`, `list_map_node_acks`, `add_map_node_ack`), `admin_routes.py` |
| **Operator metrics** | **`GET /admin/metrics`**: completed experiments by **verdict** and **move_kind**, count with reconciliation warnings, total map acks. | `db.py` (`get_operator_metrics`), `admin_routes.py` |

Tests: `tests/test_verdict_reconcile.py`, `tests/test_manager_policy.py`, `tests/test_problem_map_util.py` (obligations coercion).

## Global Shadow Manager (Collatz lab)

The dashboard now includes a dedicated **Shadow manager** workspace (`/shadow`) separate from live campaign tabs.

- **Mission-first mode**: one global objective (`SHADOW_GLOBAL_GOAL`, default Collatz) with cross-campaign memory.
- **Aggressive ideation, grounded promotion**: shadow can propose speculative frameworks, but only approved promotions create live targets/experiments.
- **Invention-first discipline**: shadow is for inventing new mathematics and proof-program structure; live promotions are scarce grounding requests, not the main product.
- **Promotion rubric**: shadow scores every grounding request on novelty, proof-program leverage, grounding need, expected signal, and queue fit before it can enter the live queue.
- **Supershadow lineage intake**: approved Supershadow concepts enter Shadow as tracked incubations, and Shadow can preserve lineage by citing `source_incubation_ids` on descendant hypotheses and promotions.
- **Structured backward-chaining output**: global runs normalize and store `solved_world`, assumptions, bridge lemmas, and Lean landing hints.
- **Hypothesis ranking**: each global hypothesis stores `score_0_100`, `groundability_tier`, and a `kill_test` to prioritize falsifiable routes.
- **Autonomous loop**: optional background auto-runs keep refining the proof program; when the live queue is too full, new promotions are suppressed instead of stopping ideation.
- **Reproducibility metadata**: each global run stores prompt/model hash metadata in `shadow_global_run.response_json`.

Endpoints:

| Method | Path | Notes |
|--------|------|-------|
| GET | `/shadow` | Dedicated global shadow dashboard tab |
| GET | `/api/shadow/panel` | HTMX fragment for shadow panel |
| POST | `/api/shadow/run` | Manual global shadow run |
| POST | `/api/shadow/promote/{id}/approve` | Approve global promotion (optionally immediate Aristotle submit) |
| POST | `/api/shadow/promote/{id}/reject` | Reject global promotion |
| GET | `/api/shadow/ops` | Queue depth, latest run meta/warnings, auto-loop config snapshot |

## Global Supershadow Lab (concept incubation)

The dashboard also includes a dedicated **Supershadow Lab** (`/supershadow`) for ontology-expanding conceptual search.

- **Zero live authority**: Supershadow cannot create targets, experiments, or direct Aristotle work.
- **Breakthrough-first search**: discovery runs now optimize for the strongest worldview shift on the board, not for producing several equally polite concepts.
- **Distill-second workflow**: Supershadow first hunts 1-3 candidate worldviews, then sharpens only the best survivor into a kill-test, smallest transfer probe, and optional first bridge.
- **Compression over novelty**: concepts are judged by whether they make several grounded facts feel structural at once, not by how exotic they sound.
- **Conceptual handoffs, not queue spam**: only the strongest surviving concept should emit a Shadow-facing handoff, and only after distillation sharpens it.
- **Incubation layer**: approving a handoff creates a tracked incubation packet rather than a live task. This preserves the conceptual leap as a first-class object instead of dissolving it into generic Shadow context.
- **Lineage tracking**: Shadow can operationalize an incubation by citing its `source_incubation_ids`; approved live promotions can then mark that incubation as grounded contact with reality.
- **Lifecycle visibility**: incubations move through states such as `incubating`, `operationalized`, and `grounded`, with event history shown in the Supershadow UI.
- **Family discovery control**: Supershadow concepts now carry `concept_family`, `family_kind` (`established|adjacent|new`), `parent_family`, `smallest_transfer_probe`, and explicit “why this is not the same family again” text.
- **Worldview-first ranking**: concept ranking now prefers compression, fit to stubborn facts, ontological delta, and falsifiability before transfer-readiness.
- **Anti-circling pressure**: repeated families that have not produced incubations or grounded descendants accumulate saturation penalty, but the system no longer forces artificial family diversity when one line looks substantially stronger.
- **Stale-family suppression**: once a family repeats without transfer, Supershadow no longer just ranks it lower; normalization can drop it entirely unless the run presents a materially cheaper probe and a concrete explanation of what changed.

Concept transfer flow:

```
Supershadow concept
  -> Supershadow handoff
  -> approved incubation packet
  -> Shadow hypothesis / bridge lemma / promotion (cites source_incubation_ids)
  -> optional live grounding via Aristotle
```

Supershadow family doctrine:

- `established` family: deepen a promising line, but only if the next probe is sharper than before.
- `adjacent` family: branch from an existing line with a meaningfully different object or interface.
- `new` family: introduce a genuinely different conceptual language, but still tether it to grounded facts and a smallest transfer probe.

Operationally, a Supershadow run now behaves like:

```
grounded facts + pressure map
  -> discovery pass (1-3 worldview candidates, no live authority)
  -> pick dominant survivor
  -> distillation pass (sharpest kill-test, smallest transfer probe, optional first bridge)
  -> optional Shadow handoff for that survivor only
```

The system is intentionally biased against abstract repetition: a family that keeps reappearing without incubation or grounding must either lower the transfer cost or lose rank, but a genuinely improving line can now stay in focus across runs instead of being diluted for variety.
Family saturation is computed from system memory rather than trusting the model's self-score, so a stale family cannot simply label itself "novel" and escape the penalty.

Endpoints:

| Method | Path | Notes |
|--------|------|-------|
| GET | `/supershadow` | Dedicated global Supershadow dashboard tab |
| GET | `/api/supershadow/panel` | HTMX fragment for the Supershadow panel |
| POST | `/api/supershadow/run` | Manual Supershadow conceptual sweep |
| POST | `/api/supershadow/handoff/{id}/approve` | Approve a conceptual handoff into a tracked Shadow incubation |
| POST | `/api/supershadow/handoff/{id}/reject` | Reject a conceptual handoff |
| GET | `/api/supershadow/ops` | Handoff counts, incubation counts, and latest run metadata |

## Workspaces and Mathlib

- **Per-campaign directories**: each campaign has its own Lake project at `WORKSPACE_ROOT/<campaign_id>/` (isolated `lean-toolchain`, `lakefile.lean`, and `OrchWorkspace/`). Aristotle uses that path as `--project-dir`.
- **Templates** (dashboard “Use Mathlib4 workspace”, `DEFAULT_WORKSPACE_TEMPLATE`, or JSON `workspace_template` / `use_mathlib`); **LeanSearch hints** (second checkbox or JSON `use_mathlib_knowledge`) require `MATHLIB_KNOWLEDGE_MODE=leansearch` on the server:
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
| `MAX_ACTIVE_EXPERIMENTS` | Max concurrent in-flight Aristotle jobs per campaign (default: `3`; Tier-0-friendly) |
| `TICK_INTERVAL` | Seconds between manager ticks (default: `60`) |
| `MAX_EXPERIMENTS` | Max total experiments per campaign (default: `100`) |
| `LLM_JSON_MODE` | Set to `0` to disable `response_format: json_object` if your API rejects it (default: on) |
| `LLM_EVIDENCE_TARGET_TAIL` | Max target evidence lines shown to the LLM (default: `24`) |
| `LLM_EXPERIMENT_SUMMARY_CHARS` | Truncation for experiment summaries in the LLM user payload (default: `4000`) |
| `LLM_TICK_REASONING_CHARS` | Truncation for prior tick reasoning (default: `4000`) |
| `LLM_RECENT_STRUCTURED_EXPERIMENTS` | Recent completed experiments with parsed fields injected into the LLM context (default: `12`) |
| `LLM_LEDGER_ENTRIES_LIMIT` | Recent lemma-ledger rows injected into the LLM context (default: `40`) |
| `LLM_SUMMARIZE_INPUT_CHARS` | Max raw Aristotle text sent to the summarizer call (default: `50000`) |
| `LLM_SUMMARIZE_MAX_LLM_CALLS_PER_TICK` | Per tick, only this many completed experiments use LLM summarize; others get truncated text (default: `2`) |
| `LLM_MIN_SECONDS_BETWEEN_REQUESTS` | Minimum spacing between any two LLM HTTP calls in-process (default: `3.5`) |
| `LLM_MAX_RETRIES_429` | Extra retries on HTTP 429 with backoff (default: `12`) |
| `MATHLIB_KNOWLEDGE_MODE` | `off` (default) or `leansearch` — allow Mathlib hints via [LeanSearch](https://leansearch.net/) when a campaign opts in (dashboard checkbox **LeanSearch Mathlib hints** or JSON `use_mathlib_knowledge`) |
| `LEANSEARCH_API_URL` | POST endpoint (default: `https://leansearch.net/search`) |
| `LEAN_TOOLCHAIN_HINT` | Optional string shown in hints so the planner matches your Lake `lean-toolchain` / Mathlib pin |
| `MATHLIB_BROAD_QUERIES_COUNT` | Natural-language broad queries from prompt + targets (default: `2`) |
| `MATHLIB_BROAD_RESULTS_PER_QUERY` | Hits per query for broad recon (default: `4`) |
| `MATHLIB_NARROW_MAX_SYMBOLS` | Max symbol queries per tick from blockers/errors (default: `8`; set `0` to disable narrow) |
| `MATHLIB_NARROW_RESULTS_PER_SYMBOL` | LeanSearch hits per symbol (default: `2`) |
| `MATHLIB_CONTEXT_MAX_CHARS` | Total budget for Mathlib sections in the LLM user payload (default: `8000`) |
| `VERDICT_RECONCILE_FROM_SUMMARY` | If `1` (default), promote inconclusive→proved when summary + lemma signals align (`verdict_reconcile.py`) |
| `MANAGER_MIN_NON_PROVE_MOVES_PER_BATCH` | If ≥1 and a tick’s plan is all `prove`, inject one `explore` experiment (default: `0`) |
| `SKEPTIC_PASS_ENABLED` | If `1`, run second LLM pass for extra `refute`/`explore` experiments (default: off) |
| `SKEPTIC_PASS_MAX_EXPERIMENTS` | Cap on skeptic experiments per tick (default: `2`) |
| `MAP_PROVED_GATE_KINDS` | Comma-separated node kinds that cannot stay `proved` without `POST /admin/map-node-ack` (default: `obstruction,equivalence`; empty disables gate) |
| `ADMIN_TOKEN` | If set, enables `/admin/status`, `/admin/config`, `/admin/metrics`, `/admin/map-node-ack`, `/admin/ui` (use `Authorization: Bearer`, `X-Admin-Token`, or `?admin_token=`; prefer headers) |
| `SHADOW_LLM_MODEL` | Optional model override for shadow runs (`None` falls back to `LLM_MODEL`) |
| `SHADOW_LLM_TEMPERATURE` | Shadow run temperature (default: `0.85`) |
| `SHADOW_GLOBAL_GOAL` | Global shadow mission text shown in `/shadow` |
| `MANAGER_SUBMIT_PENDING_EXPERIMENTS` | If `1` (default), manager submits pending experiments (including approved shadow promotions) on tick |
| `SHADOW_ARISTOTLE_IMMEDIATE_ON_APPROVE` | If `1` (default), approving a `new_experiment` promotion can call Aristotle immediately |
| `SHADOW_GLOBAL_AUTO_ENABLED` | Enable autonomous global shadow loop (default: on) |
| `SHADOW_GLOBAL_TICK_INTERVAL_SEC` | Interval for auto global shadow runs (default: `180`) |
| `SHADOW_GLOBAL_MAX_PENDING_PROMOTIONS` | Soft cap for pending live promotions; above this, auto-runs still think but emit no new promotions (default: `24`) |
| `SHADOW_GLOBAL_MAX_PROMOTIONS_PER_RUN` | Cap on total global promotions kept from a run after normalization (default: `3`) |
| `SHADOW_GLOBAL_MAX_EXPERIMENT_PROMOTIONS_PER_RUN` | Cap on `new_experiment` promotions kept from a run after normalization (default: `2`) |
| `SUPERSHADOW_LLM_MODEL` | Optional model override for Supershadow runs (`None` falls back to `SHADOW_LLM_MODEL` then `LLM_MODEL`) |
| `SUPERSHADOW_LLM_TEMPERATURE` | Supershadow run temperature (default: `0.95`) |
| `SUPERSHADOW_GLOBAL_GOAL` | Global conceptual-search mission text shown in `/supershadow` |
| `SUPERSHADOW_GLOBAL_AUTO_ENABLED` | Enable autonomous global Supershadow loop (default: on) |
| `SUPERSHADOW_GLOBAL_TICK_INTERVAL_SEC` | Interval for auto Supershadow sweeps (default: `900`) |
| `SUPERSHADOW_MAX_HANDOFFS_PER_RUN` | Cap on Shadow-facing conceptual handoffs emitted from one Supershadow run (default: `2`) |
| `SUPERSHADOW_MAX_PENDING_HANDOFFS` | Soft cap for pending Supershadow handoffs; above this, auto-runs keep ideating but emit no new handoffs (default: `18`) |

## HTTP API (selected)

| Method | Path | Notes |
|--------|------|--------|
| POST | `/api/campaign` | Form: `prompt`, optional `use_mathlib=1` (Mathlib Lake), `use_mathlib_knowledge=1` (LeanSearch hints); optional legacy `workspace_template` if checkboxes omitted |
| POST | `/api/campaign/start` | JSON: `{"prompt":"...","workspace_template":"minimal"}` or `"use_mathlib": true`; optional `"use_mathlib_knowledge": true` (LeanSearch hints, needs `MATHLIB_KNOWLEDGE_MODE=leansearch`) → `201` with `campaign_id`, `workspace_dir`, `mathlib_knowledge` |
| GET | `/api/campaign/{id}/ledger` | Read-only ledger JSON (`limit` query, capped) |
| POST | `/api/campaign/{id}/research-packet` | Update the campaign-scoped research packet. Accepts form `research_packet_json` for dashboard edits or JSON `{ "research_packet": {...} }`. |
| GET | `/admin/metrics` | With `ADMIN_TOKEN`: aggregates for methodology monitoring (verdicts, move kinds, reconciliation count) |
| POST | `/admin/map-node-ack` | With `ADMIN_TOKEN`: JSON `campaign_id` + `node_id` for **proved**-gate ack (see `MAP_PROVED_GATE_KINDS`) |

## Admin / observability

With **`ADMIN_TOKEN`** set:

- **`GET /admin/status`** — database connectivity, workspace root existence, disk usage for `WORKSPACE_ROOT`, last per-campaign tick exception metadata, `ops_counters` (Aristotle submit failure classes, LLM errors, etc.).
- **`GET /admin/config`** — effective caps, LLM context limits, and methodology flags (no secrets).
- **`GET /admin/metrics`** — completed experiments by verdict and `move_kind`, count of runs with `verdict_reconciled` in `parse_warnings`, total map-node acks.
- **`POST /admin/map-node-ack`** — JSON body `{"campaign_id":"…","node_id":"…"}` to allow gated map kinds to remain `proved` after human review.
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
- **`campaigns.mathlib_knowledge`** — `0`/`1`: per-campaign opt-in for LeanSearch Mathlib hints (with server `MATHLIB_KNOWLEDGE_MODE=leansearch`).
- **`experiments`**: `parsed_*_json` columns and `parsed_error_message` mirror `AristotleParsedResult`.
- **`lemma_ledger`** — append-only rows keyed by campaign / target / experiment / label / status (`proved` \| `attempted` \| `blocked`).
- **`ops_counters`** — failure and error class tallies for admin.
- **`manager_tick_diagnostics`** — last exception metadata per campaign.
- **`campaign_map_node_acks`** — operator acks for **proved**-gate map nodes (`campaign_id`, `node_id`).
- **`shadow_epistemic_state` / `shadow_hypothesis` / `shadow_run` / `shadow_promotion_request`** — per-campaign shadow lab state, hypotheses, runs, and promotion queue.
- **`shadow_global_state` / `shadow_global_hypothesis` / `shadow_global_run` / `shadow_global_promotion_request` / `shadow_global_evidence_link`** — global shadow manager state and artifacts.
- **`shadow_global_hypothesis.score_0_100` / `groundability_tier` / `kill_test`** — persisted ranking + fast-falsification fields for global hypotheses.

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
