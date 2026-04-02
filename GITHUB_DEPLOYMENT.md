# GitHub + Railway deployment (cached notes)

This file captures how this repo is wired to **GitHub** and **Railway**, and how to **inspect live progress from your IDE** (terminal / HTTP), without relying on dashboard memory.

## GitHub

| Item | Value |
|------|--------|
| **Remote** | `https://github.com/abdulrahimiqbal/aristotle_new_orchestrator` |
| **Default branch** | `main` |
| **Local clone** | Same machine: repo path you use with Cursor / `railway link` |

Push updates:

```bash
git add -A && git commit -m "…" && git push origin main
```

---

## Railway (high level)

| Item | Value |
|------|--------|
| **Project name** | `aristotle-orchestrator` (example from CLI `railway status`) |
| **Environment** | `production` |
| **Build** | Root `Dockerfile`; config from `railway.toml` (Dockerfile builder, `/health`, timeout 120s) |
| **Typical volume** | Mount at **`/data`** for SQLite + workspaces |
| **Paths in container** | `DATABASE_PATH=/data/orchestrator.db`, `WORKSPACE_ROOT=/data/workspaces`, legacy migration source `WORKSPACE_LEGACY_DIR=/data/workspace` (see `Dockerfile` / `README.md`) |

### Two services (important)

At one point the project had:

1. **`aristotle-orchestrator`** — deployed via **`railway up`** (upload from laptop). Holds **env vars**, **volume**, and **public URL** you already used.
2. **`aristotle_new_orchestrator`** — added with **`railway add --repo abdulrahimiqbal/aristotle_new_orchestrator`** (GitHub → auto deploy on `main`).

**Variables are per service** unless you use **shared environment / project variables**. The GitHub service does **not** automatically inherit secrets from the CLI service—mirror them (or use shared vars) before relying on it.

**CLI link** (which service `railway logs` / `railway ssh` target):

```bash
cd /path/to/aristotle_new_orchestrator
railway service link aristotle_new_orchestrator   # GitHub-backed
# or
railway service link aristotle-orchestrator      # upload-backed
```

### Connect GitHub to Railway (first time)

1. Railway dashboard → **account/workspace** → connect **GitHub** and grant access to this repo.
2. Then from the repo: `railway add --repo abdulrahimiqbal/aristotle_new_orchestrator`  
   (If you see **repo not found**, GitHub app access isn’t granted yet.)

### Sync env to Railway (optional script)

`scripts/sync-railway-env.sh` — keep `.env` out of git; use Railway **Variables** UI or this script with a filled `.env`.

---

## Admin / ops HTTP (if `ADMIN_TOKEN` is set)

```bash
export RAILWAY_PUBLIC_URL="https://YOUR-SERVICE.up.railway.app"
export ADMIN_TOKEN="…"

curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" \
  "$RAILWAY_PUBLIC_URL/admin/status" | jq

curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" \
  "$RAILWAY_PUBLIC_URL/admin/config" | jq
```

**Full snapshot (recommended vs `railway ssh` + `sqlite3`):** `GET /admin/export` returns all campaigns, targets, experiments (summaries + parsed fields; raw Aristotle text optional), ticks, lemma ledger, and ops counters as one JSON object. Same auth as above.

```bash
export BASE="$RAILWAY_PUBLIC_URL"
./scripts/pull-railway-export.sh orchestrator-snapshot.json
# Large raw logs: INCLUDE_RAW=1 ./scripts/pull-railway-export.sh with-raw.json
```

Mirror **`ADMIN_TOKEN`** on **each** Railway service if you use two deployments; call each service’s public URL separately to pull both databases.

There is also **`GET /admin/ui`** (HTML operator panel).

---

# Accessing results / progress from your IDE

The app stores campaign state in **SQLite on the Railway volume** (`/data/orchestrator.db`). There is **no magic sync** of that file into your workspace; you reach it over **HTTP** (limited JSON) or **Railway CLI + SSH**.

## 1. Terminal in the repo (recommended)

**Prerequisites:** [Railway CLI](https://docs.railway.com/guides/cli) installed, `railway login` done, directory **`railway link`**’ed to the **service that has the volume** (usually the one you actually use in production).

```bash
cd /path/to/aristotle_new_orchestrator
railway status
railway logs -n 150 --latest
```

**Remote SQL (full truth for targets / experiments / ticks):**

```bash
# List campaigns
railway ssh "sqlite3 /data/orchestrator.db \"SELECT id, status, substr(prompt,1,80) FROM campaigns;\""

# Experiments by status for one campaign
railway ssh "sqlite3 /data/orchestrator.db \"
  SELECT status, verdict, COUNT(*) FROM experiments WHERE campaign_id='YOUR_CAMPAIGN_ID' GROUP BY status, verdict;
\""

# Last ticks (manager reasoning preview)
railway ssh "sqlite3 /data/orchestrator.db \"
  SELECT tick_number, substr(reasoning,1,300) FROM ticks WHERE campaign_id='YOUR_CAMPAIGN_ID' ORDER BY id DESC LIMIT 5;
\""
```

Use your real **`campaign_id`** (from the web UI URL `/campaign/<id>` or the first query).

## 2. HTTP from IDE (no DB shell)

Replace `BASE` with your public Railway URL (no trailing slash).

| Endpoint | Notes |
|----------|--------|
| `GET /health` | JSON: DB touch + active campaign count. |
| `GET /api/campaign/{id}/ledger?limit=200` | **JSON** lemma ledger for that campaign. |
| `GET /campaign/{id}` | Full **HTML** dashboard page (open in browser). |
| `GET /api/campaign/{id}/state` | **HTML** fragment (same as HTMX poll)—not ideal for parsing in IDE. |

Examples:

```bash
export BASE="https://YOUR-SERVICE.up.railway.app"
curl -sS "$BASE/health" | jq
curl -sS "$BASE/api/campaign/YOUR_CAMPAIGN_ID/ledger" | jq
```

**Cursor / VS Code:** use the integrated terminal for `curl`, or a **`.http` / REST Client** file with the same requests.

## 3. Browser

Open `https://YOUR-SERVICE.up.railway.app/campaign/<campaign_id>` — same data as production UI; no IDE sync required.

## 4. SSH + SQLite (fragile quoting)

- Prefer **`GET /admin/export`** when `ADMIN_TOKEN` is set (see above).
- **`railway ssh`** with inline `sqlite3` strings often breaks on parentheses, commas in prompts, or WebSocket resets; if you still use it, run the SQL from a **single** remote `sh -c` with the query in **double quotes** inside **single-quoted** `-c` (see `RAILWAY_PROGRESS.md`).
- **`railway run`** runs commands **on your laptop** with Railway **env vars** injected—it does **not** attach to `/data/orchestrator.db` on the server.

---

## Quick checklist: “I want IDE access to live progress”

1. `railway link` + `railway service link <the service with /data volume>`.
2. `railway ssh "sqlite3 /data/orchestrator.db '…'"` for queries **or** `curl …/ledger` + `/health`.
3. Keep **`BASE` URL** and **`campaign_id`** in a scratch `.http` file or shell alias (don’t commit secrets).

---

## Related docs

- `README.md` — env vars, local run, Docker, Railway volume paths.
- [Railway CLI](https://docs.railway.com/guides/cli) — `up`, `add --repo`, `logs`, `ssh`, `variables`.
