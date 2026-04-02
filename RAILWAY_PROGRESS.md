# Railway: access job / campaign progress

Quick reference for seeing **what’s running in production** from your laptop (IDE terminal).

## 1. Point the CLI at the right service

From the repo root:

```bash
railway login          # once
railway link           # if this folder isn’t linked yet
railway service link aristotle-orchestrator   # service that has your /data volume + DB
# or: aristotle_new_orchestrator  (GitHub deploy — often a separate volume)
railway status
```

Use the service that actually mounts **`/data`** with `orchestrator.db` (where your campaigns live).

## 2. Logs (HTTP + LLM, not full campaign state)

```bash
railway logs -n 150 --latest
```

## 3. SQLite on the volume (full truth)

```bash
# Campaigns
railway ssh "sqlite3 /data/orchestrator.db \"SELECT id, status, substr(prompt,1,100) FROM campaigns ORDER BY created_at DESC;\""

# Experiments for one campaign
railway ssh "sqlite3 /data/orchestrator.db \"
  SELECT id, status, verdict, substr(objective,1,80) FROM experiments
  WHERE campaign_id='YOUR_CAMPAIGN_ID';\""

# Manager ticks (reasoning preview)
railway ssh "sqlite3 /data/orchestrator.db \"
  SELECT tick_number, substr(reasoning,1,400) FROM ticks
  WHERE campaign_id='YOUR_CAMPAIGN_ID' ORDER BY id DESC LIMIT 8;\""
```

Replace `YOUR_CAMPAIGN_ID` with the id from the URL `/campaign/<id>` or the first query.

**Tip:** target a specific service if needed:

```bash
railway ssh -s aristotle-orchestrator "sqlite3 /data/orchestrator.db '.tables'"
```

## 4. HTTP (public URL)

```bash
export BASE="https://YOUR-APP.up.railway.app"

curl -sS "$BASE/health" | jq
curl -sS "$BASE/api/campaign/YOUR_CAMPAIGN_ID/ledger" | jq
```

Full UI: open `$BASE/campaign/YOUR_CAMPAIGN_ID` in a browser.

## 5. Admin JSON (if `ADMIN_TOKEN` is set in Railway)

```bash
curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" "$BASE/admin/status" | jq
```

**Full DB-style export (no SSH):** `GET /admin/export` — same Bearer token. From repo: `BASE=$BASE ADMIN_TOKEN=$ADMIN_TOKEN ./scripts/pull-railway-export.sh snapshot.json`. See **`GITHUB_DEPLOYMENT.md`**.

---

More detail (GitHub deploy, env vars, two-service setup): see **`GITHUB_DEPLOYMENT.md`**.
