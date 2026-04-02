#!/usr/bin/env bash
# Pull full orchestrator state from a deployed Railway (or any) URL over HTTPS.
# Requires ADMIN_TOKEN set on the service (same value you use locally).
#
# Usage:
#   export BASE="https://your-service.up.railway.app"
#   export ADMIN_TOKEN="…"
#   ./scripts/pull-railway-export.sh                    # writes railway-export.json
#   ./scripts/pull-railway-export.sh my-snapshot.json
#
# Optional query flags (append to URL):
#   INCLUDE_RAW=1       # include experiment result_raw (large)
#   TICKS_LIMIT=8000
#   LEDGER_LIMIT=50000

set -euo pipefail

BASE="${BASE:-${RAILWAY_PUBLIC_URL:-}}"
TOKEN="${ADMIN_TOKEN:-}"
OUT="${1:-railway-export.json}"

if [[ -z "$BASE" || -z "$TOKEN" ]]; then
  echo "Set BASE or RAILWAY_PUBLIC_URL and ADMIN_TOKEN" >&2
  exit 1
fi

BASE="${BASE%/}"
QS="ticks_limit=${TICKS_LIMIT:-5000}&ledger_limit=${LEDGER_LIMIT:-20000}&result_raw_max_chars=${RESULT_RAW_MAX_CHARS:-500000}"
if [[ "${INCLUDE_RAW:-0}" == "1" ]]; then
  QS+="&include_result_raw=true"
else
  QS+="&include_result_raw=false"
fi

curl -fsS -H "Authorization: Bearer ${TOKEN}" \
  "${BASE}/admin/export?${QS}" \
  -o "${OUT}"

echo "Wrote ${OUT}"
