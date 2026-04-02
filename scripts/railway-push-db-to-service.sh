#!/usr/bin/env bash
# Push orchestrator.db via base64 (raw cat over railway ssh corrupts or hangs SQLite).
# Usage: TARGET_SERVICE=aristotle_new_orchestrator IN=/tmp/orchestrator.db ./scripts/railway-push-db-to-service.sh
set -euo pipefail
TARGET_SERVICE="${TARGET_SERVICE:-aristotle_new_orchestrator}"
IN="${IN:-/tmp/orchestrator.db}"
test -f "${IN}" || { echo "Missing file: ${IN}" >&2; exit 1; }
BYTES=$(wc -c < "${IN}" | tr -d ' ')
echo "Pushing ${IN} (${BYTES} bytes) -> ${TARGET_SERVICE} (base64; ~30s for 1MB, do not Ctrl+C)"
base64 "${IN}" | railway ssh -s "${TARGET_SERVICE}" "base64 -d > /data/orchestrator.db.new"
railway ssh -s "${TARGET_SERVICE}" "mv /data/orchestrator.db.new /data/orchestrator.db && chmod 644 /data/orchestrator.db"
railway ssh -s "${TARGET_SERVICE}" "sqlite3 /data/orchestrator.db 'PRAGMA integrity_check; SELECT COUNT(*) AS campaigns FROM campaigns;'"
echo "Done. Restarting ${TARGET_SERVICE}..."
railway restart -s "${TARGET_SERVICE}"
echo "Restart triggered. Dashboard should list campaigns after the new deploy is up (~1–2 min)."
