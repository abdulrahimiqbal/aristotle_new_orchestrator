#!/usr/bin/env bash
# One-shot: copy orchestrator.db from SOURCE_SERVICE to TARGET_SERVICE using base64 only.
set -euo pipefail
SOURCE_SERVICE="${SOURCE_SERVICE:-aristotle-orchestrator}"
TARGET_SERVICE="${TARGET_SERVICE:-aristotle_new_orchestrator}"
TMP="${TMP:-/tmp/orchestrator.railway.migrate.$$}.db"
echo "=== migrate DB: ${SOURCE_SERVICE} -> ${TARGET_SERVICE} ==="
railway ssh -s "${SOURCE_SERVICE}" "base64 /data/orchestrator.db" | base64 -d > "${TMP}"
sqlite3 "${TMP}" "PRAGMA integrity_check; SELECT COUNT(*) FROM campaigns;"
echo "=== uploading (base64) ==="
base64 "${TMP}" | railway ssh -s "${TARGET_SERVICE}" "base64 -d > /data/orchestrator.db.new"
railway ssh -s "${TARGET_SERVICE}" "mv /data/orchestrator.db.new /data/orchestrator.db && chmod 644 /data/orchestrator.db"
railway ssh -s "${TARGET_SERVICE}" "sqlite3 /data/orchestrator.db 'PRAGMA integrity_check; SELECT COUNT(*) FROM campaigns;'"
rm -f "${TMP}"
echo "=== restart ==="
railway restart -s "${TARGET_SERVICE}"
echo "OK — refresh dashboard in ~1–2 minutes."
