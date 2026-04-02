#!/usr/bin/env bash
# Pull /data/orchestrator.db via base64 (raw cat over railway ssh corrupts SQLite on many setups).
# Usage: SOURCE_SERVICE=aristotle-orchestrator OUT=/tmp/orchestrator.db ./scripts/railway-pull-db-from-service.sh
set -euo pipefail
SOURCE_SERVICE="${SOURCE_SERVICE:-aristotle-orchestrator}"
OUT="${OUT:-/tmp/orchestrator.db}"
echo "Pulling from ${SOURCE_SERVICE} -> ${OUT} (base64)"
railway ssh -s "${SOURCE_SERVICE}" "base64 /data/orchestrator.db" | base64 -d > "${OUT}"
wc -c "${OUT}"
sqlite3 "${OUT}" "PRAGMA integrity_check; SELECT COUNT(*) AS campaigns FROM campaigns;"
