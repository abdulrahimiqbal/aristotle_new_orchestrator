#!/usr/bin/env bash
# Copy SQLite + /data/workspaces from one Railway service to another (same project).
# Use when GitHub-deployed service has an empty DB but an older service has your campaigns.
#
# Prerequisites: railway CLI, `railway link`, logged in.
# Defaults match this repo's historical two-service setup.
#
# Usage:
#   ./scripts/migrate-railway-orchestrator-data.sh
#   SOURCE_SERVICE=aristotle-orchestrator TARGET_SERVICE=aristotle_new_orchestrator ./scripts/migrate-railway-orchestrator-data.sh

set -euo pipefail
SOURCE_SERVICE="${SOURCE_SERVICE:-aristotle-orchestrator}"
TARGET_SERVICE="${TARGET_SERVICE:-aristotle_new_orchestrator}"
TMP_DIR="${TMP_DIR:-$(mktemp -d)}"
DB_LOCAL="${TMP_DIR}/orchestrator.db"
TAR_LOCAL="${TMP_DIR}/workspaces.tar"

echo "==> Source:  ${SOURCE_SERVICE}"
echo "==> Target:  ${TARGET_SERVICE}"
echo "==> Staging: ${TMP_DIR}"

echo "==> Pulling database from source..."
railway ssh -s "${SOURCE_SERVICE}" "cat /data/orchestrator.db" > "${DB_LOCAL}"
ls -la "${DB_LOCAL}"

echo "==> Pulling workspaces archive from source..."
railway ssh -s "${SOURCE_SERVICE}" "cd /data && tar cf - workspaces" > "${TAR_LOCAL}"
ls -la "${TAR_LOCAL}"

echo "==> Pushing database to target (brief SQLite swap; target should be idle if possible)..."
railway ssh -s "${TARGET_SERVICE}" "cat > /data/orchestrator.db.new" < "${DB_LOCAL}"
railway ssh -s "${TARGET_SERVICE}" "mv /data/orchestrator.db.new /data/orchestrator.db && chmod 644 /data/orchestrator.db"

echo "==> Pushing workspaces to target..."
railway ssh -s "${TARGET_SERVICE}" "sh -c 'rm -rf /data/workspaces && mkdir -p /data && tar xf - -C /data'" < "${TAR_LOCAL}"

echo "==> Verifying target campaign count..."
railway ssh -s "${TARGET_SERVICE}" "sqlite3 /data/orchestrator.db 'SELECT COUNT(*) FROM campaigns;'"

echo "==> Done. Restart target service if the app cached old state:"
echo "    railway restart -s ${TARGET_SERVICE}"
