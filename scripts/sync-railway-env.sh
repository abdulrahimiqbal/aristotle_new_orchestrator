#!/usr/bin/env bash
# Push variables from .env to the linked Railway service (non-interactive).
# Prerequisites: `railway link` in this repo, and a .env file with secrets.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "Missing .env — copy .env.example to .env and fill in values."
  exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

: "${LLM_API_KEY:?Set LLM_API_KEY in .env}"
: "${ARISTOTLE_API_KEY:?Set ARISTOTLE_API_KEY in .env}"

LLM_BASE_URL="${LLM_BASE_URL:-https://api.us-west-2.modal.direct/v1}"
LLM_MODEL="${LLM_MODEL:-zai-org/GLM-5.1-FP8}"
LLM_BACKUP_API_KEY="${LLM_BACKUP_API_KEY:-}"
LLM_BACKUP_BASE_URL="${LLM_BACKUP_BASE_URL:-}"
LLM_BACKUP_MODEL="${LLM_BACKUP_MODEL:-}"
DATABASE_PATH="${DATABASE_PATH:-/data/orchestrator.db}"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-/data/workspaces}"
WORKSPACE_LEGACY_DIR="${WORKSPACE_LEGACY_DIR:-/data/workspace}"
WORKSPACE_DIR="${WORKSPACE_DIR:-/data/workspace}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"

echo "Syncing variables to Railway (service must be linked: railway link)..."
_ARGS=(
  "LLM_API_KEY=${LLM_API_KEY}"
  "LLM_BASE_URL=${LLM_BASE_URL}"
  "LLM_MODEL=${LLM_MODEL}"
  "ARISTOTLE_API_KEY=${ARISTOTLE_API_KEY}"
  "DATABASE_PATH=${DATABASE_PATH}"
  "WORKSPACE_ROOT=${WORKSPACE_ROOT}"
  "WORKSPACE_LEGACY_DIR=${WORKSPACE_LEGACY_DIR}"
  "WORKSPACE_DIR=${WORKSPACE_DIR}"
)
if [[ -n "${LLM_BACKUP_API_KEY}" ]]; then
  _ARGS+=("LLM_BACKUP_API_KEY=${LLM_BACKUP_API_KEY}")
fi
if [[ -n "${LLM_BACKUP_BASE_URL}" ]]; then
  _ARGS+=("LLM_BACKUP_BASE_URL=${LLM_BACKUP_BASE_URL}")
fi
if [[ -n "${LLM_BACKUP_MODEL}" ]]; then
  _ARGS+=("LLM_BACKUP_MODEL=${LLM_BACKUP_MODEL}")
fi
if [[ -n "${ADMIN_TOKEN}" ]]; then
  _ARGS+=("ADMIN_TOKEN=${ADMIN_TOKEN}")
fi
railway variable set "${_ARGS[@]}"

echo "Done. Railway will redeploy if auto-deploy is enabled."
