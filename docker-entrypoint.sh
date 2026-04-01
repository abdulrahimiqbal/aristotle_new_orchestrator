#!/bin/sh
set -e
# Per-campaign workspaces live under WORKSPACE_ROOT (see README / Mathlib cache notes).
mkdir -p "${WORKSPACE_ROOT}"
# Legacy shared workspace (optional migration source; app copies into WORKSPACE_ROOT/<campaign_id> once)
if [ -n "${WORKSPACE_LEGACY_DIR}" ]; then
  mkdir -p "${WORKSPACE_LEGACY_DIR}"
fi
mkdir -p "$(dirname "${DATABASE_PATH}")"
exec "$@"
