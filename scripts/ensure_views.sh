#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./_common.sh
source "$SCRIPT_DIR/_common.sh"

echo "Ensuring BigQuery views and table functions in ${BQ_PROJECT}.${BQ_DATASET}..."
$PY "$REPO_ROOT/scripts/bq_ensure_views.py"
echo "Done."

