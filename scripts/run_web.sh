#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./_common.sh
source "$SCRIPT_DIR/_common.sh"

PORT="${PORT:-8010}"
HOST="${HOST:-0.0.0.0}"

echo "Starting web app on http://${HOST}:${PORT} (SUBSCRIBE_POOLS=${SUBSCRIBE_POOLS:-0})"
$PY - <<'PY'
from autobet.sports.webapp import app
app.run(host='0.0.0.0', port=int(__import__('os').getenv('PORT','8010')))
PY

