#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./_common.sh
source "$SCRIPT_DIR/_common.sh"

PORT="${PORT:-8010}"
HOST="${HOST:-0.0.0.0}"

echo "Starting web app on http://${HOST}:${PORT} (SUBSCRIBE_POOLS=${SUBSCRIBE_POOLS:-0})"
$PY - <<'PY'
import os
from sports.webapp import create_app

app = create_app()
app.run(host=os.getenv('HOST','0.0.0.0'), port=int(os.getenv('PORT','8010')))
PY
