#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./_common.sh
source "$SCRIPT_DIR/_common.sh"

usage() {
  echo "Usage: $0 [--date YYYY-MM-DD]" >&2
}

DATE="$(date +%F)"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --date) DATE="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

if [[ -z "${BQ_PROJECT:-}" || -z "${BQ_DATASET:-}" ]]; then
  echo "BQ_PROJECT/BQ_DATASET not set (configure autobet/.env)" >&2
  exit 2
fi

echo "Transforming raw_tote -> structured for $DATE in ${BQ_PROJECT}.${BQ_DATASET}"
$PY "$REPO_ROOT/scripts/bq_transform_from_raw.py" \
  --project "$BQ_PROJECT" \
  --dataset "$BQ_DATASET" \
  --date "$DATE"
echo "Done."

