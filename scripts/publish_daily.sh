#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./_common.sh
source "$SCRIPT_DIR/_common.sh"

usage() {
  echo "Usage: $0 --project <GCP_PROJECT> --topic <TOPIC> --bucket <GCS_BUCKET> [--date YYYY-MM-DD] [--bet-types CSV] [--page-size N] [--paginate]" >&2
}

PROJECT="${BQ_PROJECT:-}"
TOPIC="ingest-jobs"
BUCKET=""
DATE="$(date +%F)"
BET_TYPES="WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA"
PAGE_SIZE=400
PAGINATE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2;;
    --topic) TOPIC="$2"; shift 2;;
    --bucket) BUCKET="$2"; shift 2;;
    --date) DATE="$2"; shift 2;;
    --bet-types) BET_TYPES="$2"; shift 2;;
    --page-size) PAGE_SIZE="$2"; shift 2;;
    --paginate) PAGINATE=1; shift 1;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

if [[ -z "$PROJECT" || -z "$TOPIC" || -z "$BUCKET" ]]; then
  usage; exit 2
fi

echo "Publishing products for $DATE to topic=$TOPIC (project=$PROJECT, bucket=$BUCKET)"
$PY "$REPO_ROOT/scripts/publish_tote_daily_jobs.py" \
  --project "$PROJECT" \
  --topic "$TOPIC" \
  --bucket "$BUCKET" \
  --date "$DATE" \
  --bet-types "$BET_TYPES" \
  --page-size "$PAGE_SIZE" \
  ${PAGINATE:+--paginate}
echo "Done."
