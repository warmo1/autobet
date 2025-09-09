#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./_common.sh
source "$SCRIPT_DIR/_common.sh"

usage() {
  echo "Usage: $0 --project <GCP_PROJECT> --topic <TOPIC> --bucket <GCS_BUCKET> [--date YYYY-MM-DD] [--page-size N] [--paginate] [--probable-limit N]" >&2
}

PROJECT="${BQ_PROJECT:-}"
TOPIC="ingest-jobs"
BUCKET=""
DATE="$(date +%F)"
PAGE_SIZE=400
PAGINATE=0
PROB_LIMIT=50

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2;;
    --topic) TOPIC="$2"; shift 2;;
    --bucket) BUCKET="$2"; shift 2;;
    --date) DATE="$2"; shift 2;;
    --page-size) PAGE_SIZE="$2"; shift 2;;
    --paginate) PAGINATE=1; shift 1;;
    --probable-limit) PROB_LIMIT="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

if [[ -z "$PROJECT" || -z "$TOPIC" || -z "$BUCKET" ]]; then
  usage; exit 2
fi

bash "$SCRIPT_DIR/ensure_views.sh"
bash "$SCRIPT_DIR/publish_daily.sh" --project "$PROJECT" --topic "$TOPIC" --bucket "$BUCKET" --date "$DATE" --page-size "$PAGE_SIZE" ${PAGINATE:+--paginate}
bash "$SCRIPT_DIR/publish_probable_today.sh" --project "$PROJECT" --topic "$TOPIC" --bucket "$BUCKET" --limit "$PROB_LIMIT"
bash "$SCRIPT_DIR/transform_for_date.sh" --date "$DATE"

echo "All publish + transform jobs completed for $DATE."
