#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./_common.sh
source "$SCRIPT_DIR/_common.sh"

usage() {
  echo "Usage: $0 --project <GCP_PROJECT> --topic <TOPIC> --bucket <GCS_BUCKET> [--limit N]" >&2
}

PROJECT="${BQ_PROJECT:-}"
TOPIC="ingest-jobs"
BUCKET=""
LIMIT=50

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2;;
    --topic) TOPIC="$2"; shift 2;;
    --bucket) BUCKET="$2"; shift 2;;
    --limit) LIMIT="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

if [[ -z "$PROJECT" || -z "$TOPIC" || -z "$BUCKET" ]]; then
  usage; exit 2
fi

echo "Publishing probable-odds jobs for today's open WIN products..."
$PY "$REPO_ROOT/scripts/publish_probable_for_today.py" \
  --project "$PROJECT" \
  --topic "$TOPIC" \
  --bucket "$BUCKET" \
  --limit "$LIMIT" \
  --bq-project "${BQ_PROJECT}" \
  --bq-dataset "${BQ_DATASET}"
echo "Done."

