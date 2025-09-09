#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./_common.sh
source "$SCRIPT_DIR/_common.sh"

usage(){
  echo "Usage: $0 --project <PROJECT> --region <REGION> --service <NAME> --topic <TOPIC> [--sa <SERVICE_ACCOUNT>]" >&2
}

PROJECT="${BQ_PROJECT:-}"
REGION="europe-west1"
SERVICE="autobet-ingest"
TOPIC="ingest-jobs"
SA=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2;;
    --region) REGION="$2"; shift 2;;
    --service) SERVICE="$2"; shift 2;;
    --topic) TOPIC="$2"; shift 2;;
    --sa) SA="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

if [[ -z "$PROJECT" ]]; then echo "--project is required" >&2; exit 2; fi

echo "Enabling APIs..."
gcloud services enable run.googleapis.com pubsub.googleapis.com --project "$PROJECT"

echo "Deploying Cloud Run service $SERVICE in $REGION..."
gcloud run deploy "$SERVICE" \
  --project "$PROJECT" \
  --region "$REGION" \
  --source "$SCRIPT_DIR/.." \
  --set-env-vars BQ_WRITE_ENABLED=true,BQ_PROJECT="$PROJECT",BQ_DATASET="${BQ_DATASET}",BQ_LOCATION="${BQ_LOCATION}",TOTE_API_KEY="${TOTE_API_KEY:-}",TOTE_GRAPHQL_URL="${TOTE_GRAPHQL_URL:-}" \
  --allow-unauthenticated=false

SRV_URL=$(gcloud run services describe "$SERVICE" --project "$PROJECT" --region "$REGION" --format='value(status.url)')
echo "Service URL: $SRV_URL"

echo "Ensuring Pub/Sub topic $TOPIC..."
gcloud pubsub topics create "$TOPIC" --project "$PROJECT" --quiet || true

SUB="${SERVICE}-sub"
echo "Creating push subscription $SUB -> $SRV_URL ..."
if [[ -n "$SA" ]]; then
  gcloud pubsub subscriptions create "$SUB" \
    --project "$PROJECT" \
    --topic "$TOPIC" \
    --push-endpoint="$SRV_URL/" \
    --push-auth-service-account="$SA" \
    --quiet || true
else
  gcloud pubsub subscriptions create "$SUB" \
    --project "$PROJECT" \
    --topic "$TOPIC" \
    --push-endpoint="$SRV_URL/" \
    --quiet || true
fi

echo "Done. Publish a test job with publish_all_today.sh using --topic $TOPIC"

