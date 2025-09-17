#!/usr/bin/env bash
set -euo pipefail

# Create or reuse a service account, grant minimal roles, and
# create a key at .secrets/gcp-service-account.json for local dev.
#
# Usage:
#   scripts/setup_service_account.sh [PROJECT_ID] [SERVICE_ACCOUNT_NAME]
#
# Defaults:
#   PROJECT_ID: gcloud config get-value project
#   SERVICE_ACCOUNT_NAME: run-ingest-sa

PROJECT_ID="${1:-$(gcloud config get-value project 2>/dev/null)}"
SA_NAME="${2:-run-ingest-sa}"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
KEY_PATH="$(cd "$(dirname "$0")/.." && pwd)/.secrets/gcp-service-account.json"

if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "Project not set. Run: gcloud config set project <PROJECT_ID>" >&2
  exit 1
fi

echo "Project: ${PROJECT_ID}"
echo "Service Account: ${SA_EMAIL}"

mkdir -p "$(dirname "$KEY_PATH")"

echo "Ensuring service account exists..."
if ! gcloud iam service-accounts describe "${SA_EMAIL}" >/dev/null 2>&1; then
  gcloud iam service-accounts create "${SA_NAME}" \
    --description "Autobet ingest/web credentials" \
    --display-name "Autobet ${SA_NAME}"
else
  echo "Service account already exists."
fi

echo "Granting roles (bq job + edit + readSessionUser, pubsub publish, storage objectAdmin)..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${SA_EMAIL}" --role roles/bigquery.jobUser >/dev/null
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${SA_EMAIL}" --role roles/bigquery.dataEditor >/dev/null
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${SA_EMAIL}" --role roles/bigquery.readSessionUser >/dev/null
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${SA_EMAIL}" --role roles/pubsub.publisher >/dev/null
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${SA_EMAIL}" --role roles/storage.objectAdmin >/dev/null

echo "Creating key at ${KEY_PATH} (overwrites existing)..."
if [[ -f "${KEY_PATH}" ]]; then
  cp "${KEY_PATH}" "${KEY_PATH}.bak.$(date +%s)"
fi
gcloud iam service-accounts keys create "${KEY_PATH}" \
  --iam-account "${SA_EMAIL}"

echo "Done. To use in the app, ensure your .env has:"
echo "  GOOGLE_APPLICATION_CREDENTIALS=\"\$REPO_ROOT/.secrets/gcp-service-account.json\""
echo "Or export it in your shell before running."
