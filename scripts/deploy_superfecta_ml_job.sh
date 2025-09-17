#!/usr/bin/env bash

set -euo pipefail

if ! command -v gcloud >/dev/null 2>&1; then
  echo "gcloud CLI is required" >&2
  exit 1
fi

PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-europe-west2}"
REPO="${REPO:-autobet}"
JOB_NAME="${JOB_NAME:-superfecta-trainer}"
IMAGE_TAG="${IMAGE_TAG:-$(date +%Y%m%d-%H%M%S)}"
IMAGE="${IMAGE:-${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/${JOB_NAME}:${IMAGE_TAG}}"

if [[ -z "${PROJECT}" ]]; then
  echo "Set PROJECT env var or run 'gcloud config set project <id>'" >&2
  exit 1
fi

echo "Project: ${PROJECT}"
echo "Region:  ${REGION}"
echo "Image:   ${IMAGE}"

gcloud services enable run.googleapis.com artifactregistry.googleapis.com --project "${PROJECT}" >/dev/null

docker build -f sports/Dockerfile.superfecta -t "${IMAGE}" .
docker push "${IMAGE}"

COMMON_ARGS=(
  --project "${PROJECT}"
  --region "${REGION}"
  --image "${IMAGE}"
  --memory 1Gi
  --cpu 1
  --set-env-vars BQ_PROJECT=${BQ_PROJECT:-autobet-470818},BQ_DATASET=${BQ_DATASET:-autobet},BQ_MODEL_DATASET=${BQ_MODEL_DATASET:-autobet_model}
  --command python
)

DAILY_ARGS=(
  scripts/train_superfecta_model.py
  --countries GB IE
  --start-date \$(date +%F)
  --end-date \$(date +%F)
  --window-start-minutes -180
  --window-end-minutes 720
)

gcloud run jobs describe "${JOB_NAME}" --project "${PROJECT}" --region "${REGION}" >/dev/null 2>&1 && \
  gcloud run jobs update "${JOB_NAME}" "${COMMON_ARGS[@]}" --args "${DAILY_ARGS[@]}" || \
  gcloud run jobs create "${JOB_NAME}" "${COMMON_ARGS[@]}" --args "${DAILY_ARGS[@]}"

echo "Cloud Run job '${JOB_NAME}' configured."

echo "Example Cloud Scheduler setup:"
cat <<SCHED
----
Daily 10:00 (UTC) run:
gcloud scheduler jobs create http ${JOB_NAME}-daily \
  --project ${PROJECT} \
  --schedule "0 10 * * *" \
  --uri "https://run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB_NAME}:run" \
  --http-method POST \
  --oauth-service-account-email ${JOB_SA:-${JOB_NAME}-runner@${PROJECT}.iam.gserviceaccount.com} \
  --location ${REGION}

Per-race refresh every 15 minutes:
gcloud scheduler jobs create http ${JOB_NAME}-intraday \
  --project ${PROJECT} \
  --schedule "*/15 10-23 * * *" \
  --uri "https://run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB_NAME}:run" \
  --http-method POST \
  --oauth-service-account-email ${JOB_SA:-${JOB_NAME}-runner@${PROJECT}.iam.gserviceaccount.com} \
  --location ${REGION} \
  --message-body '{"args":["--countries","GB","IE","--window-start-minutes","-45","--window-end-minutes","120"]}'
----
SCHED
