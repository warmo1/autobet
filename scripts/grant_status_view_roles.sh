#!/usr/bin/env bash

# Ensure the status dashboard can call Cloud Run and Cloud Scheduler APIs.
# Usage: PROJECT=your-project ./scripts/grant_status_view_roles.sh principal@example.com

set -euo pipefail

PROJECT="${PROJECT:-autobet-470818}"
PRINCIPAL="${1:-}"

if [[ -z "${PRINCIPAL}" ]]; then
  echo "usage: PROJECT=your-project $0 user-or-service-account" >&2
  exit 1
fi

case "${PRINCIPAL}" in
  *@*.gserviceaccount.com) MEMBER="serviceAccount:${PRINCIPAL}" ;;
  *@*) MEMBER="user:${PRINCIPAL}" ;;
  *)
    echo "Principal should be an email address (user or service account)." >&2
    exit 1
    ;;
esac

echo "Granting roles/run.viewer and roles/cloudscheduler.viewer to ${MEMBER} on project ${PROJECT}"

gcloud projects add-iam-policy-binding "${PROJECT}" \
  --member="${MEMBER}" \
  --role="roles/run.viewer"

gcloud projects add-iam-policy-binding "${PROJECT}" \
  --member="${MEMBER}" \
  --role="roles/cloudscheduler.viewer"

gcloud projects add-iam-policy-binding "${PROJECT}" \
  --member="${MEMBER}" \
  --role="roles/pubsub.viewer"

echo "Done. It can take a minute for permissions to propagate."
