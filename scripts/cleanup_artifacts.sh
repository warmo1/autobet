#!/bin/bash
set -e

echo "🧹 Cleaning up old Docker images and artifacts..."

# Keep the latest 3 images for each service
KEEP_COUNT=3

echo "📦 Cleaning up ingestion-fetcher images..."
gcloud artifacts docker images list europe-west2-docker.pkg.dev/autobet-470818/autobet-services/ingestion-fetcher \
  --sort-by=~UPDATE_TIME --format="value(DIGEST)" | tail -n +$((KEEP_COUNT + 1)) | \
  while read digest; do
    if [ -n "$digest" ]; then
      echo "  Deleting ingestion-fetcher@$digest"
      gcloud artifacts docker images delete europe-west2-docker.pkg.dev/autobet-470818/autobet-services/ingestion-fetcher@$digest --quiet || true
    fi
  done

echo "📦 Cleaning up ingestion-orchestrator images..."
gcloud artifacts docker images list europe-west2-docker.pkg.dev/autobet-470818/autobet-services/ingestion-orchestrator \
  --sort-by=~UPDATE_TIME --format="value(DIGEST)" | tail -n +$((KEEP_COUNT + 1)) | \
  while read digest; do
    if [ -n "$digest" ]; then
      echo "  Deleting ingestion-orchestrator@$digest"
      gcloud artifacts docker images delete europe-west2-docker.pkg.dev/autobet-470818/autobet-services/ingestion-orchestrator@$digest --quiet || true
    fi
  done

echo "📦 Cleaning up superfecta-ml images..."
gcloud artifacts docker images list europe-west2-docker.pkg.dev/autobet-470818/autobet-services/superfecta-ml \
  --sort-by=~UPDATE_TIME --format="value(DIGEST)" | tail -n +$((KEEP_COUNT + 1)) | \
  while read digest; do
    if [ -n "$digest" ]; then
      echo "  Deleting superfecta-ml@$digest"
      gcloud artifacts docker images delete europe-west2-docker.pkg.dev/autobet-470818/autobet-services/superfecta-ml@$digest --quiet || true
    fi
  done

echo "🧹 Cleanup complete! Kept latest $KEEP_COUNT images for each service."
