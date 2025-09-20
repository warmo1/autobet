#!/bin/bash
"""
Run tote daily jobs with correct parameters.
"""

echo "🚀 Running tote daily jobs..."

# Set project and region
export PROJECT=autobet-470818
export REGION=europe-west2

# Get today's date
TODAY=$(date +%Y-%m-%d)

echo "Project: $PROJECT"
echo "Date: $TODAY"

echo ""
echo "🔧 Running publish_tote_daily_jobs.py..."

python3 scripts/publish_tote_daily_jobs.py \
  --project "$PROJECT" \
  --topic "ingest-jobs" \
  --bucket "${PROJECT}-data" \
  --date "$TODAY" \
  --bet-types "WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA" \
  --paginate

echo ""
echo "✅ Tote daily jobs published successfully!"
echo ""
echo "🔍 To check job status:"
echo "   gcloud logs read --project=$PROJECT --region=$REGION --service=ingestion-orchestrator --limit=50"
echo "   gcloud logs read --project=$PROJECT --region=$REGION --service=ingestion-fetcher --limit=50"
