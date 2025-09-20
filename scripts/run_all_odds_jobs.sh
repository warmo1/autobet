#!/bin/bash
"""
Run all odds-related jobs: probable odds and tote daily jobs.
"""

echo "🚀 Running all odds-related jobs..."
echo "=================================="

# Set project and region
export PROJECT=autobet-470818
export REGION=europe-west2

# Get today's date
TODAY=$(date +%Y-%m-%d)

echo "Project: $PROJECT"
echo "Date: $TODAY"

echo ""
echo "🔧 Step 1: Publishing probable odds for today's events..."
python3 scripts/publish_probable_for_today.py

echo ""
echo "🔧 Step 2: Publishing tote daily jobs..."
python3 scripts/publish_tote_daily_jobs.py \
  --project "$PROJECT" \
  --topic "ingest-jobs" \
  --bucket "${PROJECT}-data" \
  --date "$TODAY" \
  --bet-types "WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA" \
  --paginate

echo ""
echo "🎉 All odds jobs published successfully!"
echo "======================================="
echo ""
echo "✅ Probable odds tasks published"
echo "✅ Tote daily jobs published"
echo ""
echo "🔍 To check job status:"
echo "   gcloud logs read --project=$PROJECT --region=$REGION --service=ingestion-orchestrator --limit=50"
echo "   gcloud logs read --project=$PROJECT --region=$REGION --service=ingestion-fetcher --limit=50"
echo ""
echo "🌐 To check webapp status:"
echo "   curl http://localhost:8010/status"
echo "   curl http://localhost:8010/api/status/upcoming"
