#!/bin/bash
"""
Deploy Cloud Run services with BigQuery location fixes.
This rebuilds and deploys the images with all our recent fixes.
"""

echo "🚀 Deploying Cloud Run services with BigQuery location fixes"
echo "=========================================================="

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Project root: $PROJECT_ROOT"
cd "$PROJECT_ROOT"

# Set project and region
export PROJECT=autobet-470818
export REGION=europe-west2

echo ""
echo "🔧 Step 1: Ensuring all BigQuery fixes are applied locally..."
python3 scripts/fix_bq_location_final_complete.py

echo ""
echo "🔧 Step 2: Building and pushing new images with fixes..."
make build-and-push-gcb

echo ""
echo "🔧 Step 3: Updating Cloud Run services to use new images..."
make set-images

echo ""
echo "🔧 Step 4: Checking service status..."
echo "Checking ingestion-fetcher..."
gcloud run services describe ingestion-fetcher --region=$REGION --project=$PROJECT --format="value(status.url,spec.template.spec.containers[0].image)"

echo "Checking ingestion-orchestrator..."
gcloud run services describe ingestion-orchestrator --region=$REGION --project=$PROJECT --format="value(status.url,spec.template.spec.containers[0].image)"

echo ""
echo "🔧 Step 5: Testing the services..."
echo "Testing ingestion-fetcher..."
curl -s -o /dev/null -w "%{http_code}" https://ingestion-fetcher-2aavih7hja-nw.a.run.app/health || echo "Service not responding"

echo "Testing ingestion-orchestrator..."
curl -s -o /dev/null -w "%{http_code}" https://ingestion-orchestrator-2aavih7hja-nw.a.run.app/health || echo "Service not responding"

echo ""
echo "🎉 Deployment complete!"
echo "====================="
echo ""
echo "✅ New images deployed with BigQuery location fixes"
echo "✅ Cloud Run services updated"
echo ""
echo "🔍 To check service logs:"
echo "   gcloud logs read --project=$PROJECT --region=$REGION --service=ingestion-fetcher --limit=50"
echo "   gcloud logs read --project=$PROJECT --region=$REGION --service=ingestion-orchestrator --limit=50"
echo ""
echo "🚀 To trigger odds update:"
echo "   python3 scripts/publish_probable_for_today.py"
echo "   python3 scripts/publish_tote_daily_jobs.py"
