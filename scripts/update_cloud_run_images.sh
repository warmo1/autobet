#!/bin/bash
"""
Update Cloud Run services to use the latest images with BigQuery fixes.
This uses the latest images from Artifact Registry.
"""

echo "🚀 Updating Cloud Run services to latest images"
echo "=============================================="

# Set project and region
export PROJECT=autobet-470818
export REGION=europe-west2

echo "Project: $PROJECT"
echo "Region: $REGION"

echo ""
echo "🔧 Step 1: Finding latest images in Artifact Registry..."
make set-images-latest

echo ""
echo "🔧 Step 2: Checking updated services..."
echo "Checking ingestion-fetcher..."
gcloud run services describe ingestion-fetcher --region=$REGION --project=$PROJECT --format="value(spec.template.spec.containers[0].image)"

echo "Checking ingestion-orchestrator..."
gcloud run services describe ingestion-orchestrator --region=$REGION --project=$PROJECT --format="value(spec.template.spec.containers[0].image)"

echo ""
echo "🔧 Step 3: Testing the services..."
echo "Testing ingestion-fetcher..."
curl -s -o /dev/null -w "%{http_code}" https://ingestion-fetcher-2aavih7hja-nw.a.run.app/health || echo "Service not responding"

echo "Testing ingestion-orchestrator..."
curl -s -o /dev/null -w "%{http_code}" https://ingestion-orchestrator-2aavih7hja-nw.a.run.app/health || echo "Service not responding"

echo ""
echo "🎉 Cloud Run services updated!"
echo "============================="
echo ""
echo "✅ Services updated to latest images"
echo ""
echo "🔍 To check service logs:"
echo "   gcloud logs read --project=$PROJECT --region=$REGION --service=ingestion-fetcher --limit=50"
echo "   gcloud logs read --project=$PROJECT --region=$REGION --service=ingestion-orchestrator --limit=50"
