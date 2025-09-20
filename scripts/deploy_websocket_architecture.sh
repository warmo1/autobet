#!/bin/bash
# Deploy WebSocket Architecture for Autobet

set -e

PROJECT=${PROJECT:-autobet-470818}
REGION=${REGION:-europe-west2}

echo "🚀 Deploying WebSocket Architecture for Autobet"
echo "Project: $PROJECT"
echo "Region: $REGION"
echo ""

# Step 1: Enable required APIs
echo "📋 Step 1: Enabling required APIs..."
gcloud services enable \
  run.googleapis.com \
  pubsub.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com \
  --project "$PROJECT"

# Step 2: Create Artifact Registry repository
echo "📦 Step 2: Creating Artifact Registry repository..."
gcloud artifacts repositories create "autobet-services" \
  --location="$REGION" \
  --repository-format=docker \
  --description="Autobet services" \
  --project "$PROJECT" || true

gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet

# Step 3: Build and push Docker images
echo "🐳 Step 3: Building and pushing Docker images..."
cd "$(dirname "$0")/.."
make build-and-push-gcb

# Step 4: Initialize and apply Terraform
echo "🏗️  Step 4: Deploying infrastructure with Terraform..."
cd sports
terraform init
terraform plan -var "project_id=$PROJECT" -var "region=$REGION"
terraform apply -auto-approve -var "project_id=$PROJECT" -var "region=$REGION"

# Step 5: Update Cloud Run services with new images
echo "🔄 Step 5: Updating Cloud Run services..."
cd ..
make set-images

# Step 6: Test WebSocket service
echo "🧪 Step 6: Testing WebSocket service..."
WEBSOCKET_URL=$(gcloud run services describe websocket-subscription --region="$REGION" --project="$PROJECT" --format="value(status.url)")

echo "Testing WebSocket service health..."
curl -f "$WEBSOCKET_URL/health" || echo "❌ Health check failed"

echo "Testing Pub/Sub publishing..."
curl -X POST "$WEBSOCKET_URL/test-pubsub" || echo "❌ Pub/Sub test failed"

# Step 7: Start WebSocket subscription
echo "🚀 Step 7: Starting WebSocket subscription..."
curl -X POST "$WEBSOCKET_URL/start" || echo "❌ Failed to start WebSocket subscription"

# Step 8: Verify deployment
echo "✅ Step 8: Verifying deployment..."

echo "WebSocket Service Status:"
curl -s "$WEBSOCKET_URL/status" | jq '.' || echo "Failed to get status"

echo ""
echo "🎉 WebSocket Architecture Deployment Complete!"
echo ""
echo "Services deployed:"
echo "  - WebSocket Subscription Service: $WEBSOCKET_URL"
echo "  - Pub/Sub Topics: tote-pool-total-changed, tote-product-status-changed, etc."
echo "  - Cloud Scheduler Jobs: websocket-start, websocket-stop"
echo ""
echo "Next steps:"
echo "  1. Monitor WebSocket service logs: gcloud logs tail --follow --project=$PROJECT --filter='resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription'"
echo "  2. Check Pub/Sub topics: gcloud pubsub topics list --project=$PROJECT"
echo "  3. Test real-time updates in your web app"
echo ""
echo "To stop WebSocket service: curl -X POST $WEBSOCKET_URL/stop"
echo "To start WebSocket service: curl -X POST $WEBSOCKET_URL/start"
