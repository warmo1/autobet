#!/bin/bash

# Simple WebSocket Service Deployment Script
set -e

PROJECT_ID="autobet-470818"
REGION="europe-west2"
SERVICE_NAME="websocket-subscription"
IMAGE_URL="europe-west2-docker.pkg.dev/autobet-470818/autobet-services/websocket-subscription:latest"

echo "🚀 Deploying WebSocket Service..."

# Create service account if it doesn't exist
echo "Creating service account..."
gcloud iam service-accounts create websocket-sa \
  --display-name="WebSocket Subscription Service Account" \
  --project=$PROJECT_ID || echo "Service account already exists"

# Grant necessary permissions
echo "Granting permissions..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:websocket-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.user"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:websocket-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:websocket-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"

# Deploy Cloud Run service
echo "Deploying Cloud Run service..."
gcloud run deploy $SERVICE_NAME \
  --image=$IMAGE_URL \
  --region=$REGION \
  --platform=managed \
  --allow-unauthenticated \
  --service-account="websocket-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --set-env-vars="BQ_PROJECT=$PROJECT_ID,BQ_DATASET=autobet,BQ_LOCATION=EU,GCP_PROJECT=$PROJECT_ID,CLOUD_RUN_REGION=$REGION,BQ_WRITE_ENABLED=1" \
  --set-secrets="TOTE_API_KEY=TOTE_API_KEY:latest,TOTE_SUBSCRIPTIONS_URL=TOTE_SUBSCRIPTIONS_URL:latest" \
  --memory=512Mi \
  --cpu=1 \
  --max-instances=1 \
  --min-instances=0 \
  --project=$PROJECT_ID

echo "✅ WebSocket service deployed successfully!"
echo "Service URL: $(gcloud run services describe $SERVICE_NAME --region=$REGION --project=$PROJECT_ID --format='value(status.url)')"
