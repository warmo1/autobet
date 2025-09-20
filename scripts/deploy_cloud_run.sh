#!/bin/bash

# Deploy Cloud Run services with proper error handling and logging
set -e

echo "🚀 Deploying Cloud Run Services with Quota Management Fixes"
echo "=========================================================="

# Check if we're in the right directory
if [ ! -f "sports/webapp.py" ]; then
    echo "❌ Error: Please run this script from the project root directory"
    exit 1
fi

# Set variables
PROJECT_ID="autobet-470818"
REGION="europe-west2"
SERVICES=("ingest-service" "orchestrator-service")

echo "📋 Project: $PROJECT_ID"
echo "🌍 Region: $REGION"
echo ""

# Function to deploy a service
deploy_service() {
    local service_name=$1
    echo "🔧 Deploying $service_name..."
    
    # Deploy with detailed logging
    if gcloud run deploy "$service_name" \
        --source sports/ \
        --region "$REGION" \
        --project "$PROJECT_ID" \
        --allow-unauthenticated \
        --memory 1Gi \
        --cpu 1 \
        --timeout 300 \
        --max-instances 10 \
        --platform managed; then
        echo "✅ $service_name deployed successfully"
        return 0
    else
        echo "❌ $service_name deployment failed"
        return 1
    fi
}

# Deploy each service
failed_services=()
for service in "${SERVICES[@]}"; do
    if ! deploy_service "$service"; then
        failed_services+=("$service")
    fi
    echo ""
done

# Report results
if [ ${#failed_services[@]} -eq 0 ]; then
    echo "🎉 All services deployed successfully!"
    echo ""
    echo "📊 Next steps:"
    echo "1. Test the quota management:"
    echo "   python sports/manage_quota.py status"
    echo ""
    echo "2. Check the status dashboard:"
    echo "   Visit the /status page to see quota usage"
    echo ""
    echo "3. Monitor for quota issues:"
    echo "   python sports/manage_quota.py analyze"
else
    echo "❌ Some services failed to deploy:"
    for service in "${failed_services[@]}"; do
        echo "   • $service"
    done
    echo ""
    echo "🔍 Troubleshooting steps:"
    echo "1. Check build logs in Cloud Console"
    echo "2. Verify the source directory structure"
    echo "3. Ensure all dependencies are in requirements.txt"
    echo ""
    exit 1
fi
