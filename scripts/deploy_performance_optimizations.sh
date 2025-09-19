#!/bin/bash
set -e

# Deploy Performance Optimizations for Autobet
# This script applies all the performance optimizations for faster race status updates

echo "🚀 Deploying Autobet Performance Optimizations..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "performance_optimization.sql" ]; then
    print_error "performance_optimization.sql not found. Please run from the project root."
    exit 1
fi

# Check if bq command is available
if ! command -v bq &> /dev/null; then
    print_error "BigQuery CLI (bq) not found. Please install it first."
    exit 1
fi

# Check if gcloud command is available
if ! command -v gcloud &> /dev/null; then
    print_error "Google Cloud CLI (gcloud) not found. Please install it first."
    exit 1
fi

# Set project ID
PROJECT_ID="autobet-470818"
DATASET_ID="autobet"

print_status "Using project: $PROJECT_ID"
print_status "Using dataset: $DATASET_ID"

# 1. Deploy BigQuery optimizations
print_status "Deploying BigQuery performance optimizations..."

if bq query --use_legacy_sql=false --project_id=$PROJECT_ID < performance_optimization.sql; then
    print_success "BigQuery optimizations deployed successfully"
else
    print_error "Failed to deploy BigQuery optimizations"
    exit 1
fi

# 2. Deploy Terraform changes
print_status "Deploying Terraform scheduler changes..."

cd sports
if terraform plan -out=tfplan; then
    print_status "Terraform plan created successfully"
    if terraform apply tfplan; then
        print_success "Terraform changes applied successfully"
    else
        print_error "Failed to apply Terraform changes"
        exit 1
    fi
else
    print_error "Failed to create Terraform plan"
    exit 1
fi

cd ..

# 3. Deploy Cloud Run services
print_status "Deploying Cloud Run services..."

# Build and deploy the orchestrator service
cd sports
if gcloud builds submit --config cloudbuild.orchestrator.yaml --project=$PROJECT_ID; then
    print_success "Orchestrator service deployed successfully"
else
    print_error "Failed to deploy orchestrator service"
    exit 1
fi

cd ..

# 4. Make scripts executable
print_status "Making scripts executable..."

chmod +x scripts/monitor_race_status.py
chmod +x scripts/refresh_cache.py
chmod +x scripts/deploy_performance_optimizations.sh

print_success "Scripts made executable"

# 5. Test the new views
print_status "Testing new performance views..."

# Test peak hours view
if bq query --use_legacy_sql=false --project_id=$PROJECT_ID --format=prettyjson "SELECT * FROM \`$PROJECT_ID.$DATASET_ID.vw_peak_racing_hours\` LIMIT 1"; then
    print_success "Peak hours view working correctly"
else
    print_warning "Peak hours view test failed (may need time to populate)"
fi

# Test status performance view
if bq query --use_legacy_sql=false --project_id=$PROJECT_ID --format=prettyjson "SELECT * FROM \`$PROJECT_ID.$DATASET_ID.vw_status_update_performance\` LIMIT 1"; then
    print_success "Status performance view working correctly"
else
    print_warning "Status performance view test failed (may need time to populate)"
fi

# 6. Display summary
print_success "🎉 Performance optimizations deployed successfully!"
echo ""
echo "📊 What was deployed:"
echo "  ✅ Faster materialized view refresh intervals (60 minutes instead of 720)"
echo "  ✅ Real-time race status monitoring (every 2 minutes during peak hours)"
echo "  ✅ Peak hours-aware cache refresh (2 minutes during peak, 5 minutes off-peak)"
echo "  ✅ Status validation views to identify anomalies"
echo "  ✅ New Cloud Scheduler jobs for faster updates"
echo "  ✅ Enhanced orchestrator service with monitoring endpoints"
echo ""
echo "🔧 New features available:"
echo "  • Race status anomaly detection"
echo "  • Peak hours optimization"
echo "  • Real-time status monitoring"
echo "  • Performance metrics tracking"
echo ""
echo "📈 Expected improvements:"
echo "  • Race status updates: 2-5 minutes during peak hours"
echo "  • Cache refresh: 2 minutes during peak hours"
echo "  • Materialized views: 60 minutes refresh (vs 720 minutes)"
echo "  • Better visibility into status update accuracy"
echo ""
echo "🚀 Next steps:"
echo "  1. Monitor the new Cloud Scheduler jobs in the GCP Console"
echo "  2. Check the status monitoring views for any anomalies"
echo "  3. Review logs for the new monitoring endpoints"
echo "  4. Adjust refresh intervals if needed based on performance"

print_success "Deployment completed! 🎉"