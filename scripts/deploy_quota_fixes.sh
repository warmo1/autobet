#!/bin/bash

# Deploy BigQuery quota management fixes
# This script applies the quota management improvements to resolve the quota exceeded issues

set -e

echo "🚀 Deploying BigQuery Quota Management Fixes"
echo "============================================="

# Check if we're in the right directory
if [ ! -f "sports/bq.py" ]; then
    echo "❌ Error: Please run this script from the project root directory"
    exit 1
fi

echo "📋 Current Issues Identified:"
echo "  • BigQuery quota exceeded errors preventing data ingestion"
echo "  • Aggressive scheduling causing high query frequency"
echo "  • No rate limiting or retry logic"
echo "  • Missing quota monitoring"
echo ""

echo "🔧 Applying Fixes:"
echo ""

# 1. Backup current scheduler configuration
echo "1. Backing up current scheduler configuration..."
if [ -f "sports/scheduler.tf" ]; then
    cp sports/scheduler.tf sports/scheduler.tf.backup.$(date +%Y%m%d_%H%M%S)
    echo "   ✅ Backup created: sports/scheduler.tf.backup.$(date +%Y%m%d_%H%M%S)"
else
    echo "   ⚠️  Warning: scheduler.tf not found"
fi

# 2. Apply conservative scheduling (optional)
echo ""
echo "2. Conservative scheduling configuration available..."
echo "   📄 New file: sports/scheduler_conservative.tf"
echo "   💡 To apply conservative scheduling, replace scheduler.tf with scheduler_conservative.tf"
echo "   📊 Changes:"
echo "      • Pre-race scanner: 5min → 15min"
echo "      • Probable odds sweep: 10min → 30min" 
echo "      • Race status monitor: 2min → 5min"
echo "      • Fast cache refresh: 2min → 10min"
echo "      • Aggressive cleanup: 1hr → 3hr"

# 3. Verify new quota management files
echo ""
echo "3. Verifying quota management components..."
files_to_check=(
    "sports/quota_manager.py"
    "sports/retry_utils.py" 
    "sports/manage_quota.py"
)

for file in "${files_to_check[@]}"; do
    if [ -f "$file" ]; then
        echo "   ✅ $file exists"
    else
        echo "   ❌ $file missing"
        exit 1
    fi
done

# 4. Check for syntax errors in updated files
echo ""
echo "4. Checking syntax of updated files..."
python -m py_compile sports/quota_manager.py
echo "   ✅ quota_manager.py syntax OK"

python -m py_compile sports/retry_utils.py  
echo "   ✅ retry_utils.py syntax OK"

python -m py_compile sports/bq.py
echo "   ✅ bq.py syntax OK"

python -m py_compile sports/webapp.py
echo "   ✅ webapp.py syntax OK"

# 5. Test quota management
echo ""
echo "5. Testing quota management system..."
cd sports
python -c "
from quota_manager import get_quota_manager
qm = get_quota_manager()
stats = qm.get_usage_stats()
print('   ✅ Quota manager initialized successfully')
print(f'   📊 Current query usage: {stats[\"queries\"][\"per_minute\"]}/min')
"
cd ..

# 6. Display next steps
echo ""
echo "🎯 Next Steps:"
echo "=============="
echo ""
echo "1. 🚀 Deploy the updated code to your Cloud Run services:"
echo "   gcloud run deploy ingest-service --source sports/"
echo "   gcloud run deploy orchestrator-service --source sports/"
echo ""
echo "2. 📊 Monitor quota usage:"
echo "   python sports/manage_quota.py status"
echo "   python sports/manage_quota.py analyze"
echo ""
echo "3. 🔄 Apply conservative scheduling (optional):"
echo "   cp sports/scheduler_conservative.tf sports/scheduler.tf"
echo "   terraform plan && terraform apply"
echo ""
echo "4. 📈 Monitor the status dashboard for improvements:"
echo "   Check /api/status/quota_usage endpoint"
echo ""
echo "5. 🚨 Set up alerts for quota usage (recommended):"
echo "   Monitor BigQuery quota metrics in Cloud Monitoring"
echo ""

echo "✅ Deployment preparation complete!"
echo ""
echo "📋 Summary of Changes:"
echo "====================="
echo "• ✅ Added quota management with rate limiting"
echo "• ✅ Implemented exponential backoff retry logic" 
echo "• ✅ Fixed BigQuery location parameter issue"
echo "• ✅ Added quota monitoring to status dashboard"
echo "• ✅ Created conservative scheduling configuration"
echo "• ✅ Added quota management utilities"
echo ""
echo "🔍 The quota exceeded errors should be resolved once deployed."
echo "📊 Monitor the system to ensure stable operation."
