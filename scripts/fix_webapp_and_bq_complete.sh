#!/bin/bash
"""
Complete fix for webapp and BigQuery issues on the web server.
"""

echo "🔧 Complete fix for webapp and BigQuery issues..."

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Project root: $PROJECT_ROOT"
cd "$PROJECT_ROOT"

# Activate virtual environment if it exists
if [ -f ".venv/bin/activate" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

echo ""
echo "🔧 Step 1: Fixing webapp imports..."
python3 scripts/fix_all_webapp_imports.py
python3 scripts/fix_webapp_imports_proper.py

echo ""
echo "🔧 Step 2: Fixing BigQuery location issues..."
python3 scripts/fix_bq_location_final.py

echo ""
echo "🧪 Step 3: Testing webapp..."
python3 -c "import sys; sys.path.insert(0, '.'); from sports.webapp import app; print('✅ Webapp imports working!')"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ All fixes applied successfully!"
    echo ""
    echo "🚀 To start the web server, run:"
    echo "   python3 run_webapp.py"
    echo "   OR"
    echo "   python3 -m sports.webapp"
    echo ""
    echo "The webapp will be available at: http://0.0.0.0:8010"
    echo ""
    echo "🎯 Expected results:"
    echo "   - No more import errors"
    echo "   - No more BigQuery location parameter errors"
    echo "   - All endpoints return 200 status codes"
else
    echo "❌ Webapp still has issues. Check the error messages above."
fi
