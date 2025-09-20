#!/bin/bash
"""
FINAL COMPLETE FIX for all webapp and BigQuery issues.
Run this on your web server to fix everything.
"""

echo "🚀 FINAL COMPLETE FIX for webapp and BigQuery issues"
echo "=================================================="

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Project root: $PROJECT_ROOT"
cd "$PROJECT_ROOT"

# Activate virtual environment if it exists
if [ -f ".venv/bin/activate" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
else
    echo "⚠️ No virtual environment found, using system Python"
fi

echo ""
echo "🔧 Step 1: Fixing webapp imports..."
python3 scripts/fix_all_webapp_imports.py
python3 scripts/fix_webapp_imports_proper.py

echo ""
echo "🔧 Step 2: Fixing BigQuery location issues..."
python3 scripts/fix_bq_location_final.py

echo ""
echo "🔧 Step 3: Creating webapp runner..."
cat > run_webapp.py << 'EOF'
#!/usr/bin/env python3
"""
Webapp runner script to avoid import issues.
"""

import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import and run the webapp
from sports.webapp import app

if __name__ == "__main__":
    print("🚀 Starting web server...")
    app.run(host="0.0.0.0", port=8010, debug=True)
EOF

chmod +x run_webapp.py

echo ""
echo "🧪 Step 4: Testing webapp..."
python3 -c "import sys; sys.path.insert(0, '.'); from sports.webapp import app; print('✅ Webapp imports working!')"

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 ALL FIXES APPLIED SUCCESSFULLY!"
    echo "=================================="
    echo ""
    echo "🚀 To start the web server, run:"
    echo "   python3 run_webapp.py"
    echo ""
    echo "The webapp will be available at: http://0.0.0.0:8010"
    echo ""
    echo "✅ Expected results:"
    echo "   - No more import errors"
    echo "   - No more BigQuery location parameter errors"  
    echo "   - All endpoints return 200 status codes"
    echo "   - Webapp starts without crashes"
    echo ""
    echo "🔍 To test endpoints:"
    echo "   curl http://localhost:8010/status"
    echo "   curl http://localhost:8010/api/status/upcoming"
    echo "   curl http://localhost:8010/api/status/data_freshness"
else
    echo ""
    echo "❌ Webapp still has issues. Check the error messages above."
    echo "You may need to check the specific error and fix it manually."
fi
