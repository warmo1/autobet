#!/bin/bash
"""
Complete fix for webapp import issues on the web server.
"""

echo "🔧 Complete webapp fix for web server..."

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

# Fix all webapp imports
echo "🔧 Fixing webapp imports..."
python3 scripts/fix_all_webapp_imports.py
python3 scripts/fix_webapp_imports_proper.py

# Test the webapp
echo "🧪 Testing webapp..."
python3 -c "import sys; sys.path.insert(0, '.'); from sports.webapp import app; print('✅ Webapp imports working!')"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Webapp is now fixed and ready to run!"
    echo ""
    echo "🚀 To start the web server, run:"
    echo "   python3 run_webapp.py"
    echo "   OR"
    echo "   python3 -m sports.webapp"
    echo "   OR"
    echo "   bash scripts/run_webapp.sh"
    echo ""
    echo "The webapp will be available at: http://0.0.0.0:8010"
else
    echo "❌ Webapp still has issues. Check the error messages above."
fi
