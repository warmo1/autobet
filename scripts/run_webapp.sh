#!/bin/bash
"""
Simple script to run the webapp with proper Python path setup.
"""

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🔧 Setting up Python path..."
echo "Project root: $PROJECT_ROOT"

# Change to project root
cd "$PROJECT_ROOT"

# Add project root to Python path
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

echo "🚀 Starting web server..."

# Try different methods to run the webapp
if [ -f "run_webapp.py" ]; then
    echo "Using run_webapp.py..."
    python run_webapp.py
elif [ -f "sports/webapp.py" ]; then
    echo "Using python -m sports.webapp..."
    python -m sports.webapp
else
    echo "❌ webapp not found"
    exit 1
fi
