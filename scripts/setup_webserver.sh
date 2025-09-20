#!/bin/bash

# WebSocket Architecture Setup Script for jw-trader webserver
# This script configures the webserver to run the web app with WebSocket integration

set -e

echo "🚀 Setting up WebSocket Architecture on jw-trader webserver..."

# Configuration
PROJECT_ID="autobet-470818"
REGION="europe-west2"
WEBSOCKET_URL="https://websocket-subscription-539619253741.europe-west2.run.app"
WEBAPP_PORT="8081"

# Check if we're on the right machine
if [[ "$(hostname)" != "jw-trader" ]]; then
    echo "❌ This script should be run on jw-trader webserver"
    exit 1
fi

echo "✅ Running on jw-trader webserver"

# 1. Set up virtual environment
echo "📦 Setting up Python virtual environment..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi

source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# 2. Set up environment variables
echo "🔧 Setting up environment variables..."
cat > .env << EOF
# GCP Configuration
BQ_PROJECT=${PROJECT_ID}
BQ_DATASET=autobet
BQ_LOCATION=EU
GCP_PROJECT=${PROJECT_ID}
CLOUD_RUN_REGION=${REGION}
BQ_WRITE_ENABLED=1

# WebSocket Configuration
WEBSOCKET_SERVICE_URL=${WEBSOCKET_URL}

# Web App Configuration
FLASK_ENV=production
FLASK_DEBUG=False
EOF

# 3. Create startup script
echo "📝 Creating startup script..."
cat > start_webapp.sh << 'EOF'
#!/bin/bash

# WebSocket Web App Startup Script
set -e

echo "🚀 Starting WebSocket Web App..."

# Activate virtual environment
source .venv/bin/activate

# Load environment variables
export $(cat .env | xargs)

# Start the web app
echo "Starting Flask app on port 8081..."
python -c "
from autobet.sports.webapp import app
import os

# Set up logging
import logging
logging.basicConfig(level=logging.INFO)

print('🌐 WebSocket Web App starting...')
print(f'📊 Status page: http://$(hostname -I | awk \"{print \$1}\"):8081/status')
print(f'🔗 Main page: http://$(hostname -I | awk \"{print \$1}\"):8081/')
print('⏰ Auto-refresh every 30 seconds')
print('🔄 Press Ctrl+C to stop')

app.run(host='0.0.0.0', port=8081, debug=False)
"
EOF

chmod +x start_webapp.sh

# 4. Create WebSocket management script
echo "🔧 Creating WebSocket management script..."
cat > manage_websocket.sh << 'EOF'
#!/bin/bash

# WebSocket Service Management Script
set -e

WEBSOCKET_URL="https://websocket-subscription-539619253741.europe-west2.run.app"
PROJECT_ID="autobet-470818"

case "$1" in
    start)
        echo "🚀 Starting WebSocket service..."
        # Use gcloud auth to authenticate the request
        gcloud auth print-access-token | xargs -I {} curl -X POST \
            -H "Authorization: Bearer {}" \
            -H "Content-Type: application/json" \
            -d '{}' \
            "${WEBSOCKET_URL}/start"
        echo "✅ WebSocket service started"
        ;;
    stop)
        echo "🛑 Stopping WebSocket service..."
        gcloud auth print-access-token | xargs -I {} curl -X POST \
            -H "Authorization: Bearer {}" \
            -H "Content-Type: application/json" \
            -d '{}' \
            "${WEBSOCKET_URL}/stop"
        echo "✅ WebSocket service stopped"
        ;;
    status)
        echo "📊 Checking WebSocket service status..."
        gcloud auth print-access-token | xargs -I {} curl -s \
            -H "Authorization: Bearer {}" \
            "${WEBSOCKET_URL}/health" | jq . || echo "Service not accessible"
        ;;
    *)
        echo "Usage: $0 {start|stop|status}"
        exit 1
        ;;
esac
EOF

chmod +x manage_websocket.sh

# 5. Create Pub/Sub consumer management script
echo "📡 Creating Pub/Sub consumer management script..."
cat > manage_pubsub.sh << 'EOF'
#!/bin/bash

# Pub/Sub Consumer Management Script
set -e

WEBAPP_URL="http://localhost:8081"

case "$1" in
    start)
        echo "🚀 Starting Pub/Sub consumer..."
        curl -X POST "${WEBAPP_URL}/pubsub/start"
        echo "✅ Pub/Sub consumer started"
        ;;
    stop)
        echo "🛑 Stopping Pub/Sub consumer..."
        curl -X POST "${WEBAPP_URL}/pubsub/stop"
        echo "✅ Pub/Sub consumer stopped"
        ;;
    status)
        echo "📊 Checking Pub/Sub consumer status..."
        curl -s "${WEBAPP_URL}/pubsub/status" | jq . || echo "Web app not running"
        ;;
    *)
        echo "Usage: $0 {start|stop|status}"
        exit 1
        ;;
esac
EOF

chmod +x manage_pubsub.sh

# 6. Create monitoring script
echo "📊 Creating monitoring script..."
cat > monitor.sh << 'EOF'
#!/bin/bash

# WebSocket Architecture Monitoring Script
set -e

echo "🔍 WebSocket Architecture Status Monitor"
echo "========================================"

# Check WebSocket service
echo "📡 WebSocket Service:"
./manage_websocket.sh status

echo ""

# Check Pub/Sub consumer
echo "📨 Pub/Sub Consumer:"
./manage_pubsub.sh status

echo ""

# Check web app
echo "🌐 Web App:"
if curl -s http://localhost:8081/api/status/websocket > /dev/null; then
    echo "✅ Web app running on port 8081"
    echo "🔗 Status page: http://$(hostname -I | awk '{print $1}'):8081/status"
else
    echo "❌ Web app not running"
fi

echo ""
echo "📋 Quick Commands:"
echo "  Start everything: ./start_webapp.sh & ./manage_websocket.sh start && ./manage_pubsub.sh start"
echo "  Monitor status:   ./monitor.sh"
echo "  Stop everything:  ./manage_websocket.sh stop && ./manage_pubsub.sh stop"
EOF

chmod +x monitor.sh

# 7. Test GCP authentication
echo "🔐 Testing GCP authentication..."
if gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
    echo "✅ GCP authentication working"
    gcloud config set project ${PROJECT_ID}
else
    echo "❌ GCP authentication not set up"
    echo "Please run: gcloud auth login"
    exit 1
fi

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Start the web app:     ./start_webapp.sh"
echo "2. Start WebSocket:       ./manage_websocket.sh start"
echo "3. Start Pub/Sub:         ./manage_pubsub.sh start"
echo "4. Monitor status:        ./monitor.sh"
echo ""
echo "🌐 Access your web app at: http://$(hostname -I | awk '{print $1}'):8081/status"
