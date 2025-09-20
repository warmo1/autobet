#!/bin/bash

# Deploy WebSocket Architecture to jw-trader webserver
set -e

WEBSERVER="jwtrader@jw-trader"
REMOTE_DIR="~/autobet"

echo "🚀 Deploying WebSocket Architecture to jw-trader webserver..."

# Copy the setup script
echo "📦 Copying setup script..."
scp scripts/setup_webserver.sh ${WEBSERVER}:${REMOTE_DIR}/

# Copy the updated webapp.py
echo "📝 Copying updated webapp.py..."
scp sports/webapp.py ${WEBSERVER}:${REMOTE_DIR}/sports/

# Copy the updated status.html template
echo "🎨 Copying updated status.html..."
scp sports/templates/status.html ${WEBSERVER}:${REMOTE_DIR}/sports/templates/

# Copy the pubsub_consumer.py
echo "📡 Copying pubsub_consumer.py..."
scp sports/pubsub_consumer.py ${WEBSERVER}:${REMOTE_DIR}/sports/

# Copy requirements.txt
echo "📋 Copying requirements.txt..."
scp requirements.txt ${WEBSERVER}:${REMOTE_DIR}/

echo "✅ Files copied successfully!"
echo ""
echo "📋 Next steps on jw-trader webserver:"
echo "1. cd ~/autobet"
echo "2. chmod +x setup_webserver.sh"
echo "3. ./setup_webserver.sh"
echo "4. ./start_webapp.sh"
echo "5. ./manage_websocket.sh start"
echo "6. ./manage_pubsub.sh start"
