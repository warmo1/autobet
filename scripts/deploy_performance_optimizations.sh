#!/bin/bash
# Deploy performance optimizations for Autobet web app

set -e

echo "🚀 Deploying Autobet performance optimizations..."

# Make scripts executable
chmod +x scripts/refresh_cache.py
chmod +x scripts/monitor_performance.py

# Install Redis if not already installed
if ! command -v redis-server &> /dev/null; then
    echo "📦 Installing Redis..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        brew install redis
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Ubuntu/Debian
        sudo apt-get update
        sudo apt-get install -y redis-server
    else
        echo "⚠️  Please install Redis manually for your OS"
    fi
fi

# Start Redis if not running
if ! pgrep -x "redis-server" > /dev/null; then
    echo "🔄 Starting Redis server..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew services start redis
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo systemctl start redis-server
    fi
fi

# Install Python dependencies
echo "📦 Installing Python dependencies..."
pip install redis>=4.5.0

# Create systemd service files for background processes (Linux only)
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "🔧 Creating systemd services..."
    
    # Cache refresh service
    sudo tee /etc/systemd/system/autobet-cache-refresh.service > /dev/null <<EOF
[Unit]
Description=Autobet Cache Refresh Service
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$(pwd)
ExecStart=/usr/bin/python3 $(pwd)/scripts/refresh_cache.py
Restart=always
RestartSec=10
Environment=PYTHONPATH=$(pwd)

[Install]
WantedBy=multi-user.target
EOF

    # Performance monitoring service
    sudo tee /etc/systemd/system/autobet-performance-monitor.service > /dev/null <<EOF
[Unit]
Description=Autobet Performance Monitor
After=network.target

[Service]
Type=oneshot
User=$USER
WorkingDirectory=$(pwd)
ExecStart=/usr/bin/python3 $(pwd)/scripts/monitor_performance.py
Environment=PYTHONPATH=$(pwd)

[Install]
WantedBy=multi-user.target
EOF

    # Reload systemd and enable services
    sudo systemctl daemon-reload
    sudo systemctl enable autobet-cache-refresh.service
    echo "✅ Systemd services created and enabled"
fi

# Create cron job for performance monitoring (all platforms)
echo "⏰ Setting up performance monitoring cron job..."
(crontab -l 2>/dev/null; echo "0 */6 * * * cd $(pwd) && python3 scripts/monitor_performance.py >> logs/performance.log 2>&1") | crontab -

# Create logs directory
mkdir -p logs

echo "✅ Performance optimizations deployed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Run the BigQuery SQL from performance_optimization.sql"
echo "2. Set REDIS_URL in your .env file (e.g., redis://localhost:6379)"
echo "3. Restart your web app to use the optimizations"
echo "4. Monitor performance with: python3 scripts/monitor_performance.py"
echo ""
echo "🔧 Configuration options in .env:"
echo "REDIS_URL=redis://localhost:6379"
echo "REDIS_CACHE_ENABLED=true"
echo "CACHE_REFRESH_INTERVAL=300"
echo "CACHE_CLEANUP_INTERVAL=3600"
echo "PERFORMANCE_REPORT_FILE=logs/performance_report.json"
