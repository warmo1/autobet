"""
WebSocket Subscription Service for Tote API

This Cloud Run service handles WebSocket subscriptions to the Tote API
and publishes real-time events to Pub/Sub topics for downstream consumption.
"""

import asyncio
import json
import time
import threading
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1

from .config import cfg
from .providers.tote_subscriptions import run_subscriber
from .bq import get_bq_sink
from .gcp import publish_pubsub_message

app = Flask(__name__)

# Global state
subscription_task: Optional[asyncio.Task] = None
subscription_running = False
pubsub_client: Optional[pubsub_v1.PublisherClient] = None

def get_pubsub_client() -> pubsub_v1.PublisherClient:
    """Get or create Pub/Sub client"""
    global pubsub_client
    if pubsub_client is None:
        pubsub_client = pubsub_v1.PublisherClient()
    return pubsub_client

def publish_event(event_type: str, data: Dict[str, Any]) -> None:
    """Publish event to appropriate Pub/Sub topic"""
    try:
        client = get_pubsub_client()
        topic_id = f"tote-{event_type.replace('_', '-')}"
        topic_path = client.topic_path(cfg.gcp_project, topic_id)
        
        message_data = {
            "event_type": event_type,
            "data": data,
            "timestamp": time.time(),
            "source": "websocket-service"
        }
        
        client.publish(
            topic_path,
            json.dumps(message_data).encode('utf-8'),
            event_type=event_type
        )
        
        print(f"Published {event_type} to {topic_id}")
        
    except Exception as e:
        print(f"Failed to publish {event_type}: {e}")

def event_callback(event_type: str, data: Dict[str, Any]) -> None:
    """Callback for WebSocket events - publishes to Pub/Sub"""
    try:
        publish_event(event_type, data)
    except Exception as e:
        print(f"Error in event callback for {event_type}: {e}")

async def run_websocket_subscription():
    """Run WebSocket subscription in background"""
    global subscription_running
    try:
        subscription_running = True
        print("Starting WebSocket subscription...")
        
        conn = get_bq_sink()
        if not conn:
            print("ERROR: BigQuery sink not available")
            return
            
        # Run subscription with event callback
        await asyncio.create_task(
            run_subscriber(conn, event_callback=event_callback)
        )
        
    except Exception as e:
        print(f"WebSocket subscription error: {e}")
    finally:
        subscription_running = False
        print("WebSocket subscription stopped")

def start_subscription_background():
    """Start WebSocket subscription in background thread"""
    global subscription_task
    
    if subscription_running:
        print("Subscription already running")
        return
        
    def run_in_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_websocket_subscription())
        finally:
            loop.close()
    
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    print("WebSocket subscription started in background thread")

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "ok",
        "subscription_running": subscription_running,
        "timestamp": time.time()
    }), 200

@app.route('/start', methods=['POST'])
def start_subscription():
    """Start WebSocket subscription"""
    global subscription_running
    
    if subscription_running:
        return jsonify({
            "status": "already_running",
            "message": "WebSocket subscription is already running"
        }), 200
    
    try:
        start_subscription_background()
        return jsonify({
            "status": "started",
            "message": "WebSocket subscription started"
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to start subscription: {str(e)}"
        }), 500

@app.route('/stop', methods=['POST'])
def stop_subscription():
    """Stop WebSocket subscription"""
    global subscription_running
    
    if not subscription_running:
        return jsonify({
            "status": "not_running",
            "message": "WebSocket subscription is not running"
        }), 200
    
    try:
        subscription_running = False
        return jsonify({
            "status": "stopped",
            "message": "WebSocket subscription stopped"
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to stop subscription: {str(e)}"
        }), 500

@app.route('/status')
def status():
    """Get subscription status"""
    return jsonify({
        "subscription_running": subscription_running,
        "timestamp": time.time(),
        "gcp_project": cfg.gcp_project
    }), 200

@app.route('/test-pubsub', methods=['POST'])
def test_pubsub():
    """Test Pub/Sub publishing"""
    try:
        test_data = {
            "test": True,
            "message": "Test message from WebSocket service",
            "timestamp": time.time()
        }
        
        publish_event("test_event", test_data)
        
        return jsonify({
            "status": "success",
            "message": "Test event published to Pub/Sub"
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to publish test event: {str(e)}"
        }), 500

if __name__ == '__main__':
    # Auto-start subscription when service starts
    start_subscription_background()
    app.run(host='0.0.0.0', port=8080, debug=False)
