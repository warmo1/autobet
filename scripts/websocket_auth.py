#!/usr/bin/env python3
"""
WebSocket Service Authentication Helper
This script helps authenticate requests to the WebSocket service using gcloud auth
"""

import subprocess
import requests
import json
import sys

def get_gcloud_token():
    """Get access token from gcloud auth"""
    try:
        result = subprocess.run(['gcloud', 'auth', 'print-access-token'], 
                              capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"❌ Error getting gcloud token: {e}")
        return None

def make_authenticated_request(method, url, data=None):
    """Make authenticated request to WebSocket service"""
    token = get_gcloud_token()
    if not token:
        return None
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    try:
        if method.upper() == 'POST':
            response = requests.post(url, headers=headers, json=data or {})
        else:
            response = requests.get(url, headers=headers)
        
        return response
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return None

def main():
    if len(sys.argv) < 2:
        print("Usage: python websocket_auth.py {start|stop|status}")
        sys.exit(1)
    
    action = sys.argv[1]
    base_url = "https://websocket-subscription-539619253741.europe-west2.run.app"
    
    if action == "start":
        print("🚀 Starting WebSocket service...")
        response = make_authenticated_request('POST', f"{base_url}/start")
    elif action == "stop":
        print("🛑 Stopping WebSocket service...")
        response = make_authenticated_request('POST', f"{base_url}/stop")
    elif action == "status":
        print("📊 Checking WebSocket service status...")
        response = make_authenticated_request('GET', f"{base_url}/health")
    else:
        print("❌ Invalid action. Use: start, stop, or status")
        sys.exit(1)
    
    if response:
        print(f"Status Code: {response.status_code}")
        try:
            print(json.dumps(response.json(), indent=2))
        except:
            print(response.text)
    else:
        print("❌ Request failed")

if __name__ == "__main__":
    main()
