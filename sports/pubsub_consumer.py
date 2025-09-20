"""
Pub/Sub Consumer for Real-time WebSocket Events

This module handles consuming real-time events from Pub/Sub topics
and publishing them to the in-memory event bus for SSE consumption.
"""

import json
import threading
import time
from typing import Dict, Any, Optional
from google.cloud import pubsub_v1
from google.api_core import retry

from .config import cfg
from .realtime import bus as event_bus


class PubSubConsumer:
    """Consumes Pub/Sub messages and publishes to event bus"""
    
    def __init__(self):
        self.subscriber = pubsub_v1.SubscriberClient()
        self.running = False
        self.threads = []
        
        # Topic mappings
        self.topics = {
            'tote-pool-total-changed': 'pool_total_changed',
            'tote-product-status-changed': 'product_status_changed',
            'tote-event-status-changed': 'event_status_changed',
            'tote-event-result-changed': 'event_result_changed',
            'tote-pool-dividend-changed': 'pool_dividend_changed',
            'tote-selection-status-changed': 'selection_status_changed',
            'tote-competitor-status-changed': 'competitor_status_changed',
            'tote-bet-lifecycle': 'bet_lifecycle'
        }
    
    def callback(self, message, event_type: str):
        """Process incoming Pub/Sub message"""
        try:
            # Decode message data
            data = json.loads(message.data.decode('utf-8'))
            
            # Extract event data
            event_data = data.get('data', {})
            timestamp = data.get('timestamp', time.time())
            
            # Add timestamp if not present
            if 'ts_ms' not in event_data:
                event_data['ts_ms'] = int(timestamp * 1000)
            
            # Publish to in-memory event bus
            event_bus.publish(event_type, event_data)
            
            print(f"Processed {event_type} event: {event_data.get('product_id', event_data.get('event_id', 'unknown'))}")
            
        except Exception as e:
            print(f"Error processing {event_type} message: {e}")
        finally:
            # Acknowledge message
            message.ack()
    
    def subscribe_to_topic(self, topic_name: str, event_type: str):
        """Subscribe to a specific Pub/Sub topic"""
        try:
            subscription_path = self.subscriber.subscription_path(
                cfg.gcp_project, 
                f"{topic_name}-sub"
            )
            
            # Create callback with event type
            callback_func = lambda message: self.callback(message, event_type)
            
            # Start streaming pull
            streaming_pull_future = self.subscriber.pull(
                request={"subscription": subscription_path},
                callback=callback_func,
                retry=retry.Retry(deadline=300)
            )
            
            print(f"Subscribed to {topic_name} -> {event_type}")
            return streaming_pull_future
            
        except Exception as e:
            print(f"Error subscribing to {topic_name}: {e}")
            return None
    
    def start(self):
        """Start consuming from all topics"""
        if self.running:
            print("Consumer already running")
            return
        
        self.running = True
        print("Starting Pub/Sub consumer...")
        
        # Subscribe to all topics
        for topic_name, event_type in self.topics.items():
            try:
                future = self.subscribe_to_topic(topic_name, event_type)
                if future:
                    self.threads.append(future)
            except Exception as e:
                print(f"Failed to subscribe to {topic_name}: {e}")
        
        print(f"Started {len(self.threads)} topic subscriptions")
    
    def stop(self):
        """Stop consuming from all topics"""
        if not self.running:
            return
        
        self.running = False
        print("Stopping Pub/Sub consumer...")
        
        # Cancel all futures
        for future in self.threads:
            try:
                future.cancel()
            except Exception as e:
                print(f"Error canceling subscription: {e}")
        
        self.threads.clear()
        print("Pub/Sub consumer stopped")
    
    def is_running(self) -> bool:
        """Check if consumer is running"""
        return self.running


# Global consumer instance
_consumer: Optional[PubSubConsumer] = None

def get_consumer() -> PubSubConsumer:
    """Get or create global consumer instance"""
    global _consumer
    if _consumer is None:
        _consumer = PubSubConsumer()
    return _consumer

def start_pubsub_consumer():
    """Start the Pub/Sub consumer in a background thread"""
    consumer = get_consumer()
    
    if consumer.is_running():
        print("Pub/Sub consumer already running")
        return
    
    def run_consumer():
        consumer.start()
        # Keep the thread alive
        while consumer.is_running():
            time.sleep(1)
    
    thread = threading.Thread(target=run_consumer, daemon=True)
    thread.start()
    print("Pub/Sub consumer started in background thread")

def stop_pubsub_consumer():
    """Stop the Pub/Sub consumer"""
    consumer = get_consumer()
    consumer.stop()

def is_consumer_running() -> bool:
    """Check if consumer is running"""
    consumer = get_consumer()
    return consumer.is_running()
