"""Utilities for interacting with Google Cloud services.

This module provides thin wrappers around Pub/Sub and Cloud Storage so the
application can start to adopt Google Cloud's native data ingestion tools.
"""
from __future__ import annotations

import base64
import json
from typing import Dict, Any

from google.cloud import pubsub_v1, storage


def publish_pubsub_message(project_id: str, topic_id: str, payload: Dict[str, Any]) -> str:
    """Publish a JSON payload to a Pub/Sub topic and return the message ID."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    data = json.dumps(payload).encode("utf-8")
    future = publisher.publish(topic_path, data)
    return future.result()


def parse_pubsub_envelope(envelope: Dict[str, Any]) -> Dict[str, Any]:
    """Decode a Pub/Sub push request envelope into a Python dictionary."""
    if not envelope or "message" not in envelope:
        raise ValueError("Invalid Pub/Sub message format")
    message = envelope["message"]
    data = message.get("data", "")
    if data:
        decoded = base64.b64decode(data).decode("utf-8")
        return json.loads(decoded)
    return {}


def upload_text_to_bucket(bucket_name: str, destination_blob: str, text: str) -> None:
    """Upload arbitrary text content to a Cloud Storage bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_string(text)
