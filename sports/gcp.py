"""Utilities for interacting with Google Cloud services.

This module provides thin wrappers around Pub/Sub and Cloud Storage so the
application can start to adopt Google Cloud's native data ingestion tools.
"""
from __future__ import annotations

import base64
import json
from typing import Dict, Any

import os
import json
from google.cloud import pubsub_v1, storage
from typing import Optional

_VALID_GOOGLE_CRED_TYPES = {"service_account", "authorized_user"}

def _is_valid_google_credentials_file(path: str) -> bool:
    try:
        if not path or not os.path.exists(path):
            return False
        with open(path, "r", encoding="utf-8") as f:
            info = json.load(f)
        typ = info.get("type")
        return typ in _VALID_GOOGLE_CRED_TYPES
    except Exception:
        return False

def sanitize_adc_env() -> None:
    """If GOOGLE_APPLICATION_CREDENTIALS points at an invalid/placeholder file, ignore it.

    This avoids google.auth.default failing on a bundled placeholder JSON and
    allows fallback to Application Default Credentials (e.g., gcloud login) or
    other configured methods.
    """
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if sa_path and not _is_valid_google_credentials_file(sa_path):
        # Unset only in-process so other shells are unaffected
        try:
            del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        except Exception:
            pass

def _load_gcp_credentials():
    """Load service account credentials from env if provided; else None for ADC.

    Supports either:
    - GCP_SERVICE_ACCOUNT_JSON or GOOGLE_APPLICATION_CREDENTIALS_JSON with raw JSON
    - GOOGLE_APPLICATION_CREDENTIALS with a filesystem path
    """
    try:
        from google.oauth2 import service_account  # type: ignore
    except Exception:
        return None
    # JSON content in env
    sa_json_env = os.getenv("GCP_SERVICE_ACCOUNT_JSON") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if sa_json_env:
        try:
            info = json.loads(sa_json_env)
            return service_account.Credentials.from_service_account_info(info)
        except Exception:
            return None
    # File path in env
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if sa_path and os.path.exists(sa_path):
        try:
            return service_account.Credentials.from_service_account_file(sa_path)
        except Exception:
            return None
    return None


def publish_pubsub_message(project_id: str, topic_id: str, payload: Dict[str, Any]) -> str:
    """Publish a JSON payload to a Pub/Sub topic and return the message ID."""
    sanitize_adc_env()
    creds = _load_gcp_credentials()
    publisher = pubsub_v1.PublisherClient(credentials=creds) if creds else pubsub_v1.PublisherClient()
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
    sanitize_adc_env()
    creds = _load_gcp_credentials()
    client = storage.Client(credentials=creds) if creds else storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_string(text)
