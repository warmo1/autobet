"""Cloud Run entrypoint to handle ingestion jobs from Pub/Sub.

The service expects Pub/Sub push messages containing JSON with the keys
``url`` (HTTP resource to fetch), ``bucket`` (Cloud Storage bucket), and
``name`` (destination object name). The referenced resource is downloaded
and stored in the given bucket.
"""
from __future__ import annotations

import os
import requests
from flask import Flask, request

from sports.gcp import parse_pubsub_envelope, upload_text_to_bucket

app = Flask(__name__)


@app.post("/")
def handle_pubsub() -> tuple[str, int]:
    """Endpoint for Pub/Sub push subscriptions."""
    try:
        payload = parse_pubsub_envelope(request.get_json())
    except ValueError:
        return ("Bad Request", 400)

    url = payload.get("url")
    bucket = payload.get("bucket")
    name = payload.get("name")

    if url and bucket and name:
        resp = requests.get(url)
        resp.raise_for_status()
        upload_text_to_bucket(bucket, name, resp.text)
    else:
        # If required fields are missing we still return 204 so Pub/Sub doesn't retry indefinitely.
        return ("Missing fields", 204)

    return ("", 204)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
