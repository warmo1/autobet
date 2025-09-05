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
from sports.config import cfg
from sports.providers.tote_api import ToteClient, ToteError

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

    # Support Tote-aware fetch when provider/op is specified, otherwise fallback to URL fetch
    provider = (payload.get("provider") or "").lower()
    op = (payload.get("op") or "").lower()

    if not (bucket and name):
        return ("Missing bucket/name", 204)

    try:
        if provider == "tote" and op:
            client = ToteClient()
            # For now support only GraphQL operations via 'graphql' with provided query/variables,
            # or simple REST path passthrough via 'path'.
            if op == "graphql":
                query = payload.get("query")
                variables = payload.get("variables") or {}
                if not query:
                    return ("Missing GraphQL query", 204)
                data = client.graphql(query, variables)
                upload_text_to_bucket(bucket, name, json.dumps(data))
            else:
                # REST path passthrough using requests with Tote auth header
                path = payload.get("path") or "/"
                base = cfg.tote_graphql_url or ""
                # Convert GraphQL base to REST base if needed by trimming trailing path
                # If caller passes full URL, use it directly
                if path.startswith("http://") or path.startswith("https://"):
                    full_url = path
                else:
                    base_root = base.split("/partner/")[0].rstrip("/") if base else ""
                    full_url = f"{base_root}{path}"
                headers = {"Authorization": f"Api-Key {cfg.tote_api_key}", "Accept": "application/json"}
                resp = requests.get(full_url, headers=headers, timeout=20)
                resp.raise_for_status()
                upload_text_to_bucket(bucket, name, resp.text)
        elif url:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            upload_text_to_bucket(bucket, name, resp.text)
        else:
            return ("Missing url/provider", 204)
    except ToteError as te:
        # Do not trigger Pub/Sub retry storms; log via response body
        return (f"Tote error: {str(te)[:500]}", 204)
    except requests.exceptions.Timeout:
        return ("Timeout", 204)
    except Exception as e:
        return (f"Error: {str(e)[:500]}", 204)

    return ("", 204)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
