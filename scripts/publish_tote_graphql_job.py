"""Publish a Tote GraphQL fetch job to Pub/Sub.

Message contract consumed by sports/gcp_ingest_service.py:
{
  "provider": "tote",
  "op": "graphql",
  "query": "<GraphQL string>",
  "variables": {...},
  "bucket": "<gcs bucket>",
  "name": "<object path>",
  "bq": {"table": "raw_tote", "sport": "horse_racing", "entity_id": "optional"}
}

The service will:
- Execute the query using configured TOTE_API_KEY/TOTE_GRAPHQL_URL
- Store JSON response at gs://bucket/name
- If bq.table==raw_tote and BQ_* envs are set, append to BigQuery raw_tote
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

# Ensure repo root (containing 'sports') is on sys.path when run as a script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.gcp import publish_pubsub_message


def main() -> None:
    p = argparse.ArgumentParser(description="Publish a Tote GraphQL job")
    p.add_argument("--project", required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--bucket", required=True)
    p.add_argument("--name", required=True, help="GCS object path for JSON output")
    p.add_argument("--entity-id", help="Optional entity id to tag in BQ raw")
    p.add_argument("--sport", default="horse_racing")
    p.add_argument("--query-file", required=True, help="Path to a .graphql or .txt containing the query")
    p.add_argument("--vars", default="{}", help='JSON string for variables, e.g. {"first":100}')
    args = p.parse_args()

    with open(args.query_file, "r", encoding="utf-8") as fh:
        query = fh.read()

    try:
        variables = json.loads(args.vars)
    except Exception:
        variables = {}

    payload = {
        "provider": "tote",
        "op": "graphql",
        "query": query,
        "variables": variables,
        "bucket": args.bucket,
        "name": args.name,
        "bq": {"table": "raw_tote", "sport": args.sport, "entity_id": args.entity_id or ""},
    }
    msg_id = publish_pubsub_message(args.project, args.topic, payload)
    print("Published message", msg_id)


if __name__ == "__main__":
    main()
