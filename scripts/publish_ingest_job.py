"""CLI helper to publish ingestion jobs to a Pub/Sub topic."""
from __future__ import annotations

import argparse

from sports.gcp import publish_pubsub_message


def main() -> None:
    p = argparse.ArgumentParser(description="Publish an ingestion job to Pub/Sub")
    p.add_argument("--project", required=True, help="GCP project ID")
    p.add_argument("--topic", required=True, help="Pub/Sub topic ID")
    p.add_argument("--url", required=True, help="Source URL to download")
    p.add_argument("--bucket", required=True, help="Cloud Storage bucket to write to")
    p.add_argument("--name", required=True, help="Destination object name")
    args = p.parse_args()

    payload = {"url": args.url, "bucket": args.bucket, "name": args.name}
    msg_id = publish_pubsub_message(args.project, args.topic, payload)
    print("Published message", msg_id)


if __name__ == "__main__":
    main()
