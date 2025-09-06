"""Publish a Tote probable-odds fetch job to Pub/Sub.

Example:
  python autobet/scripts/publish_tote_probable_job.py \
    --project autobet-470818 --topic ingest-jobs \
    --bucket autobet-470818-data --path /v1/products/<id>/probable-odds \
    --name raw/tote/probable/<id>.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.gcp import publish_pubsub_message


def main() -> None:
    ap = argparse.ArgumentParser(description="Publish a Tote probable-odds job")
    ap.add_argument("--project", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--path", required=True, help="REST path to probable odds endpoint or full URL")
    ap.add_argument("--name", required=True, help="GCS object path for JSON output")
    args = ap.parse_args()

    payload = {
        "provider": "tote",
        "op": "probable",
        "path": args.path,
        "bucket": args.bucket,
        "name": args.name,
        # BigQuery sink handled server-side (raw_tote_probable_odds)
    }
    msg_id = publish_pubsub_message(args.project, args.topic, payload)
    print("Published probable-odds message", msg_id)


if __name__ == "__main__":
    main()

