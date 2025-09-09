"""Publish a probable-odds job to Pub/Sub (task or legacy).

Example:
  python autobet/scripts/publish_tote_probable_job.py \
    --project autobet-470818 --topic ingest-jobs --event-id <EID>

Legacy (not recommended; requires legacy worker):
  python autobet/scripts/publish_tote_probable_job.py \
    --project autobet-470818 --topic ingest-jobs \
    --bucket autobet-470818-data --path /v1/products/<id>/probable-odds \
    --name raw/tote/probable/<id>.json --mode rest
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
    ap = argparse.ArgumentParser(description="Publish a probable-odds job")
    import os
    from sports.config import cfg as _cfg
    ap.add_argument("--project", default=(os.getenv("GCP_PROJECT") or os.getenv("BQ_PROJECT") or _cfg.bq_project))
    ap.add_argument("--topic", default=(os.getenv("PUBSUB_TOPIC_ID") or "ingest-jobs"))
    # Task mode (preferred)
    ap.add_argument("--event-id", help="Event ID for task-based ingest (preferred)")
    # Legacy REST mode
    ap.add_argument("--bucket", help="GCS bucket (legacy mode)")
    ap.add_argument("--path", help="REST path or full URL (legacy mode)")
    ap.add_argument("--name", help="GCS object path for JSON output (legacy mode)")
    ap.add_argument("--mode", choices=["task","rest"], default="task")
    args = ap.parse_args()

    if not args.project:
        raise SystemExit("Pub/Sub project not set. Provide --project or set GCP_PROJECT/BQ_PROJECT in .env")

    if args.mode == "task":
        if not args.event_id:
            raise SystemExit("--event-id is required in task mode")
        payload = {"task": "ingest_probable_odds", "event_id": args.event_id}
        msg_id = publish_pubsub_message(args.project, args.topic, payload)
        print("Published probable-odds task", msg_id)
    else:
        # Legacy mode
        if not (args.bucket and args.path and args.name):
            raise SystemExit("--bucket, --path and --name are required in rest mode")
        payload = {
            "provider": "tote",
            "op": "probable",
            "path": args.path,
            "bucket": args.bucket,
            "name": args.name,
        }
        msg_id = publish_pubsub_message(args.project, args.topic, payload)
        print("Published probable-odds legacy message", msg_id)


if __name__ == "__main__":
    main()
