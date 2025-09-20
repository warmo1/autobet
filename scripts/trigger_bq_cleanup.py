#!/usr/bin/env python3
"""Trigger BigQuery cleanup job immediately.

Usage:
  python scripts/trigger_bq_cleanup.py [--older DAYS] [--aggressive]

This will publish a cleanup task to the Pub/Sub topic for immediate processing.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure package root is importable when running as a script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.gcp import publish_pubsub_message
from sports.config import cfg
import os


def main() -> None:
    ap = argparse.ArgumentParser(description="Trigger BigQuery cleanup job")
    ap.add_argument("--older", type=int, default=1, help="Only delete temp tables older than N days (default: 1)")
    ap.add_argument("--aggressive", action="store_true", help="Delete ALL temp tables (older_than_days=0)")
    ap.add_argument("--project", default=(os.getenv("GCP_PROJECT") or os.getenv("BQ_PROJECT") or cfg.bq_project))
    ap.add_argument("--topic", default=(os.getenv("PUBSUB_TOPIC_ID") or "ingest-jobs"))
    args = ap.parse_args()

    if not args.project:
        print("ERROR: Project not specified. Set GCP_PROJECT/BQ_PROJECT or use --project")
        sys.exit(1)

    # Determine older_than_days value
    older_days = 0 if args.aggressive else args.older
    
    payload = {
        "task": "cleanup_bq_temps",
        "older_than_days": older_days
    }
    
    try:
        msg_id = publish_pubsub_message(args.project, args.topic, payload)
        print(f"Published cleanup task: {msg_id}")
        print(f"  - Project: {args.project}")
        print(f"  - Topic: {args.topic}")
        print(f"  - Older than days: {older_days}")
        print(f"  - Aggressive mode: {args.aggressive}")
        print("\nThe cleanup job will be processed by the Cloud Run service shortly.")
    except Exception as e:
        print(f"ERROR: Failed to publish cleanup task: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
