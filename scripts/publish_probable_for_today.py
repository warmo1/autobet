"""Publish probable-odds jobs for today's open WIN products.

Usage:
  python autobet/scripts/publish_probable_for_today.py \
    --project autobet-470818 --topic ingest-jobs \
    --bucket autobet-470818-data [--limit 50]

Requires: GOOGLE_APPLICATION_CREDENTIALS or gcloud ADC; BQ_PROJECT/BQ_DATASET env used for selection.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.gcp import publish_pubsub_message
from sports.config import cfg


def main() -> None:
    ap = argparse.ArgumentParser(description="Publish probable-odds jobs for today's open WIN products")
    ap.add_argument("--project", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--bq-project", help="Override BQ project (defaults to env/cfg)")
    ap.add_argument("--bq-dataset", help="Override BQ dataset (defaults to env/cfg)")
    args = ap.parse_args()

    from google.cloud import bigquery
    # Resolve BQ project/dataset: CLI > env > cfg
    bq_project = args.bq_project or os.getenv("BQ_PROJECT") or cfg.bq_project
    bq_dataset = args.bq_dataset or os.getenv("BQ_DATASET") or cfg.bq_dataset
    if not (bq_project and bq_dataset):
        raise SystemExit("BQ project/dataset missing. Provide --bq-project/--bq-dataset or set BQ_PROJECT/BQ_DATASET or .env")

    client = bigquery.Client(project=bq_project)
    sql = f"""
    SELECT product_id
    FROM `{bq_project}.{bq_dataset}.tote_products`
    WHERE UPPER(bet_type)='WIN' AND status='OPEN' AND DATE(SUBSTR(start_iso,1,10))=CURRENT_DATE()
    ORDER BY start_iso
    LIMIT {max(1, int(args.limit))}
    """
    rows = list(client.query(sql).result())
    if not rows:
        print("No open WIN products found for today.")
        return
    n = 0
    for r in rows:
        pid = r[0]
        path = f"/v1/products/{pid}/probable-odds"
        name = f"raw/tote/probable/{pid}.json"
        payload = {
            "provider": "tote",
            "op": "probable",
            "path": path,
            "bucket": args.bucket,
            "name": name,
        }
        msg_id = publish_pubsub_message(args.project, args.topic, payload)
        print(f"Published probable odds for {pid}: {msg_id}")
        n += 1
    print(f"Done. Published {n} probable-odds jobs.")


if __name__ == "__main__":
    main()
