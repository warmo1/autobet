"""Cleanup BigQuery temporary tables created by this project.

Usage:
  python autobet/scripts/bq_cleanup.py [--older DAYS]

Requires env: BQ_PROJECT, BQ_DATASET (and local ADC or service account).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure package root is importable when running as a script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.bq import get_bq_sink


def main() -> None:
    ap = argparse.ArgumentParser(description="Delete leftover _tmp tables in BigQuery dataset")
    ap.add_argument("--older", type=int, default=None, help="Only delete temp tables older than N days")
    args = ap.parse_args()

    sink = get_bq_sink()
    if not sink:
        print("BigQuery configuration missing or disabled. Set BQ_PROJECT/BQ_DATASET and BQ_WRITE_ENABLED=1.")
        sys.exit(2)
    try:
        n = sink.cleanup_temp_tables(older_than_days=args.older)
        print(f"Deleted {n} temp table(s)")
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

