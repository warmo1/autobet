from __future__ import annotations

"""Run the Tote pool subscriber with BigQuery sink.

Usage:
  python autobet/scripts/run_pool_subscriber.py [--duration SEC]
Requires: .env with TOTE_API_KEY, TOTE_SUBSCRIPTIONS_URL (optional), and BQ_* envs.
"""

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.bq import get_bq_sink
from sports.providers.tote_subscriptions import run_subscriber


def main() -> None:
    ap = argparse.ArgumentParser(description="Run Tote pool subscriber (writes snapshots to BigQuery)")
    ap.add_argument("--duration", type=int, default=None, help="Optional run time in seconds")
    args = ap.parse_args()

    sink = get_bq_sink()
    if not sink:
        raise SystemExit("BigQuery not configured. Ensure BQ_WRITE_ENABLED=true and BQ_PROJECT/BQ_DATASET envs are set.")
    run_subscriber(sink, duration=args.duration)


if __name__ == "__main__":
    main()

