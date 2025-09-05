from __future__ import annotations

"""Ensure BigQuery views required by the app exist (idempotent)."""

import os
from dotenv import load_dotenv
from pathlib import Path
import sys

# Ensure repo root on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.bq import get_bq_sink


def main() -> None:
    load_dotenv()
    sink = get_bq_sink()
    if not sink or not sink.enabled:
        raise SystemExit("BigQuery not configured. Set BQ_WRITE_ENABLED=true, BQ_PROJECT, BQ_DATASET, BQ_LOCATION.")
    sink.ensure_views()
    print("Views ensured in", f"{sink.project}.{sink.dataset}")


if __name__ == "__main__":
    main()
