import argparse
import os
import time
from datetime import date, timedelta

# This script assumes it's run from a context where the `autobet` package is available.
# Example from project root: python -m autobet.sports.backfill --start 2023-01-01

# Set environment variables if not already set, for local runs
if 'BQ_PROJECT' not in os.environ:
    os.environ['BQ_PROJECT'] = 'autobet-470818'
if 'BQ_DATASET' not in os.environ:
    os.environ['BQ_DATASET'] = 'autobet'

# These imports are based on the structure seen in webapp.py
from autobet.sports.providers.tote_api import ToteClient
from autobet.sports.ingest.tote_products import ingest_products
from autobet.sports.db import get_db


def backfill_range(start_date: date, end_date: date, bet_types: list[str]):
    """
    Iterates through a date range and ingests historical data for each day.
    """
    print(f"Starting backfill from {start_date.isoformat()} to {end_date.isoformat()}...")

    try:
        client = ToteClient()
        sink = get_db()
        if not sink or not sink.enabled:
            print("BigQuery sink is not configured. Aborting.")
            return
    except Exception as e:
        print(f"Failed to initialize clients: {e}")
        return

    current_date = start_date
    while current_date <= end_date:
        date_iso = current_date.isoformat()
        print(f"--- Processing {date_iso} ---")

        try:
            # Ingest products. For historical data, we want 'CLOSED' status.
            # The 'RESULTED' status is not a valid filter for the Tote API's product query;
            # 'CLOSED' is the correct status for products that are no longer open for betting.
            print(f"  Ingesting 'CLOSED' products for {date_iso}...")
            ingest_products(
                sink,
                client,
                date_iso=date_iso,
                status="CLOSED",
                first=1000,  # Fetch a large number to get all products for the day
                # Pass None if list is empty, which the ingestor should interpret as "all types".
                bet_types=(bet_types or None)
            )

            print(f"  Successfully processed {date_iso}.")

        except Exception as e:
            print(f"  [ERROR] Failed to process {date_iso}: {e}")

        # Move to the next day
        current_date += timedelta(days=1)
        # Be a good citizen and don't hammer the API
        time.sleep(2)

    print("Backfill complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill historical Tote data into BigQuery.")
    parser.add_argument("--start", required=True, help="Start date for backfill in YYYY-MM-DD format.")
    parser.add_argument("--end", default=date.today().isoformat(), help="End date for backfill in YYYY-MM-DD format (defaults to today).")
    parser.add_argument("--types", default="WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA", help="Comma-separated bet types to ingest.")
    args = parser.parse_args()

    try:
        start_dt = date.fromisoformat(args.start)
        end_dt = date.fromisoformat(args.end)
        if start_dt > end_dt:
            raise ValueError("Start date cannot be after end date.")
    except ValueError as e:
        print(f"Invalid date: {e}. Please use YYYY-MM-DD format.")
        exit(1)

    bet_types_list = [s.strip().upper() for s in (args.types or '').split(',') if s.strip()]

    backfill_range(start_dt, end_dt, bet_types_list)