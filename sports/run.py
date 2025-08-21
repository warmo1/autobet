import os
import argparse
import subprocess
from dotenv import load_dotenv
from sports.db import connect
from sports.schema import init_schema

# Import all ingestor functions
from sports.ingest.football_fd import ingest_dir as ingest_fd_dir
from sports.ingest.cricket_cricsheet import ingest_dir as ingest_cric_dir
from sports.ingest.football_kaggle import ingest_file as ingest_kaggle_file
from sports.ingest.football_premier_league import ingest_dir as ingest_pl_dir
from sports.ingest.football_openfootball import ingest_dir as ingest_openfootball_dir
from sports.ingest.fixtures import ingest_football_fixtures
# **FIX**: Changed the import to the new, generic function name
from sports.ingest.bbc_fixtures import ingest_bbc_fixtures 

# Import suggestion and other core functions
from sports.suggest import generate_football_suggestions
from sports.betdaq_api import place_bet_on_betdaq
from sports.webapp import create_app
from sports.telegram_bot import run_bot

def main(argv=None):
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        print("Error: DATABASE_URL not found in .env file.")
        return

    p = argparse.ArgumentParser(description="Sports Betting Bot")
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- Data Fetching Command ---
    # ... (existing fetch commands)

    # --- Historical Data Ingest Commands ---
    # ... (existing historical ingest commands)

    # --- Live Fixtures Commands ---
    sp_fixtures = sub.add_parser("ingest-fixtures", help="Ingest upcoming fixtures from a live API")
    sp_fixtures.add_argument("--sport", required=True, choices=['football'])
    def _cmd_ingest_fixtures(args):
        conn = connect(db_url); init_schema(conn)
        if args.sport == 'football':
            n = ingest_football_fixtures(conn)
            print(f"[Fixtures] Ingested {n} upcoming football events from API.")
        conn.close()
    sp_fixtures.set_defaults(func=_cmd_ingest_fixtures)

    sp_bbc = sub.add_parser("ingest-bbc-fixtures", help="Ingest upcoming fixtures by scraping the BBC Sport website")
    sp_bbc.add_argument("--sport", required=True, choices=['football', 'cricket', 'rugby-union'])
    def _cmd_ingest_bbc(args):
        conn = connect(db_url); init_schema(conn)
        # **FIX**: Call the new, flexible ingestor with the chosen sport
        ingest_bbc_fixtures(conn, args.sport)
        conn.close()
    sp_bbc.set_defaults(func=_cmd_ingest_bbc)


    # --- Suggestion Generation Command ---
    # ... (existing suggestion command)

    # --- Application Commands ---
    # ... (existing web, telegram, betdaq commands)

    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
