import os
import argparse
from dotenv import load_dotenv
from sports.db import connect
from sports.schema import init_schema
from sports.ingest.football_fd import ingest_dir as ingest_fd_dir
from sports.ingest.cricket_cricsheet import ingest_dir as ingest_cric_dir
from sports.ingest.football_kaggle import ingest_file as ingest_kaggle_file
from sports.ingest.football_premier_league import ingest_dir as ingest_pl_dir
from sports.ingest.fixtures import ingest_football_fixtures
from sports.suggest import generate_football_suggestions
from sports.betdaq_api import place_bet_on_betdaq

def main(argv=None):
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        print("Error: DATABASE_URL not found in .env file. Please ensure it is set correctly.")
        return

    p = argparse.ArgumentParser(description="Sports Betting Bot")
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- Ingest Historical Data Commands ---

    sp_football = sub.add_parser("ingest-football-csv", help="Ingest football-data.co.uk CSVs")
    sp_football.add_argument("--dir", required=True, help="Directory for *.csv files")
    def _cmd_ingest_football(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_fd_dir(conn, args.dir)
        print(f"[Football] Ingested {n} rows.")
        conn.close()
    sp_football.set_defaults(func=_cmd_ingest_football)

    sp_kaggle = sub.add_parser("ingest-football-kaggle", help="Ingest Kaggle domestic football CSV")
    sp_kaggle.add_argument("--file", required=True, help="Path to the main.csv file")
    def _cmd_ingest_kaggle(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_kaggle_file(conn, args.file)
        print(f"[Kaggle Football] Ingested {n} rows.")
        conn.close()
    sp_kaggle.set_defaults(func=_cmd_ingest_kaggle)

    sp_pl = sub.add_parser("ingest-pl-stats", help="Ingest Kaggle Premier League stats CSVs")
    sp_pl.add_argument("--dir", required=True, help="Path to the directory containing fixtures.csv")
    def _cmd_ingest_pl(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_pl_dir(conn, args.dir)
        print(f"[Premier League] Ingested {n} rows.")
        conn.close()
    sp_pl.set_defaults(func=_cmd_ingest_pl)

    sp_cricket = sub.add_parser("ingest-cricket-csv", help="Ingest Cricsheet CSVs")
    sp_cricket.add_argument("--dir", required=True, help="Directory for *.csv files")
    def _cmd_ingest_cricket(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_cric_dir(conn, args.dir)
        print(f"[Cricket] Ingested {n} rows.")
        conn.close()
    sp_cricket.set_defaults(func=_cmd_ingest_cricket)

    # --- Ingest Live Fixtures Command ---
    sp_fixtures = sub.add_parser("ingest-fixtures", help="Ingest upcoming fixtures from a live API")
    sp_fixtures.add_argument("--sport", required=True, choices=['football'], help="The sport to fetch fixtures for")
    def _cmd_ingest_fixtures(args):
        conn = connect(db_url); init_schema(conn)
        if args.sport == 'football':
            n = ingest_football_fixtures(conn)
            print(f"[Fixtures] Ingested {n} upcoming football events.")
        conn.close()
    sp_fixtures.set_defaults(func=_cmd_ingest_fixtures)

    # --- Generate Suggestions Command ---
    sp_suggest = sub.add_parser("generate-suggestions", help="Generate betting suggestions for upcoming fixtures")
    sp_suggest.add_argument("--sport", required=True, choices=['football'], help="The sport to generate suggestions for")
    def _cmd_generate_suggestions(args):
        conn = connect(db_url); init_schema(conn)
        if args.sport == 'football':
            generate_football_suggestions(conn)
        conn.close()
    sp_suggest.set_defaults(func=_cmd_generate_suggestions)

    # --- Live Betdaq Command ---
    sp_betdaq = sub.add_parser("live-betdaq", help="Place a live bet on the Betdaq exchange")
    sp_betdaq.add_argument("--symbol", required=True, help="The market symbol")
    sp_betdaq.add_argument("--odds", required=True, type=float)
    sp_betdaq.add_argument("--stake", required=True, type=float)
    sp_betdaq.add_argument("--confirm", action="store_true")
    def _cmd_live_betdaq(args):
        if not args.confirm:
            print("Error: You must add the --confirm flag to place a live bet.")
            return
        try:
            result = place_bet_on_betdaq(args.symbol, args.odds, args.stake)
            print("Bet placement result:", result)
        except Exception as e:
            print(f"An error occurred: {e}")
    sp_betdaq.set_defaults(func=_cmd_live_betdaq)

    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
