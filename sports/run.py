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
# **FIX**: Changed the import from ingest_file to ingest_dir
from sports.ingest.football_premier_league import ingest_dir as ingest_pl_dir
from sports.ingest.football_openfootball import ingest_dir as ingest_openfootball_dir
from sports.ingest.fixtures import ingest_football_fixtures

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
    sp_fetch = sub.add_parser("fetch-openfootball", help="Download/update openfootball datasets")
    sp_fetch.add_argument("--mode", required=True, choices=['init', 'update'])
    def _cmd_fetch_openfootball(args):
        script_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'fetch_openfootball.sh')
        subprocess.run(['bash', script_path, args.mode], check=True)
    sp_fetch.set_defaults(func=_cmd_fetch_openfootball)

    # --- Historical Data Ingest Commands ---
    sp_fd = sub.add_parser("ingest-football-csv", help="Ingest football-data.co.uk CSVs")
    sp_fd.add_argument("--dir", required=True)
    def _cmd_ingest_fd(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_fd_dir(conn, args.dir)
        print(f"[Football-Data] Ingested {n} rows.")
        conn.close()
    sp_fd.set_defaults(func=_cmd_ingest_fd)

    sp_kaggle = sub.add_parser("ingest-football-kaggle", help="Ingest Kaggle domestic football CSV")
    sp_kaggle.add_argument("--file", required=True)
    def _cmd_ingest_kaggle(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_kaggle_file(conn, args.file)
        print(f"[Kaggle Football] Ingested {n} rows.")
        conn.close()
    sp_kaggle.set_defaults(func=_cmd_ingest_kaggle)

    sp_pl = sub.add_parser("ingest-pl-stats", help="Ingest Kaggle Premier League stats CSVs")
    sp_pl.add_argument("--dir", required=True)
    def _cmd_ingest_pl(args):
        conn = connect(db_url); init_schema(conn)
        # **FIX**: Changed the function call from ingest_pl_file to ingest_pl_dir
        n = ingest_pl_dir(conn, args.dir)
        print(f"[Premier League] Ingested {n} rows.")
        conn.close()
    sp_pl.set_defaults(func=_cmd_ingest_pl)

    sp_openfootball = sub.add_parser("ingest-openfootball", help="Recursively ingest all openfootball .txt files")
    sp_openfootball.add_argument("--dir", required=True)
    def _cmd_ingest_openfootball(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_openfootball_dir(conn, args.dir)
        print(f"[OpenFootball] Ingested a total of {n} rows.")
        conn.close()
    sp_openfootball.set_defaults(func=_cmd_ingest_openfootball)

    sp_cricket = sub.add_parser("ingest-cricket-csv", help="Ingest Cricsheet CSVs")
    sp_cricket.add_argument("--dir", required=True)
    def _cmd_ingest_cricket(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_cric_dir(conn, args.dir)
        print(f"[Cricket] Ingested {n} rows.")
        conn.close()
    sp_cricket.set_defaults(func=_cmd_ingest_cricket)

    # --- Live Fixtures Command ---
    sp_fixtures = sub.add_parser("ingest-fixtures", help="Ingest upcoming fixtures from a live API")
    sp_fixtures.add_argument("--sport", required=True, choices=['football'])
    def _cmd_ingest_fixtures(args):
        conn = connect(db_url); init_schema(conn)
        if args.sport == 'football':
            n = ingest_football_fixtures(conn)
            print(f"[Fixtures] Ingested {n} upcoming football events.")
        conn.close()
    sp_fixtures.set_defaults(func=_cmd_ingest_fixtures)

    # --- Suggestion Generation Command ---
    sp_suggest = sub.add_parser("generate-suggestions", help="Generate suggestions for upcoming fixtures")
    sp_suggest.add_argument("--sport", required=True, choices=['football'])
    def _cmd_generate_suggestions(args):
        conn = connect(db_url); init_schema(conn)
        if args.sport == 'football':
            generate_football_suggestions(conn)
        conn.close()
    sp_suggest.set_defaults(func=_cmd_generate_suggestions)

    # --- Application Commands ---
    sp_web = sub.add_parser("web", help="Run the web dashboard")
    def _cmd_web(args):
        app = create_app()
        app.run(host="0.0.0.0", port=8010)
    sp_web.set_defaults(func=_cmd_web)

    sp_telegram = sub.add_parser("telegram", help="Run the Telegram bot")
    def _cmd_telegram(args):
        run_bot()
    sp_telegram.set_defaults(func=_cmd_telegram)
    
    sp_betdaq = sub.add_parser("live-betdaq", help="Place a live bet on the Betdaq exchange")
    sp_betdaq.add_argument("--symbol", required=True)
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
