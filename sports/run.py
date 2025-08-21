import os
import argparse
import subprocess
from dotenv import load_dotenv
from sports.db import connect
from sports.schema import init_schema
from sports.ingest.football_fd import ingest_dir as ingest_fd_dir
from sports.ingest.cricket_cricsheet import ingest_dir as ingest_cric_dir
from sports.ingest.football_kaggle import ingest_file as ingest_kaggle_file
from sports.ingest.football_premier_league import ingest_dir as ingest_pl_dir
from sports.ingest.football_openfootball import ingest_dir as ingest_openfootball_dir

def main(argv=None):
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    
    p = argparse.ArgumentParser(description="Sports Betting Bot")
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- Fetch OpenFootball Data Command ---
    sp_fetch = sub.add_parser("fetch-openfootball", help="Download/update openfootball datasets from GitHub")
    sp_fetch.add_argument("--mode", required=True, choices=['init', 'update'], help="'init' for first-time bulk download, 'update' for daily changes.")
    def _cmd_fetch_openfootball(args):
        script_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'fetch_openfootball.sh')
        subprocess.run(['bash', script_path, args.mode], check=True)
    sp_fetch.set_defaults(func=_cmd_fetch_openfootball)

    # --- Ingest Football (OpenFootball) ---
    # **FIX**: Removed the unnecessary --comp and --season arguments
    sp_openfootball = sub.add_parser("ingest-openfootball", help="Recursively ingest all openfootball .txt files in a directory")
    sp_openfootball.add_argument("--dir", required=True, help="Path to the root of a cloned openfootball repository (e.g., data/football/openfootball/england)")
    def _cmd_ingest_openfootball(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_openfootball_dir(conn, args.dir)
        print(f"[OpenFootball] Ingested a total of {n} rows.")
        conn.close()
    sp_openfootball.set_defaults(func=_cmd_ingest_openfootball)
    
    # --- Other Ingest Commands ---
    # ... (Your other ingest commands for Kaggle, Premier League, etc. would go here)

    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
