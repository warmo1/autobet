import os
import argparse
from dotenv import load_dotenv
from sports.db import connect
from sports.schema import init_schema
from sports.ingest.football_fd import ingest_dir as ingest_fd_dir
from sports.ingest.cricket_cricsheet import ingest_dir as ingest_cric_dir
from sports.ingest.football_kaggle import ingest_file as ingest_kaggle_file
from sports.ingest.football_premier_league import ingest_dir as ingest_pl_dir
from sports.ingest.football_openfootball import ingest_dir as ingest_openfootball_dir # New import

def main(argv=None):
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    
    p = argparse.ArgumentParser(description="Sports Betting Bot")
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- Ingest Football (football-data.co.uk) ---
    # ... (existing command)

    # --- Ingest Football (Kaggle Domestic) ---
    # ... (existing command)

    # --- Ingest Football (Premier League Stats) ---
    # ... (existing command)
    
    # --- Ingest Football (OpenFootball) ---
    sp_openfootball = sub.add_parser("ingest-openfootball", help="Ingest openfootball .txt files")
    sp_openfootball.add_argument("--dir", required=True, help="Directory containing the season's .txt files")
    sp_openfootball.add_argument("--comp", required=True, help="Name of the competition (e.g., 'Premier League')")
    sp_openfootball.add_argument("--season", required=True, help="The season in YYYY/YY format (e.g., '2023/24')")
    def _cmd_ingest_openfootball(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_openfootball_dir(conn, args.dir, args.comp, args.season)
        print(f"[OpenFootball] Ingested {n} rows for {args.comp} {args.season}.")
        conn.close()
    sp_openfootball.set_defaults(func=_cmd_ingest_openfootball)

    # --- Ingest Cricket ---
    # ... (existing command)
    
    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
