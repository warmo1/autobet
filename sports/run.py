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
        # Pass the selected mode as an argument to the shell script
        subprocess.run(['bash', script_path, args.mode], check=True)
    sp_fetch.set_defaults(func=_cmd_fetch_openfootball)

    # --- Ingest Football (OpenFootball) ---
    sp_ingest = sub.add_parser("ingest-openfootball", help="Ingest openfootball .txt files")
    sp_ingest.add_argument("--dir", required=True, help="Directory containing the season's .txt files")
    sp_ingest.add_argument("--comp", required=True, help="Name of the competition (e.g., 'Premier League')")
    sp_ingest.add_argument("--season", required=True, help="The season in YYYY/YY format (e.g., '2023/24')")
    def _cmd_ingest_openfootball(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_openfootball_dir(conn, args.dir, args.comp, args.season)
        print(f"[OpenFootball] Ingested {n} rows for {args.comp} {args.season}.")
        conn.close()
    sp_ingest.set_defaults(func=_cmd_ingest_openfootball)
    
    # --- Other Ingest Commands ---

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

    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
