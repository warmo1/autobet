import os
import argparse
from dotenv import load_dotenv
from sports.db import connect
from sports.schema import init_schema
from sports.ingest.football_fd import ingest_dir as ingest_fd_dir
from sports.ingest.cricket_cricsheet import ingest_dir as ingest_cric_dir
from sports.ingest.football_kaggle import ingest_file as ingest_kaggle_file
from sports.ingest.football_premier_league import ingest_file as ingest_pl_file # Import the new PL ingestor
from sports.betdaq_api import place_bet_on_betdaq

def main(argv=None):
    load_dotenv()
    db_url = os.getenv("DATABASE_URL", "sqlite:///sports_bot.db")
    
    p = argparse.ArgumentParser(description="Sports Betting Bot")
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- Ingest Football (football-data.co.uk) ---
    sp_football = sub.add_parser("ingest-football-csv", help="Ingest football-data.co.uk CSVs")
    sp_football.add_argument("--dir", required=True, help="Directory for *.csv files")
    def _cmd_ingest_football(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_fd_dir(conn, args.dir)
        print(f"[Football] Ingested {n} rows.")
        conn.close()
    sp_football.set_defaults(func=_cmd_ingest_football)

    # --- Ingest Football (Kaggle Domestic) ---
    sp_kaggle = sub.add_parser("ingest-football-kaggle", help="Ingest Kaggle domestic football CSV")
    sp_kaggle.add_argument("--file", required=True, help="Path to the main.csv file")
    def _cmd_ingest_kaggle(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_kaggle_file(conn, args.file)
        print(f"[Kaggle Football] Ingested {n} rows.")
        conn.close()
    sp_kaggle.set_defaults(func=_cmd_ingest_kaggle)

    # --- Ingest Football (Premier League Stats) ---
    sp_pl = sub.add_parser("ingest-pl-stats", help="Ingest Kaggle Premier League stats CSV")
    sp_pl.add_argument("--file", required=True, help="Path to the matches.csv file")
    def _cmd_ingest_pl(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_pl_file(conn, args.file)
        print(f"[Premier League] Ingested {n} rows.")
        conn.close()
    sp_pl.set_defaults(func=_cmd_ingest_pl)

    # --- Ingest Cricket ---
    sp_cricket = sub.add_parser("ingest-cricket-csv", help="Ingest Cricsheet CSVs")
    sp_cricket.add_argument("--dir", required=True, help="Directory for *.csv files")
    def _cmd_ingest_cricket(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_cric_dir(conn, args.dir)
        print(f"[Cricket] Ingested {n} rows.")
        conn.close()
    sp_cricket.set_defaults(func=_cmd_ingest_cricket)

    # --- Live Betdaq Command ---
    sp_betdaq = sub.add_parser("live-betdaq", help="Place a live bet on the Betdaq exchange")
    # ... (Betdaq arguments and function)
    
    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
