import os
import argparse
from dotenv import load_dotenv
from sports.db import connect
from sports.schema import init_schema
from sports.ingest.football_fd import ingest_dir as ingest_fd_dir
from sports.ingest.cricket_cricsheet import ingest_dir as ingest_cric_dir

def main(argv=None):
    load_dotenv()
    db_url = os.getenv("DATABASE_URL", "sqlite:///sports_bot.db")
    
    p = argparse.ArgumentParser(description="Sports Betting Bot")
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- Ingest Football Command ---
    sp_football = sub.add_parser("ingest-football-csv", help="Ingest football-data.co.uk CSVs")
    sp_football.add_argument("--dir", required=True, help="Directory containing *.csv season files")
    def _cmd_ingest_football(args):
        conn = connect(db_url)
        init_schema(conn)
        n = ingest_fd_dir(conn, args.dir)
        print(f"[Football] Ingested {n} rows.")
    sp_football.set_defaults(func=_cmd_ingest_football)

    # --- Ingest Cricket Command ---
    sp_cricket = sub.add_parser("ingest-cricket-csv", help="Ingest Cricsheet CSVs")
    sp_cricket.add_argument("--dir", required=True, help="Directory containing *.csv files")
    def _cmd_ingest_cricket(args):
        conn = connect(db_url)
        init_schema(conn)
        n = ingest_cric_dir(conn, args.dir)
        print(f"[Cricket] Ingested {n} rows.")
    sp_cricket.set_defaults(func=_cmd_ingest_cricket)

    # --- Placeholder for existing commands ---
    # You can add your 'web', 'suggest', 'telegram' commands back in here
    # For example:
    # sp_web = sub.add_parser("web", help="Run the web dashboard")
    # def _cmd_web(args):
    #     print("Web command placeholder")
    # sp_web.set_defaults(func=_cmd_web)

    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
