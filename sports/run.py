import os
import argparse
from dotenv import load_dotenv
from sports.db import connect
from sports.schema import init_schema
from sports.ingest.football_fd import ingest_dir as ingest_fd_dir
from sports.ingest.cricket_cricsheet import ingest_dir as ingest_cric_dir
from sports.betdaq_api import place_bet_on_betdaq # Import the new Betdaq function

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
        conn.close()
    sp_football.set_defaults(func=_cmd_ingest_football)

    # --- Ingest Cricket Command ---
    sp_cricket = sub.add_parser("ingest-cricket-csv", help="Ingest Cricsheet CSVs")
    sp_cricket.add_argument("--dir", required=True, help="Directory containing *.csv files")
    def _cmd_ingest_cricket(args):
        conn = connect(db_url)
        init_schema(conn)
        n = ingest_cric_dir(conn, args.dir)
        print(f"[Cricket] Ingested {n} rows.")
        conn.close()
    sp_cricket.set_defaults(func=_cmd_ingest_cricket)

    # --- Live Betdaq Command ---
    sp_betdaq = sub.add_parser("live-betdaq", help="Place a live bet on the Betdaq exchange")
    sp_betdaq.add_argument("--symbol", required=True, help="The market symbol (e.g., 'Man Utd vs Liverpool')")
    sp_betdaq.add_argument("--odds", required=True, type=float, help="The decimal odds for the bet")
    sp_betdaq.add_argument("--stake", required=True, type=float, help="The stake for the bet")
    sp_betdaq.add_argument("--confirm", action="store_true", help="You must include this flag to confirm the bet")
    
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

    # --- Placeholder for other commands (web, suggest, telegram) ---
    # You can integrate your other commands back into this structure.

    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
