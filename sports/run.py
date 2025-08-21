import os
import argparse
from dotenv import load_dotenv

from sports.db import connect
from sports.schema import init_schema

# Ingestors
from sports.ingest.football_fd import ingest_dir as ingest_fd_dir
from sports.ingest.cricket_cricsheet import ingest_dir as ingest_cric_dir
from sports.ingest.football_kaggle import ingest_file as ingest_kaggle_file
from sports.ingest.football_premier_league import ingest_file as ingest_pl_file

# Telegram bot runner
from sports.telegram_bot import run_bot

# Optional exchange integration (kept as a stub for now)
try:
    from sports.betdaq_api import place_bet_on_betdaq  # noqa: F401
except Exception:  # pragma: no cover
    place_bet_on_betdaq = None


def main(argv=None):
    load_dotenv()
    db_url = os.getenv("DATABASE_URL", "sqlite:///sports_bot.db")

    p = argparse.ArgumentParser(description="Sports Betting Bot")
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- Init DB ---
    sp_init = sub.add_parser("initdb", help="Create/upgrade database schema")
    def _cmd_initdb(args):
        conn = connect(db_url)
        init_schema(conn)
        conn.close()
        print("[DB] Schema initialised.")
    sp_init.set_defaults(func=_cmd_initdb)

    # --- Ingest Football (football-data.co.uk) ---
    sp_football = sub.add_parser("ingest-football-csv", help="Ingest football-data.co.uk CSVs")
    sp_football.add_argument("--dir", required=True, help="Directory for *.csv files")
    def _cmd_ingest_football(args):
        conn = connect(db_url); init_schema(conn)
        n = ingest_fd_dir(conn, args.dir)
        print(f"[Football FD] Ingested {n} rows.")
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

    # --- Telegram bot ---
    sp_bot = sub.add_parser("telegram-bot", help="Run the Telegram bot")
    sp_bot.add_argument("--token", required=False, help="Telegram Bot Token (or set TELEGRAM_TOKEN)")
    sp_bot.add_argument("--hour", type=int, default=8, help="Daily digest hour (Europe/London)")
    sp_bot.add_argument("--minute", type=int, default=30, help="Daily digest minute (Europe/London)")
    def _cmd_bot(args):
        token = args.token or os.getenv("TELEGRAM_TOKEN")
        if not token:
            raise SystemExit("TELEGRAM_TOKEN not set and --token not provided")
        run_bot(token, db_url=db_url, digest_hour=args.hour, digest_minute=args.minute)
    sp_bot.set_defaults(func=_cmd_bot)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
