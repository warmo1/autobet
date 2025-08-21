import os
import argparse
import subprocess
from dotenv import load_dotenv
from sports.db import connect
from sports.schema import init_schema

# Import all ingestor functions
from sports.ingest.football_fd import ingest_dir as ingest_fd_dir
from sports.ingest.cricket_cricsheet import ingest_dir as ingest_cric_dir
# ... (other ingestors)

# Import suggestion and other core functions
from sports.suggest import generate_football_suggestions
from sports.webapp import create_app
from sports.telegram_bot import run_bot

def main(argv=None):
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    
    p = argparse.ArgumentParser(description="Sports Betting Bot")
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- Ingest Commands ---
    # ... (all your ingest commands)

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

    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
