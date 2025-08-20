import argparse
from .config import cfg
from .db import get_conn, init_schema
from .ingest import ingest_football_csv_dir, ingest_horse_racing_data, ingest_tennis_data_from_api, ingest_football_fixtures_from_api
from .suggest import run_suggestions
from .news import fetch_and_store_news
from .llm import summarise_news
from .db import insert_insight
from .webapp import create_app
from .telegram_bot import run_bot

def cmd_ingest(args):
    """Handles the data ingestion command."""
    conn = get_conn(cfg.database_url)
    init_schema(conn)
    
    if args.sport == 'football':
        # If a CSV directory is provided, use it for historical data.
        # Otherwise, fetch live fixtures from the API.
        if args.csv_dir:
            n = ingest_football_csv_dir(conn, args.csv_dir)
            print(f"Ingested {n} historical football rows from CSV.")
        else:
            n = ingest_football_fixtures_from_api(conn)
            print(f"Fetched {n} live football fixtures from API.")
    
    elif args.sport == 'horse_racing':
        n = ingest_horse_racing_data(conn)
        print(f"Ingested {n} horse racing entries.")
    
    elif args.sport == 'tennis':
        n = ingest_tennis_data_from_api(conn)
        print(f"Ingested {n} tennis matches.")

def cmd_suggest(args):
    conn = get_conn(cfg.database_url); init_schema(conn)
    picks = run_suggestions(conn, args.sport)
    print(f"Generated {len(picks)} suggestions for {args.sport}.")

# ... (other cmd functions remain the same) ...
def cmd_web(args):
    app = create_app()
    app.run(host="0.0.0.0", port=8010, debug=True)

def cmd_telegram(args):
    """Starts the Telegram bot."""
    run_bot()

def main(argv=None):
    p = argparse.ArgumentParser(description="Sports Betting Bot (Starter)")
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- Ingest Command ---
    sp = sub.add_parser("ingest", help="Ingest data from API or historical CSVs")
    sp.add_argument("--sport", type=str, required=True, choices=['football', 'horse_racing', 'tennis'])
    # **FIX**: --csv-dir is now optional
    sp.add_argument("--csv-dir", type=str, required=False, help="Path to historical CSVs (football only)")
    sp.set_defaults(func=cmd_ingest)

    # --- Suggest Command ---
    sp = sub.add_parser("suggest", help="Generate value suggestions")
    sp.add_argument("--sport", type=str, required=True, choices=['football', 'horse_racing', 'tennis'])
    # ... (other suggest args)
    sp.set_defaults(func=cmd_suggest)

    # ... (rest of the commands: news, insights, web, paper, live, telegram) ...
    sp = sub.add_parser("web", help="Run dashboard") 
    sp.set_defaults(func=cmd_web)
    
    sp = sub.add_parser("telegram", help="Run the Telegram bot")
    sp.set_defaults(func=cmd_telegram)


    args = p.parse_args(argv)
    return args.func(args)

if __name__ == "__main__":
    main()
