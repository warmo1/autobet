import os
import argparse
import subprocess
import json
from dotenv import load_dotenv # Keep this
from .db import get_db, init_db

# Core application components
from .webapp import create_app
from .telegram_bot import run_bot
from .providers.tote_api import ToteClient
from .providers.tote_bets import place_audit_superfecta, place_audit_win
from .providers.tote_bets import refresh_bet_status
from .providers.tote_bets import audit_list_bets, sync_bets_from_api
from .providers.tote_subscriptions import run_subscriber as run_pool_subscriber
from .ingest.tote_events import ingest_tote_events
from .ingest.tote_products import ingest_products

def main(argv=None):
    """Main entry point for the command-line runner."""
    load_dotenv()

    p = argparse.ArgumentParser(description="Sports Betting Bot")
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- DB initialization ---
    # Initializes the BigQuery database by creating tables and views.
    sp_init_db = sub.add_parser("init-db", help="Initialize the BigQuery database (create tables and views)")
    sp_init_db.add_argument("--force", action="store_true", help="Run without confirmation prompt.")
    def _cmd_init_db(args):
        if not args.force:
            confirm = input("This will run many CREATE TABLE/VIEW statements and should only be run once for setup. Continue? (y/n): ")
            if confirm.lower() != 'y':
                print("Initialization cancelled.")
                return
        init_db()
    sp_init_db.set_defaults(func=_cmd_init_db)

    # --- Application Commands ---
    # Runs the web dashboard.
    sp_web = sub.add_parser("web", help="Run the web dashboard")
    sp_web.add_argument("--port", type=int, default=int(os.getenv("PORT", "8010")))
    def _cmd_web(args):
        app = create_app()
        app.run(host="0.0.0.0", port=args.port)
    sp_web.set_defaults(func=_cmd_web)

    # Runs the Telegram bot.
    sp_telegram = sub.add_parser("telegram", help="Run the Telegram bot")
    def _cmd_telegram(args):
        run_bot()
    sp_telegram.set_defaults(func=_cmd_telegram)

    # --- Tote API: Events via GraphQL ---
    # Fetches Tote events (GraphQL) and store directly in BigQuery.
    sp_tote_events = sub.add_parser("tote-events", help="Fetch Tote events (GraphQL) and store directly in BigQuery.")
    sp_tote_events.add_argument("--first", type=int, default=100, help="Number of events to fetch")
    sp_tote_events.add_argument("--since", help="Filter events since ISO8601 (e.g., 2025-09-01T00:00:00Z)")
    sp_tote_events.add_argument("--until", help="Filter events until ISO8601")
    def _cmd_tote_events(args):
        db = get_db()
        client = ToteClient()
        print(f"Ingesting events since='{args.since}' until='{args.until}' directly to BigQuery...")
        n = ingest_tote_events(db, client, first=args.first, since_iso=args.since, until_iso=args.until)
        print(f"[Tote Events] Ingested {n} event(s) into BigQuery.")
    sp_tote_events.set_defaults(func=_cmd_tote_events)

    # --- Tote API: Events via GraphQL (date range) ---
    # Fetches Tote events (GraphQL) for a date range directly into BigQuery
    sp_tote_events_range = sub.add_parser("tote-events-range", help="Fetch Tote events (GraphQL) for a date range directly into BigQuery")
    sp_tote_events_range.add_argument("--from", dest="date_from", required=True, help="Start date YYYY-MM-DD (inclusive)")
    sp_tote_events_range.add_argument("--to", dest="date_to", required=True, help="End date YYYY-MM-DD (inclusive)")
    sp_tote_events_range.add_argument("--first", type=int, default=500, help="Number of events to fetch per day")
    def _cmd_tote_events_range(args):
        from datetime import datetime, timedelta
        try:
            from tqdm import tqdm
        except ImportError:
            def tqdm(iterable, **kwargs):
                return iterable # Fallback if tqdm is not installed

        d0 = datetime.fromisoformat(args.date_from).date()
        d1 = datetime.fromisoformat(args.date_to).date()
        if d1 < d0:
            raise SystemExit("--to must be >= --from")
        
        db = get_db()
        client = ToteClient()
        cur = d0
        total_events = 0
        
        day_count = (d1 - d0).days + 1
        progress_bar = tqdm(range(day_count), desc="Ingesting Events")
        for _ in progress_bar:
            ds = cur.isoformat()
            since_iso = f"{ds}T00:00:00Z"
            until_iso = f"{ds}T23:59:59Z"
            progress_bar.set_description(f"Ingesting Events for {ds}")
            try:
                n = ingest_tote_events(db, client, first=args.first, since_iso=since_iso, until_iso=until_iso)
                total_events += n
            except Exception as e:
                print(f"\n[Tote Events Range] ERROR for date {ds}: {e}")
            cur += timedelta(days=1)
        print(f"[Tote Events Range] Total events ingested: {total_events}")
    sp_tote_events_range.set_defaults(func=_cmd_tote_events_range)

    # --- Tote API: GraphQL SDL probe ---
    # Fetches the GraphQL SDL to verify access
    sp_tote_sdl = sub.add_parser("tote-graphql-sdl", help="Fetch the GraphQL SDL to verify access")
    def _cmd_tote_sdl(args):
        client = ToteClient()
        sdl = client.graphql_sdl()
        print(sdl[:2000])
    sp_tote_sdl.set_defaults(func=_cmd_tote_sdl)

    # --- Tote API: SUPERFECTA products ---
    # Fetches SUPERFECTA products and store directly in BigQuery.
    sp_tote_super = sub.add_parser("tote-superfecta", help="Fetch SUPERFECTA products and store directly in BigQuery.")
    sp_tote_super.add_argument("--date", help="ISO date for products (YYYY-MM-DD)")
    sp_tote_super.add_argument("--status", choices=["OPEN","CLOSED","UNKNOWN"], default="OPEN")
    sp_tote_super.add_argument("--first", type=int, default=200)
    def _cmd_tote_super(args):
        db = get_db() # Use the BigQuery sink
        client = ToteClient()
        print(f"Ingesting SUPERFECTA products for date='{args.date or 'today'}' status='{args.status}' directly to BigQuery...")
        n = ingest_products(db, client, date_iso=args.date, status=args.status, first=args.first, bet_types=["SUPERFECTA"])
        print(f"[Tote SUPERFECTA] Ingested and upserted {n} product(s) to BigQuery.")
    sp_tote_super.set_defaults(func=_cmd_tote_super)

    # --- Tote API: Generic products (pool values + probable odds raw) ---
    # Fetch products across bet types and store directly in BigQuery (includes raw probable odds payloads)
    sp_tote_prods = sub.add_parser("tote-products", help="Fetch products across bet types to BigQuery (also stores raw probable odds)")
    sp_tote_prods.add_argument("--date", help="ISO date for products (YYYY-MM-DD)")
    sp_tote_prods.add_argument("--status", choices=["OPEN","CLOSED","UNKNOWN"], default="OPEN")
    sp_tote_prods.add_argument("--first", type=int, default=500) # Default to include Jackpot and Pacepot
    sp_tote_prods.add_argument("--types", default="WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA,JACKPOT", help="Comma-separated bet types, e.g. WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA,JACKPOT")
    sp_tote_prods.add_argument("--products", default=None, help="Comma-separated product IDs to fetch by id (skips paging)")
    def _cmd_tote_prods(args):
        bt = [s.strip().upper() for s in (args.types or '').split(',') if s.strip()]
        prod_ids = [s.strip() for s in (args.products or '').split(',') if s.strip()] if args.products else None
        db = get_db()
        client = ToteClient()
        print(f"Ingesting products for date='{args.date or 'today'}' status='{args.status}' types='{args.types}' directly to BigQuery...")
        n = ingest_products(db, client, date_iso=args.date, status=args.status, first=args.first, bet_types=(bt or None), product_ids=prod_ids)
        print(f"[Tote Products] Ingested {n} product(s) into BigQuery")
    sp_tote_prods.set_defaults(func=_cmd_tote_prods)

    # --- Tote API: Results (horse placements) ---
    # Ingests horse finishing positions for a date directly to BigQuery.
    sp_tote_results = sub.add_parser("tote-results", help="Ingest horse finishing positions for a date directly to BigQuery.")
    sp_tote_results.add_argument("--date", help="ISO date (YYYY-MM-DD)")
    sp_tote_results.add_argument("--first", type=int, default=500)
    def _cmd_tote_results(args):
        db = get_db()
        client = ToteClient()
        print(f"Ingesting results for date='{args.date or 'today'}' by fetching CLOSED products directly to BigQuery...")
        n = ingest_products(db, client, date_iso=args.date, status="CLOSED", first=args.first, bet_types=None)
        print(f"[Tote Results] Scanned {n} product(s). Check logs for finishing positions.")
    sp_tote_results.set_defaults(func=_cmd_tote_results)

    # --- Tote API: Backfill helper (products CLOSED + results + weather) ---
    # Backfills a date range directly to BigQuery: CLOSED products, results, and weather.
    sp_backfill = sub.add_parser("tote-backfill", help="Backfill a date range directly to BigQuery: CLOSED products, results, and weather.")
    sp_backfill.add_argument("--from", dest="date_from", required=True, help="Start date YYYY-MM-DD (inclusive)")
    sp_backfill.add_argument("--to", dest="date_to", required=True, help="End date YYYY-MM-DD (inclusive)")
    sp_backfill.add_argument("--first", type=int, default=500, help="GraphQL page size per call") # Default to include Jackpot and Pacepot
    sp_backfill.add_argument("--types", default="WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA,JACKPOT", help="Comma-separated bet types")
    def _cmd_backfill(args):
        from datetime import datetime, timedelta
        try:
            from tqdm import tqdm
        except ImportError:
            def tqdm(iterable, **kwargs):
                return iterable

        bt = [s.strip().upper() for s in (args.types or '').split(',') if s.strip()]
        d0 = datetime.fromisoformat(args.date_from).date()
        d1 = datetime.fromisoformat(args.date_to).date()
        if d1 < d0:
            raise SystemExit("--to must be >= --from")
        
        db = get_db()
        client = ToteClient()
        cur = d0
        total_prod = 0
        
        day_count = (d1 - d0).days + 1
        progress_bar = tqdm(range(day_count), desc="Backfilling Data")
        for _ in progress_bar:
            ds = cur.isoformat()
            progress_bar.set_description(f"Backfilling {ds}")
            try:
                n1 = ingest_products(db, client, date_iso=ds, status="CLOSED", first=args.first, bet_types=bt)
            except Exception as e:
                print(f"\n[Backfill] {ds}: products/results ERROR: {e}")
                n1 = 0
            total_prod += n1
            cur += timedelta(days=1)
        print(f"[Backfill] Done. products_scanned={total_prod}")
    sp_backfill.set_defaults(func=_cmd_backfill)

    # --- BigQuery maintenance: cleanup temp tables ---
    # Deletes leftover _tmp tables in the configured BigQuery dataset
    sp_bq_clean = sub.add_parser("bq-cleanup", help="Delete leftover _tmp tables in the configured BigQuery dataset")
    sp_bq_clean.add_argument("--older", type=int, default=None, help="Only delete temp tables older than N days")
    def _cmd_bq_clean(args):
        from sports.bq import get_bq_sink
        sink = get_bq_sink()
        if not sink:
            print("[BQ Cleanup] BigQuery sink not enabled/configured")
            return
        try:
            n = sink.cleanup_temp_tables(older_than_days=args.older)
            print(f"[BQ Cleanup] Deleted {n} temp table(s)")
        except Exception as e:
            print(f"[BQ Cleanup] ERROR: {e}")
    sp_bq_clean.set_defaults(func=_cmd_bq_clean)

    # --- Daily Tote pipeline: ingest products/results/weather directly to BigQuery ---
    # Runs daily pipeline for a date directly to BigQuery: products (OPEN+CLOSED), results, weather.
    sp_pipe = sub.add_parser("tote-pipeline", help="Run daily pipeline for a date directly to BigQuery: products (OPEN+CLOSED), results, weather.")
    sp_pipe.add_argument("--date", required=True, help="ISO date YYYY-MM-DD")
    sp_pipe.add_argument("--first", type=int, default=500, help="GraphQL page size per call")
    sp_pipe.add_argument("--types", default="WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA,JACKPOT", help="Comma-separated bet types for products fetch")
    def _cmd_pipeline(args):
        bt = [s.strip().upper() for s in (args.types or '').split(',') if s.strip()]
        db = get_db()
        client = ToteClient()
        ds = args.date
        print(f"[Pipeline] {ds}: products OPEN...")
        try:
            n_open = ingest_products(db, client, date_iso=ds, status="OPEN", first=args.first, bet_types=bt)
        except Exception as e:
            print(f"[Pipeline] {ds}: products OPEN ERROR: {e}")
            n_open = 0
        print(f"[Pipeline] {ds}: products CLOSED...")
        try:
            n_closed = ingest_products(db, client, date_iso=ds, status="CLOSED", first=args.first, bet_types=bt)
        except Exception as e:
            print(f"[Pipeline] {ds}: products CLOSED/results ERROR: {e}")
            n_closed = 0
        print(f"[Pipeline] {ds}: OPEN={n_open} CLOSED={n_closed}")
    sp_pipe.set_defaults(func=_cmd_pipeline)

    

    # --- Tote: Audit Bets ---
    # Places an audit-mode SUPERFECTA bet (no live placement unless configured)
    sp_audit_sf = sub.add_parser("tote-audit-superfecta", help="Place an audit-mode SUPERFECTA bet (no live placement unless configured)")
    sp_audit_sf.add_argument("--product", required=True)
    sp_audit_sf.add_argument("--sel", required=True, help="Selection string like 3-7-1-5")
    sp_audit_sf.add_argument("--stake", required=True, type=float)
    sp_audit_sf.add_argument("--currency", default="GBP")
    sp_audit_sf.add_argument("--post", action="store_true", help="If set, attempt to POST to Tote API in audit mode")
    def _cmd_audit_sf(args):
        db = get_db()
        res = place_audit_superfecta(db, product_id=args.product, selection=args.sel, stake=args.stake, currency=args.currency, post=args.post, mode='audit')
        print("[Audit SF]", res)
    sp_audit_sf.set_defaults(func=_cmd_audit_sf)

    # Places an audit-mode WIN bet (no live placement unless configured)
    sp_audit_win = sub.add_parser("tote-audit-win", help="Place an audit-mode WIN bet (no live placement unless configured)")
    sp_audit_win.add_argument("--event", required=True)
    sp_audit_win.add_argument("--selection", required=True, help="Selection id for the runner")
    sp_audit_win.add_argument("--stake", required=True, type=float)
    sp_audit_win.add_argument("--currency", default="GBP")
    sp_audit_win.add_argument("--post", action="store_true")
    def _cmd_audit_win(args):
        db = get_db()
        res = place_audit_win(db, event_id=args.event, selection_id=args.selection, stake=args.stake, currency=args.currency, post=args.post, mode='audit')
        print("[Audit WIN]", res)
    sp_audit_win.set_defaults(func=_cmd_audit_win)

    # Refreshes Tote bet status (audit/live) by bet_id
    sp_bet_status = sub.add_parser("tote-bet-status", help="Refresh Tote bet status (audit/live) by bet_id")
    sp_bet_status.add_argument("--bet-id", required=True)
    sp_bet_status.add_argument("--post", action="store_true", help="If set, attempt to call Tote API for status")
    def _cmd_bet_status(args):
        db = get_db()
        res = refresh_bet_status(db, bet_id=args.bet_id, post=args.post)
        print("[Bet Status]", res)
    sp_bet_status.set_defaults(func=_cmd_bet_status)

    # --- Tote subscriptions: onPoolTotalChanged listener ---
    # Subscribes to onPoolTotalChanged and persist snapshots to BigQuery.
    sp_sub = sub.add_parser("tote-subscribe-pools", help="Subscribe to onPoolTotalChanged and persist snapshots to BigQuery.")
    sp_sub.add_argument("--duration", type=int, help="Run for N seconds then exit")
    def _cmd_sub(args):
        db = get_db()
        run_pool_subscriber(db, duration=args.duration)
    sp_sub.set_defaults(func=_cmd_sub)

    # --- Tote audit: list bets and optional sync ---
    # Lists audit bets from GraphQL and optionally sync outcomes to BigQuery.
    sp_list = sub.add_parser("tote-audit-bets", help="List audit bets from GraphQL and optionally sync outcomes to BigQuery.")
    sp_list.add_argument("--since", help="ISO8601 since")
    sp_list.add_argument("--until", help="ISO8601 until")
    sp_list.add_argument("--first", type=int, default=20)
    sp_list.add_argument("--sync", action="store_true", help="If set, update tote_bets outcomes by matching toteId")
    def _cmd_list(args):
        data = audit_list_bets(since_iso=args.since, until_iso=args.until, first=args.first)
        print(json.dumps(data, indent=2)[:2000])
        if args.sync:
            db = get_db()
            n = sync_bets_from_api(db, data)
            print(f"[Audit Bets] Synced {n} bet(s)")
    sp_list.set_defaults(func=_cmd_list)

    
    # This command does not interact with the database and is preserved.
    # Exports BigQuery schema to a CSV file
    sp_bq_schema = sub.add_parser("bq-schema-export", help="Export BigQuery schema to a CSV file")
    def _cmd_bq_schema_export(args):
        from sports.bq_schema import export_bq_schema_to_csv
        export_bq_schema_to_csv()
    sp_bq_schema.set_defaults(func=_cmd_bq_schema_export)

    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
