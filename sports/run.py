import os
import argparse
import subprocess
import json
from dotenv import load_dotenv
from sports.db import connect
from sports.schema import init_schema

# Core application components
from sports.webapp import create_app
from sports.telegram_bot import run_bot
from sports.providers.tote_api import ToteClient
from sports.providers.tote_bets import place_audit_superfecta, place_audit_win
from sports.providers.tote_bets import refresh_bet_status
from sports.providers.tote_bets import audit_list_bets, sync_bets_from_api
from sports.providers.tote_subscriptions import run_subscriber as run_pool_subscriber
from sports.ingest.tote_horse import ingest_horse_day
from sports.ingest.tote_events import ingest_tote_events
from sports.ingest.tote_products import ingest_superfecta_products
from sports.ingest.tote_products import ingest_products
from sports.ingest.tote_results import ingest_results_for_closed_products
from sports.ingest.weather import backfill_weather_for_date
from sports.ingest.hr_kaggle_results import (
    ingest_file as ingest_hr_kaggle_file,
    ingest_dir as ingest_hr_kaggle_dir,
    ingest_file_bq as ingest_hr_kaggle_file_bq,
    ingest_dir_bq as ingest_hr_kaggle_dir_bq,
)
from sports.extract import export_runner_dataset
from sports.features import build_features
from sports.weights import train_win_model, score_win_model
from sports.eval_sf import evaluate_superfecta
from sports.superfecta import recommend_superfecta_for_date
from sports.export_bq import export_sqlite_to_bq
from sports.worklog import sync_readme, post_slack_summary

def main(argv=None):
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        print("Error: DATABASE_URL not found in .env file.")
        return

    p = argparse.ArgumentParser(description="Sports Betting Bot")
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- Data Fetching Command ---
    sp_fetch = sub.add_parser("fetch-openfootball", help="Download/update openfootball datasets")
    sp_fetch.add_argument("--mode", required=True, choices=['init', 'update'])
    def _cmd_fetch_openfootball(args):
        script_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'fetch_openfootball.sh')
        subprocess.run(['bash', script_path, args.mode], check=True)
    sp_fetch.set_defaults(func=_cmd_fetch_openfootball)

    # --- Application Commands ---
    sp_web = sub.add_parser("web", help="Run the web dashboard")
    sp_web.add_argument("--port", type=int, default=int(os.getenv("PORT", "8010")))
    def _cmd_web(args):
        app = create_app()
        app.run(host="0.0.0.0", port=args.port)
    sp_web.set_defaults(func=_cmd_web)

    sp_telegram = sub.add_parser("telegram", help="Run the Telegram bot")
    def _cmd_telegram(args):
        run_bot()
    sp_telegram.set_defaults(func=_cmd_telegram)

    # --- Tote API: Horse Racing Ingest ---
    sp_tote_h = sub.add_parser("tote-horse", help="Ingest horse racing data from Tote API for a date or range")
    sp_tote_h.add_argument("--date", help="YYYY-MM-DD (defaults to today)")
    sp_tote_h.add_argument("--days", type=int, default=0, help="Also fetch N subsequent days")
    # Endpoint paths are configurable to avoid hardcoding. Provide format placeholders where needed.
    sp_tote_h.add_argument("--meetings-path", required=True, help="Relative path for meetings list, expects ?date=YYYY-MM-DD")
    sp_tote_h.add_argument("--races-for-meeting", required=True, help="Path template for races, e.g. /v1/meetings/{meeting_id}/races")
    sp_tote_h.add_argument("--runners-for-race", required=True, help="Path template for runners, e.g. /v1/races/{race_id}/runners")
    sp_tote_h.add_argument("--pools-for-race", required=True, help="Path template for pools, e.g. /v1/races/{race_id}/pools")
    def _cmd_tote_h(args):
        conn = connect(db_url); init_schema(conn)
        client = ToteClient()
        base_date = args.date or None
        total_races = 0
        for i in range(0, max(0, args.days) + 1):
            d = None
            if base_date:
                try:
                    from datetime import datetime, timedelta
                    d = (datetime.fromisoformat(base_date) + timedelta(days=i)).date().isoformat()
                except Exception:
                    d = None
            paths = {
                "meetings": args.meetings_path,
                "races_for_meeting": args.races_for_meeting,
                "runners_for_race": args.runners_for_race,
                "pools_for_race": args.pools_for_race,
            }
            count = ingest_horse_day(conn, client, date_iso=d, paths=paths)
            print(f"[Tote Horse] {d or 'today'}: ingested {count} race(s)")
            total_races += count
        conn.close()
        print(f"[Tote Horse] Total races ingested: {total_races}")
    sp_tote_h.set_defaults(func=_cmd_tote_h)

    # --- Tote API: Raw fetch helper ---
    sp_tote_raw = sub.add_parser("tote-raw", help="Fetch a Tote endpoint and archive JSON into raw_tote")
    sp_tote_raw.add_argument("--path", required=True, help="Relative endpoint path, e.g. /v1/meetings")
    sp_tote_raw.add_argument("--entity-id", help="Optional entity id to tag the raw record")
    sp_tote_raw.add_argument("--sport", default="horse_racing", help="Tag sport for the raw record")
    sp_tote_raw.add_argument("--param", action="append", default=[], help="Query param as key=value; repeatable")
    def _cmd_tote_raw(args):
        from urllib.parse import parse_qsl
        from sports.providers.tote_api import store_raw
        conn = connect(db_url); init_schema(conn)
        client = ToteClient()
        params = {}
        for p in args.param:
            if "=" in p:
                k, v = p.split("=", 1)
                params[k] = v
        data = client.get(args.path, params=params)
        rid = store_raw(conn, endpoint=args.path, entity_id=args.entity_id, sport=args.sport, payload=data)
        conn.commit(); conn.close()
        print(f"[Tote Raw] stored id={rid} size={len(str(data))} bytes")
    sp_tote_raw.set_defaults(func=_cmd_tote_raw)

    # --- Tote API: Events via GraphQL ---
    sp_tote_events = sub.add_parser("tote-events", help="Fetch Tote events (GraphQL) and store in tote_events")
    sp_tote_events.add_argument("--first", type=int, default=100, help="Number of events to fetch")
    sp_tote_events.add_argument("--since", help="Filter events since ISO8601 (e.g., 2025-09-01T00:00:00Z)")
    sp_tote_events.add_argument("--until", help="Filter events until ISO8601")
    sp_tote_events.add_argument("--export-bq", action="store_true", help="Also export events/products to BigQuery (uses current DB state)")
    def _cmd_tote_events(args):
        conn = connect(db_url); init_schema(conn)
        client = ToteClient()
        n = ingest_tote_events(conn, client, first=args.first, since_iso=args.since, until_iso=args.until)
        # Optional BQ export of latest events only
        if args.export_bq:
            try:
                res = export_sqlite_to_bq(conn, ["tote_events"])  # type: ignore
                print(f"[Tote Events] Exported to BQ: tote_events={res.get('tote_events',0)} rows")
            except Exception as e:
                print(f"[Tote Events] BQ export ERROR: {e}")
        conn.close()
        print(f"[Tote Events] Ingested {n} event(s)")
    sp_tote_events.set_defaults(func=_cmd_tote_events)

    # --- Tote API: GraphQL SDL probe ---
    sp_tote_sdl = sub.add_parser("tote-graphql-sdl", help="Fetch the GraphQL SDL to verify access")
    def _cmd_tote_sdl(args):
        client = ToteClient()
        sdl = client.graphql_sdl()
        print(sdl[:2000])
    sp_tote_sdl.set_defaults(func=_cmd_tote_sdl)

    # --- Tote API: SUPERFECTA products ---
    sp_tote_super = sub.add_parser("tote-superfecta", help="Fetch SUPERFECTA products and store in tote_products")
    sp_tote_super.add_argument("--date", help="ISO date for products (YYYY-MM-DD)")
    sp_tote_super.add_argument("--status", choices=["OPEN","CLOSED","UNKNOWN"], default="OPEN")
    sp_tote_super.add_argument("--first", type=int, default=200)
    def _cmd_tote_super(args):
        conn = connect(db_url); init_schema(conn)
        client = ToteClient()
        n = ingest_superfecta_products(conn, client, date_iso=args.date, status=args.status, first=args.first)
        conn.close()
        print(f"[Tote SUPERFECTA] Ingested {n} product(s)")
    sp_tote_super.set_defaults(func=_cmd_tote_super)

    # --- Tote API: Generic products (pool values) ---
    sp_tote_prods = sub.add_parser("tote-products", help="Fetch products across bet types and store pool values")
    sp_tote_prods.add_argument("--date", help="ISO date for products (YYYY-MM-DD)")
    sp_tote_prods.add_argument("--status", choices=["OPEN","CLOSED","UNKNOWN"], default="OPEN")
    sp_tote_prods.add_argument("--first", type=int, default=500)
    sp_tote_prods.add_argument("--types", help="Comma-separated bet types, e.g. WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA")
    sp_tote_prods.add_argument("--export-bq", action="store_true")
    def _cmd_tote_prods(args):
        bt = [s.strip().upper() for s in (args.types or '').split(',') if s.strip()]
        conn = connect(db_url); init_schema(conn)
        client = ToteClient()
        n = ingest_products(conn, client, date_iso=args.date, status=args.status, first=args.first, bet_types=(bt or None))
        if args.export_bq:
            try:
                res = export_sqlite_to_bq(conn, ["tote_products","tote_product_selections"])  # type: ignore
                print(f"[Tote Products] Exported to BQ: products={res.get('tote_products',0)} selections={res.get('tote_product_selections',0)}")
            except Exception as e:
                print(f"[Tote Products] BQ export ERROR: {e}")
        conn.close()
        print(f"[Tote Products] Ingested {n} product(s)")
    sp_tote_prods.set_defaults(func=_cmd_tote_prods)

    # --- Convenience: Today GB snapshot (events + products) ---
    sp_today = sub.add_parser("tote-today-gb", help="Fetch today’s GB horse events and products, export to BQ, create views")
    sp_today.add_argument("--first", type=int, default=500)
    sp_today.add_argument("--types", default="WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA")
    def _cmd_today(args):
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        day = now.strftime('%Y-%m-%d')
        since = f"{day}T00:00:00Z"; until = f"{day}T23:59:59Z"
        bt = [s.strip().upper() for s in (args.types or '').split(',') if s.strip()]
        conn = connect(db_url); init_schema(conn)
        client = ToteClient()
        print(f"[Today GB] Fetching events {since}..{until}")
        try:
            ingest_tote_events(conn, client, first=args.first, since_iso=since, until_iso=until)
        except Exception as e:
            print(f"[Today GB] Events ERROR: {e}")
        print(f"[Today GB] Fetching products for {day} {','.join(bt)}")
        try:
            ingest_products(conn, client, date_iso=day, status="OPEN", first=args.first, bet_types=bt)
        except Exception as e:
            print(f"[Today GB] Products ERROR: {e}")
        # Export all relevant tables and ensure views
        try:
            res = export_sqlite_to_bq(conn)
            print("[Today GB] Exported to BigQuery:")
            for k, v in res.items():
                print(f"  - {k}: {v} rows")
            print("[Today GB] Views refreshed (vw_today_gb_events, vw_today_gb_superfecta, etc.)")
        except Exception as e:
            print(f"[Today GB] Export ERROR: {e}")
        conn.close()
        print("[Today GB] Done")
    sp_today.set_defaults(func=_cmd_today)

    # --- Tote API: Results (horse placements) ---
    sp_tote_results = sub.add_parser("tote-results", help="Ingest horse finishing positions for CLOSED products on a date")
    sp_tote_results.add_argument("--date", help="ISO date (YYYY-MM-DD)")
    sp_tote_results.add_argument("--first", type=int, default=500)
    def _cmd_tote_results(args):
        conn = connect(db_url); init_schema(conn)
        client = ToteClient()
        n = ingest_results_for_closed_products(conn, client, date_iso=args.date, first=args.first)
        conn.close()
        print(f"[Tote Results] Recorded {n} horse run(s)")
    sp_tote_results.set_defaults(func=_cmd_tote_results)

    # --- Tote API: Weather backfill ---
    sp_weather = sub.add_parser("tote-weather", help="Backfill historical weather for events on a date")
    sp_weather.add_argument("--date", required=True, help="ISO date (YYYY-MM-DD)")
    sp_weather.add_argument("--limit", type=int, default=300)
    def _cmd_weather(args):
        conn = connect(db_url); init_schema(conn)
        n = backfill_weather_for_date(conn, date_iso=args.date, limit=args.limit)
        conn.close()
        print(f"[Weather] Updated {n} event(s) with weather")
    sp_weather.set_defaults(func=_cmd_weather)

    # --- Tote API: Backfill helper (products CLOSED + results + weather) ---
    sp_backfill = sub.add_parser("tote-backfill", help="Backfill a date range: CLOSED products, results, and weather")
    sp_backfill.add_argument("--from", dest="date_from", required=True, help="Start date YYYY-MM-DD (inclusive)")
    sp_backfill.add_argument("--to", dest="date_to", required=True, help="End date YYYY-MM-DD (inclusive)")
    sp_backfill.add_argument("--first", type=int, default=500, help="GraphQL page size per call")
    sp_backfill.add_argument("--types", default="WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA", help="Comma-separated bet types")
    def _cmd_backfill(args):
        from datetime import datetime, timedelta
        bt = [s.strip().upper() for s in (args.types or '').split(',') if s.strip()]
        d0 = datetime.fromisoformat(args.date_from).date()
        d1 = datetime.fromisoformat(args.date_to).date()
        if d1 < d0:
            raise SystemExit("--to must be >= --from")
        conn = connect(db_url); init_schema(conn)
        client = ToteClient()
        cur = d0
        total_prod = total_runs = total_wx = 0
        while cur <= d1:
            ds = cur.isoformat()
            print(f"[Backfill] {ds}: products CLOSED...")
            try:
                n1 = ingest_products(conn, client, date_iso=ds, status="CLOSED", first=args.first, bet_types=bt)
            except Exception as e:
                print(f"[Backfill] {ds}: products ERROR: {e}")
                n1 = 0
            print(f"[Backfill] {ds}: products={n1}")
            print(f"[Backfill] {ds}: results...")
            try:
                n2 = ingest_results_for_closed_products(conn, client, date_iso=ds, first=args.first)
            except Exception as e:
                print(f"[Backfill] {ds}: results ERROR: {e}")
                n2 = 0
            print(f"[Backfill] {ds}: runs={n2}")
            print(f"[Backfill] {ds}: weather...")
            try:
                n3 = backfill_weather_for_date(conn, date_iso=ds, limit=args.first)
            except Exception as e:
                print(f"[Backfill] {ds}: weather ERROR: {e}")
                n3 = 0
            print(f"[Backfill] {ds}: weather updated events={n3}")
            total_prod += n1; total_runs += n2; total_wx += n3
            cur += timedelta(days=1)
        conn.close()
        print(f"[Backfill] Done. products={total_prod} runs={total_runs} weather_updates={total_wx}")
    sp_backfill.set_defaults(func=_cmd_backfill)

    # --- Dataset export for training ---
    sp_export = sub.add_parser("extract-training", help="Export runner-level training dataset to CSV")
    sp_export.add_argument("--from", dest="date_from", required=True, help="Start date YYYY-MM-DD (inclusive)")
    sp_export.add_argument("--to", dest="date_to", required=True, help="End date YYYY-MM-DD (inclusive)")
    sp_export.add_argument("--bet-types", default="WIN", help="Comma-separated bet types (e.g., WIN,PLACE)")
    sp_export.add_argument("--out", default="data/exports/train_runs.csv", help="Output CSV path")
    def _cmd_export(args):
        conn = connect(db_url); init_schema(conn)
        bt = [s.strip().upper() for s in (args.bet_types or '').split(',') if s.strip()]
        n = export_runner_dataset(conn, date_from=args.date_from, date_to=args.date_to, bet_types=bt, out_path=args.out)
        conn.close()
        print(f"[Export] Wrote {n} rows to {args.out}")
    sp_export.set_defaults(func=_cmd_export)

    # (Football CSV ingest intentionally omitted for Tote-only pipeline)

    # --- Features build ---
    sp_feat = sub.add_parser("build-features", help="Build runner-level features for a date range")
    sp_feat.add_argument("--from", dest="date_from", required=True)
    sp_feat.add_argument("--to", dest="date_to", required=True)
    def _cmd_feat(args):
        conn = connect(db_url); init_schema(conn)
        n = build_features(conn, date_from=args.date_from, date_to=args.date_to)
        conn.close()
        print(f"[Features] Built {n} rows for {args.date_from}..{args.date_to}")
    sp_feat.set_defaults(func=_cmd_feat)

    # --- Train WIN model ---
    sp_train = sub.add_parser("train-weights", help="Train baseline WIN weighting model (logistic regression)")
    sp_train.add_argument("--from", dest="date_from", required=True)
    sp_train.add_argument("--to", dest="date_to", required=True)
    sp_train.add_argument("--out-dir", default="models")
    def _cmd_train(args):
        conn = connect(db_url); init_schema(conn)
        res = train_win_model(conn, date_from=args.date_from, date_to=args.date_to, out_dir=args.out_dir)
        conn.close()
        print(f"[Train] model={res['model_id']} metrics={res['metrics']} saved={res['path']}")
    sp_train.set_defaults(func=_cmd_train)

    # --- Score WIN model ---
    sp_score = sub.add_parser("score-weights", help="Score events with a trained WIN model and store predictions")
    sp_score.add_argument("--model", required=True, help="Path to joblib model file")
    sp_score.add_argument("--from", dest="date_from", required=True)
    sp_score.add_argument("--to", dest="date_to", required=True)
    def _cmd_score(args):
        conn = connect(db_url); init_schema(conn)
        n = score_win_model(conn, model_path=args.model, date_from=args.date_from, date_to=args.date_to)
        conn.close()
        print(f"[Score] Wrote {n} predictions for {args.date_from}..{args.date_to}")
    sp_score.set_defaults(func=_cmd_score)

    # --- Kaggle Horse Racing (historical results) ---
    sp_hrk_f = sub.add_parser("ingest-hr-kaggle-file", help="Ingest Kaggle horse racing results CSV (UK/Ireland)")
    sp_hrk_f.add_argument("--file", required=True)
    sp_hrk_f.add_argument("--bq", action="store_true", help="If set, ingest directly to BigQuery (requires config)")
    def _cmd_hrk_f(args):
        if args.bq:
            n = ingest_hr_kaggle_file_bq(args.file)
            print(f"[HR Kaggle>BQ] Ingested {n} rows from {args.file}")
        else:
            conn = connect(db_url); init_schema(conn)
            n = ingest_hr_kaggle_file(conn, args.file)
            conn.close()
            print(f"[HR Kaggle] Ingested {n} rows from {args.file}")
    sp_hrk_f.set_defaults(func=_cmd_hrk_f)

    sp_hrk_d = sub.add_parser("ingest-hr-kaggle-dir", help="Ingest all CSVs in a directory (recursively)")
    sp_hrk_d.add_argument("--dir", required=True)
    sp_hrk_d.add_argument("--bq", action="store_true", help="If set, ingest directly to BigQuery (requires config)")
    def _cmd_hrk_d(args):
        if args.bq:
            n = ingest_hr_kaggle_dir_bq(args.dir)
            print(f"[HR Kaggle>BQ] Ingested {n} rows from {args.dir}")
        else:
            conn = connect(db_url); init_schema(conn)
            n = ingest_hr_kaggle_dir(conn, args.dir)
            conn.close()
            print(f"[HR Kaggle] Ingested {n} rows from {args.dir}")
    sp_hrk_d.set_defaults(func=_cmd_hrk_d)

    # --- SUPERFECTA evaluation from predictions ---
    sp_sfe = sub.add_parser("eval-superfecta", help="Evaluate SUPERFECTA using stored WIN predictions")
    sp_sfe.add_argument("--model-id", required=True, help="Model id recorded in DB (not the path)")
    sp_sfe.add_argument("--ts", type=int, help="Prediction run timestamp (ms). Defaults to latest run for model")
    sp_sfe.add_argument("--from", dest="date_from", help="Filter products from YYYY-MM-DD")
    sp_sfe.add_argument("--to", dest="date_to", help="Filter products to YYYY-MM-DD")
    def _cmd_sfe(args):
        conn = connect(db_url); init_schema(conn)
        n = evaluate_superfecta(conn, model_id=args.model_id, ts_ms=args.ts, date_from=args.date_from, date_to=args.date_to)
        conn.close()
        print(f"[SF Eval] Evaluated {n} superfecta products for model {args.model_id}")
    sp_sfe.set_defaults(func=_cmd_sfe)

    # --- Superfecta recommendations (greedy PL from WIN predictions) ---
    sp_sfr = sub.add_parser("superfecta-recommend", help="Recommend SUPERFECTA selection(s) for a date using a model's predictions")
    sp_sfr.add_argument("--model-id", required=True)
    sp_sfr.add_argument("--ts", type=int, help="Prediction run timestamp (ms). If omitted, uses latest run")
    sp_sfr.add_argument("--date", required=True, help="YYYY-MM-DD")
    sp_sfr.add_argument("--top", type=int, default=5, help="Return top-N recommendations")
    def _cmd_sfr(args):
        conn = connect(db_url); init_schema(conn)
        recs = recommend_superfecta_for_date(conn, model_id=args.model_id, ts_ms=args.ts, date_iso=args.date, top_n=args.top)
        conn.close()
        if not recs:
            print("[SF Recommend] No recommendations (check products/predictions)")
            return
        print("[SF Recommend] Top selections:")
        for r in recs:
            print(f"  {r['date'] if 'date' in r else args.date} {r['start_iso']}: product={r['product_id']} event={r['event_id']} sel={r['selection']} prob={r['combo_prob']:.6f}")
    sp_sfr.set_defaults(func=_cmd_sfr)

    # --- Export SQLite -> BigQuery ---
    sp_bq = sub.add_parser("export-bq", help="Export selected tables from SQLite to BigQuery and create views")
    sp_bq.add_argument("--tables", help="Comma-separated list of tables; defaults to all supported")
    def _cmd_bq(args):
        conn = connect(db_url); init_schema(conn)
        tables = [t.strip() for t in (args.tables or "").split(",") if t.strip()] or None
        res = export_sqlite_to_bq(conn, tables)
        conn.close()
        for k, v in res.items():
            print(f"[BQ Export] {k}: {v} rows")
        if not res:
            print("[BQ Export] Nothing exported (check tables or configuration)")
    sp_bq.set_defaults(func=_cmd_bq)

    # --- BigQuery maintenance: cleanup temp tables ---
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

    # --- Daily Tote pipeline: ingest products/results/weather and optionally export to BQ ---
    sp_pipe = sub.add_parser("tote-pipeline", help="Run daily pipeline for a date: products (OPEN+CLOSED), results, weather, optional BQ export")
    sp_pipe.add_argument("--date", required=True, help="ISO date YYYY-MM-DD")
    sp_pipe.add_argument("--first", type=int, default=500, help="GraphQL page size per call")
    sp_pipe.add_argument("--types", default="WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA", help="Comma-separated bet types for products fetch")
    sp_pipe.add_argument("--export-bq", action="store_true", help="If set, export SQLite tables to BigQuery at the end")
    def _cmd_pipeline(args):
        bt = [s.strip().upper() for s in (args.types or '').split(',') if s.strip()]
        conn = connect(db_url); init_schema(conn)
        client = ToteClient()
        ds = args.date
        print(f"[Pipeline] {ds}: products OPEN...")
        try:
            n_open = ingest_products(conn, client, date_iso=ds, status="OPEN", first=args.first, bet_types=bt)
        except Exception as e:
            print(f"[Pipeline] {ds}: products OPEN ERROR: {e}")
            n_open = 0
        print(f"[Pipeline] {ds}: products CLOSED...")
        try:
            n_closed = ingest_products(conn, client, date_iso=ds, status="CLOSED", first=args.first, bet_types=bt)
        except Exception as e:
            print(f"[Pipeline] {ds}: products CLOSED ERROR: {e}")
            n_closed = 0
        print(f"[Pipeline] {ds}: results...")
        try:
            n_runs = ingest_results_for_closed_products(conn, client, date_iso=ds, first=args.first)
        except Exception as e:
            print(f"[Pipeline] {ds}: results ERROR: {e}")
            n_runs = 0
        print(f"[Pipeline] {ds}: weather...")
        try:
            n_wx = backfill_weather_for_date(conn, date_iso=ds, limit=args.first)
        except Exception as e:
            print(f"[Pipeline] {ds}: weather ERROR: {e}")
            n_wx = 0
        print(f"[Pipeline] {ds}: OPEN={n_open} CLOSED={n_closed} runs={n_runs} weather_updates={n_wx}")
        # Optional export to BigQuery
        if args.export_bq:
            try:
                res = export_sqlite_to_bq(conn)
                print("[Pipeline] Exported to BigQuery:")
                for k, v in res.items():
                    print(f"  - {k}: {v} rows")
            except Exception as e:
                print(f"[Pipeline] BQ export ERROR: {e}")
        conn.close()
    sp_pipe.set_defaults(func=_cmd_pipeline)

    # --- Tote: Audit Bets ---
    sp_audit_sf = sub.add_parser("tote-audit-superfecta", help="Place an audit-mode SUPERFECTA bet (no live placement unless configured)")
    sp_audit_sf.add_argument("--product", required=True)
    sp_audit_sf.add_argument("--sel", required=True, help="Selection string like 3-7-1-5")
    sp_audit_sf.add_argument("--stake", required=True, type=float)
    sp_audit_sf.add_argument("--currency", default="GBP")
    sp_audit_sf.add_argument("--post", action="store_true", help="If set, attempt to POST to Tote API in audit mode")
    def _cmd_audit_sf(args):
        conn = connect(db_url); init_schema(conn)
        res = place_audit_superfecta(conn, product_id=args.product, selection=args.sel, stake=args.stake, currency=args.currency, post=args.post)
        conn.commit(); conn.close()
        print("[Audit SF]", res)
    sp_audit_sf.set_defaults(func=_cmd_audit_sf)

    sp_audit_win = sub.add_parser("tote-audit-win", help="Place an audit-mode WIN bet (no live placement unless configured)")
    sp_audit_win.add_argument("--event", required=True)
    sp_audit_win.add_argument("--selection", required=True, help="Selection id for the runner")
    sp_audit_win.add_argument("--stake", required=True, type=float)
    sp_audit_win.add_argument("--currency", default="GBP")
    sp_audit_win.add_argument("--post", action="store_true")
    def _cmd_audit_win(args):
        conn = connect(db_url); init_schema(conn)
        res = place_audit_win(conn, event_id=args.event, selection_id=args.selection, stake=args.stake, currency=args.currency, post=args.post)
        conn.commit(); conn.close()
        print("[Audit WIN]", res)
    sp_audit_win.set_defaults(func=_cmd_audit_win)

    sp_bet_status = sub.add_parser("tote-bet-status", help="Refresh Tote bet status (audit/live) by bet_id")
    sp_bet_status.add_argument("--bet-id", required=True)
    sp_bet_status.add_argument("--post", action="store_true", help="If set, attempt to call Tote API for status")
    def _cmd_bet_status(args):
        conn = connect(db_url); init_schema(conn)
        res = refresh_bet_status(conn, bet_id=args.bet_id, post=args.post)
        conn.commit(); conn.close()
        print("[Bet Status]", res)
    sp_bet_status.set_defaults(func=_cmd_bet_status)

    # --- Tote subscriptions: onPoolTotalChanged listener ---
    sp_sub = sub.add_parser("tote-subscribe-pools", help="Subscribe to onPoolTotalChanged over WebSocket and persist snapshots")
    sp_sub.add_argument("--duration", type=int, help="Run for N seconds then exit")
    def _cmd_sub(args):
        conn = connect(db_url); init_schema(conn)
        try:
            run_pool_subscriber(conn, duration=args.duration)
        finally:
            conn.close()
    sp_sub.set_defaults(func=_cmd_sub)

    # --- Tote audit: list bets and optional sync ---
    sp_list = sub.add_parser("tote-audit-bets", help="List audit bets from GraphQL and optionally sync outcomes to tote_bets")
    sp_list.add_argument("--since", help="ISO8601 since")
    sp_list.add_argument("--until", help="ISO8601 until")
    sp_list.add_argument("--first", type=int, default=20)
    sp_list.add_argument("--sync", action="store_true", help="If set, update tote_bets outcomes by matching toteId")
    def _cmd_list(args):
        data = audit_list_bets(since_iso=args.since, until_iso=args.until, first=args.first)
        print(json.dumps(data, indent=2)[:2000])
        if args.sync:
            conn = connect(db_url); init_schema(conn)
            n = sync_bets_from_api(conn, data)
            conn.close()
            print(f"[Audit Bets] Synced {n} bet(s)")
    sp_list.set_defaults(func=_cmd_list)

    # --- Worklog utilities ---
    sp_wl = sub.add_parser("worklog-sync", help="Mirror docs/WORKLOG.md into README (and optionally post to Slack)")
    sp_wl.add_argument("--entries", type=int, default=1, help="How many latest entries to include in README/Slack")
    sp_wl.add_argument("--no-readme", action="store_true", help="Skip README update")
    sp_wl.add_argument("--slack", action="store_true", help="Post summary to Slack via SLACK_WEBHOOK_URL")
    def _cmd_wl(args):
        changed = False
        if not args.no_readme:
            readme_path = os.path.join(os.path.dirname(__file__), '..', 'README.md')
            readme_path = os.path.abspath(readme_path)
            try:
                changed = sync_readme(readme_path, n_entries=max(1, args.entries))
                print(f"[Worklog] README updated: {changed}")
            except Exception as e:
                print(f"[Worklog] README update error: {e}")
        if args.slack:
            ok, msg = post_slack_summary(n_entries=max(1, args.entries))
            print(f"[Worklog] Slack post: {'ok' if ok else 'fail'} ({msg})")
    sp_wl.set_defaults(func=_cmd_wl)

    sp_bq_schema = sub.add_parser("bq-schema-export", help="Export BigQuery schema to a CSV file")
    def _cmd_bq_schema_export(args):
        from sports.bq_schema import export_bq_schema_to_csv
        export_bq_schema_to_csv()
    sp_bq_schema.set_defaults(func=_cmd_bq_schema_export)

    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
