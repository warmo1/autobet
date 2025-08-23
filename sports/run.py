import os
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Core DB & schema
try:
    from sports.db import connect
    from sports.schema import init_schema
except Exception as e:
    raise SystemExit(f"Failed to import core DB/schema: {e}")

# Optional/new ingestors (loaded eagerly; if missing, commands are hidden)
_opt_import_errors = {}

def _try_import(name, alias=None):
    try:
        module = __import__(name, fromlist=['*'])
        return module
    except Exception as e:
        _opt_import_errors[alias or name] = str(e)
        return None

# Newer fixture sources
_mod_bbc_html = _try_import('sports.ingest.bbc_html', alias='bbc_html')
_mod_bbc_fx = _try_import('sports.ingest.bbc_fixtures', alias='bbc_fixtures')
_mod_fpl = _try_import('sports.ingest.fpl_fixtures', alias='fpl_fixtures')
_mod_espn = _try_import('sports.ingest.fixtures_api', alias='fixtures_api')

# Existing CSV ingestors (best effort)
_mod_fd = _try_import('sports.ingest.football_fd', alias='football_fd')
_mod_cric = _try_import('sports.ingest.cricket_cricsheet', alias='cricket_cricsheet')
_mod_kaggle = _try_import('sports.ingest.football_kaggle', alias='football_kaggle')
_mod_pl = _try_import('sports.ingest.football_premier_league', alias='football_premier_league')

# Optional runtime pieces
_mod_tg = _try_import('sports.telegram_bot', alias='telegram_bot')
_mod_web = _try_import('sports.webapp', alias='webapp')

# Legacy/older commands (preserve if present)
_mod_legacy_ingest_fx = _try_import('sports.ingest.fixtures', alias='legacy_fixtures')
_mod_legacy_openfootball = _try_import('sports.ingest.openfootball', alias='openfootball')
_mod_legacy_suggestions = _try_import('sports.suggest', alias='suggest')
_mod_legacy_live_betdaq = _try_import('sports.betdaq_live', alias='betdaq_live')


def main(argv=None):
    load_dotenv()

    parser = argparse.ArgumentParser(description="Sports CLI / Ingestion & Runtime")
    default_db = os.getenv("DATABASE_URL", "sqlite:///sports_bot.db")
    parser.add_argument("--db", dest="db", default=None, help=f"Database URL (default: {default_db})")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # ---- Core: initdb ----
    sp_init = sub.add_parser("initdb", help="Create/upgrade database schema")
    def _cmd_initdb(args):
        db_url = args.db or default_db
        conn = connect(db_url)
        init_schema(conn)
        conn.close()
        print("[DB] Schema initialised.")
    sp_init.set_defaults(func=_cmd_initdb)

    # ---- CSV/Data ingestors (if present) ----
    if _mod_fd and hasattr(_mod_fd, 'ingest_dir'):
        sp_fd = sub.add_parser("ingest-football-csv", help="Ingest football-data.co.uk CSVs")
        sp_fd.add_argument("--dir", required=True, help="Directory for *.csv files")
        def _cmd_fd(args):
            db_url = args.db or default_db
            conn = connect(db_url); init_schema(conn)
            n = _mod_fd.ingest_dir(conn, args.dir)
            print(f"[Football FD] Ingested {n} rows.")
            conn.close()
        sp_fd.set_defaults(func=_cmd_fd)

    if _mod_kaggle and hasattr(_mod_kaggle, 'ingest_file'):
        sp_kg = sub.add_parser("ingest-football-kaggle", help="Ingest Kaggle domestic football CSV")
        sp_kg.add_argument("--file", required=True, help="Path to main.csv")
        def _cmd_kg(args):
            db_url = args.db or default_db
            conn = connect(db_url); init_schema(conn)
            n = _mod_kaggle.ingest_file(conn, args.file)
            print(f"[Kaggle Football] Ingested {n} rows.")
            conn.close()
        sp_kg.set_defaults(func=_cmd_kg)

    if _mod_pl and hasattr(_mod_pl, 'ingest_file'):
        sp_pl = sub.add_parser("ingest-pl-stats", help="Ingest Kaggle Premier League matches.csv")
        sp_pl.add_argument("--file", required=True, help="Path to matches.csv")
        def _cmd_pl(args):
            db_url = args.db or default_db
            conn = connect(db_url); init_schema(conn)
            n = _mod_pl.ingest_file(conn, args.file)
            print(f"[Premier League] Ingested {n} rows.")
            conn.close()
        sp_pl.set_defaults(func=_cmd_pl)

    if _mod_cric and hasattr(_mod_cric, 'ingest_dir'):
        sp_cric = sub.add_parser("ingest-cricket-csv", help="Ingest Cricsheet CSVs")
        sp_cric.add_argument("--dir", required=True, help="Directory for *.csv files")
        def _cmd_cric(args):
            db_url = args.db or default_db
            conn = connect(db_url); init_schema(conn)
            n = _mod_cric.ingest_dir(conn, args.dir)
            print(f"[Cricket] Ingested {n} rows.")
            conn.close()
        sp_cric.set_defaults(func=_cmd_cric)

    # ---- New: BBC fixtures (HTML) ----
    # Support either `bbc_html.ingest_bbc_range` or `bbc_fixtures.ingest_bbc_fixtures`
    if (
        (_mod_bbc_html and hasattr(_mod_bbc_html, 'ingest_bbc_range')) or
        (_mod_bbc_fx and hasattr(_mod_bbc_fx, 'ingest_bbc_fixtures'))
    ):
        sp_bbc = sub.add_parser("ingest-bbc-fixtures", help="Scrape BBC football fixtures for a date or range")
        sp_bbc.add_argument("--date", help="Start date ISO (YYYY-MM-DD); default=today")
        sp_bbc.add_argument("--days", type=int, default=1, help="Number of days to fetch (default 1)")
        sp_bbc.add_argument("--debug", action="store_true", help="Verbose diagnostics (parsing counts, selectors)")
        def _cmd_bbc(args):
            if args.debug:
                os.environ["FIXTURE_DEBUG"] = "1"
            db_url = args.db or default_db
            start = args.date or datetime.now().date().isoformat()
            conn = connect(db_url); init_schema(conn)
            if _mod_bbc_html and hasattr(_mod_bbc_html, 'ingest_bbc_range'):
                n = _mod_bbc_html.ingest_bbc_range(conn, start_date_iso=start, days=args.days)
            else:
                # Fallback: call single-date ingestor repeatedly for range
                total = 0
                from datetime import timedelta, datetime as _dt
                d0 = _dt.fromisoformat(start).date()
                for i in range(args.days):
                    ds = (d0 + timedelta(days=i)).isoformat()
                    total += _mod_bbc_fx.ingest_bbc_fixtures(conn, 'football', date_iso=ds)
                n = total
            print(f"[BBC Ingest] Upserted {n} fixtures from {start} (+{args.days-1}d).")
            conn.close()
        sp_bbc.set_defaults(func=_cmd_bbc)

    # ---- New: FPL fixtures (EPL) ----
    if _mod_fpl and hasattr(_mod_fpl, 'ingest'):
        sp_fpl = sub.add_parser("fetch-fpl-fixtures", help="Fetch EPL fixtures from the FPL API")
        sp_fpl.add_argument("--all", action="store_true", help="Include past fixtures as well (default: future only)")
        sp_fpl.add_argument("--debug", action="store_true", help="Verbose diagnostics (totals and sample items)")
        def _cmd_fpl(args):
            if args.debug:
                os.environ["FIXTURE_DEBUG"] = "1"
            db_url = args.db or default_db
            conn = connect(db_url); init_schema(conn)
            n = _mod_fpl.ingest(conn, future_only=(not args.all))
            print(f"[FPL] Upserted {n} fixtures.")
            conn.close()
        sp_fpl.set_defaults(func=_cmd_fpl)

    # ---- New: ESPN fixtures (EPL/Championship) ----
    if _mod_espn and hasattr(_mod_espn, 'ingest_fixtures'):
        sp_espn = sub.add_parser("fetch-espn", help="Fetch ESPN fixtures (EPL/Championship)")
        sp_espn.add_argument("--league", choices=["football", "football_championship"], default="football")
        sp_espn.add_argument("--date", help="YYYY-MM-DD (default: today)")
        sp_espn.add_argument("--debug", action="store_true", help="Verbose diagnostics (URL tried, events count)")
        def _cmd_espn(args):
            if args.debug:
                os.environ["FIXTURE_DEBUG"] = "1"
            db_url = args.db or default_db
            conn = connect(db_url); init_schema(conn)
            n = _mod_espn.ingest_fixtures(conn, args.league, date_iso=args.date)
            print(f"[ESPN] Upserted {n} fixtures for {args.league}.")
            conn.close()
        sp_espn.set_defaults(func=_cmd_espn)

    # ---- Telegram bot (if module present) ----
    if _mod_tg and hasattr(_mod_tg, 'run_bot'):
        sp_bot = sub.add_parser("telegram-bot", help="Run the Telegram bot")
        sp_bot.add_argument("--token", required=False, help="Telegram Bot Token (or set TELEGRAM_TOKEN)")
        sp_bot.add_argument("--hour", type=int, default=8, help="Daily digest hour (Europe/London)")
        sp_bot.add_argument("--minute", type=int, default=30, help="Daily digest minute (Europe/London)")
        def _cmd_bot(args):
            token = args.token or os.getenv("TELEGRAM_TOKEN")
            if not token:
                raise SystemExit("TELEGRAM_TOKEN not set and --token not provided")
            db_url = args.db or default_db
            _mod_tg.run_bot(token, db_url=db_url, digest_hour=args.hour, digest_minute=args.minute)
        sp_bot.set_defaults(func=_cmd_bot)

    # ---- Web app (if module present) ----
    if _mod_web and hasattr(_mod_web, 'create_app'):
        sp_web = sub.add_parser("web", help="Run the Flask web app")
        sp_web.add_argument("--host", default="127.0.0.1")
        sp_web.add_argument("--port", type=int, default=5000)
        def _cmd_web(args):
            db_url = args.db or default_db
            app = _mod_web.create_app(db_url)
            app.run(host=args.host, port=args.port)
        sp_web.set_defaults(func=_cmd_web)

    # ---- Legacy passthroughs (only if those modules exist) ----
    # ingest-fixtures
    if _mod_legacy_ingest_fx and hasattr(_mod_legacy_ingest_fx, 'main'):
        sp_legacy_fx = sub.add_parser("ingest-fixtures", help="(legacy) Ingest fixtures via legacy script")
        def _cmd_legacy_fx(args):
            _mod_legacy_ingest_fx.main()
        sp_legacy_fx.set_defaults(func=_cmd_legacy_fx)

    # fetch-openfootball / ingest-openfootball
    if _mod_legacy_openfootball:
        if hasattr(_mod_legacy_openfootball, 'fetch_openfootball'):
            sp_fetch_of = sub.add_parser("fetch-openfootball", help="(legacy) Fetch OpenFootball datasets")
            sp_fetch_of.add_argument("--mode", choices=["init", "update"], default="update")
            def _cmd_fetch_of(args):
                _mod_legacy_openfootball.fetch_openfootball(mode=args.mode)
            sp_fetch_of.set_defaults(func=_cmd_fetch_of)
        if hasattr(_mod_legacy_openfootball, 'ingest_openfootball'):
            sp_ing_of = sub.add_parser("ingest-openfootball", help="(legacy) Ingest OpenFootball datasets")
            def _cmd_ing_of(args):
                _mod_legacy_openfootball.ingest_openfootball()
            sp_ing_of.set_defaults(func=_cmd_ing_of)

    # generate-suggestions (legacy)
    if _mod_legacy_suggestions and hasattr(_mod_legacy_suggestions, 'main'):
        sp_sug = sub.add_parser("generate-suggestions", help="(legacy) Generate betting suggestions")
        def _cmd_sug(args):
            _mod_legacy_suggestions.main()
        sp_sug.set_defaults(func=_cmd_sug)

    # telegram (legacy alt entrypoint)
    if _mod_tg and hasattr(_mod_tg, 'run_bot'):
        sp_tg_legacy = sub.add_parser("telegram", help="(legacy) Run Telegram bot")
        sp_tg_legacy.add_argument("--token", required=False)
        def _cmd_tg_legacy(args):
            token = args.token or os.getenv("TELEGRAM_TOKEN")
            if not token:
                raise SystemExit("TELEGRAM_TOKEN not set and --token not provided")
            db_url = args.db or default_db
            _mod_tg.run_bot(token, db_url=db_url)
        sp_tg_legacy.set_defaults(func=_cmd_tg_legacy)

    # live-betdaq (legacy)
    if _mod_legacy_live_betdaq and hasattr(_mod_legacy_live_betdaq, 'main'):
        sp_betdaq = sub.add_parser("live-betdaq", help="(legacy) Live Betdaq stream")
        def _cmd_betdaq(args):
            _mod_legacy_live_betdaq.main()
        sp_betdaq.set_defaults(func=_cmd_betdaq)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
import os

DEBUG = os.getenv("FIXTURE_DEBUG") == "1"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.bbc.co.uk/sport/football/scores-fixtures",
}

def ingest_bbc_fixtures(conn, sport, date_iso=None):
    # existing code to fetch html
    html = ...  # placeholder for actual fetch code
    if DEBUG:
        print(f"[BBC DEBUG] HTML bytes: {len(html.encode('utf-8')) if html else 0}")
    # rest of ingest processing

def _parse_football_day(soup):
    found = 0
    sections = soup.find_all(...)  # placeholder for actual code
    sections_count = len(sections)
    for section in sections:
        comp_name = ...  # extract competition name
        cards = section.find_all(...)  # placeholder
        cards_count = len(cards)
        if DEBUG:
            print(f"[BBC DEBUG] Section '{comp_name}' -> {cards_count} cards")
        for card in cards:
            # parse fixture info
            found += 1
            yield ...  # yield fixture dict or object
    if DEBUG:
        print(f"[BBC DEBUG] Parsed fixtures: {found} (sections: {sections_count})")
import os

DEBUG = os.getenv("FIXTURE_DEBUG") == "1"

def ingest(conn, future_only=True):
    # existing code to fetch fixtures
    fixtures = ...  # fetched fixtures list
    if DEBUG:
        print(f"[FPL DEBUG] fixtures returned: {len(fixtures)}")
    count = 0
    for fixture in fixtures:
        # upsert fixture
        count += 1
    if DEBUG:
        print(f"[FPL DEBUG] upserted: {count}")
    return count
import os

DEBUG = os.getenv("FIXTURE_DEBUG") == "1"
BASE_URL = "https://site.web.api.espn.com/apis/v2/sports/{path}/scoreboard"
BASE_URL_FALLBACK = "https://site.web.api.espn.com/apis/v2/sports/{path}/scoreboard"

def ingest_fixtures(conn, league, date_iso=None):
    path = "soccer/eng.1" if league == "football" else "soccer/eng.2"
    url = BASE_URL.format(path=path)
    params = {}
    if date_iso:
        params["dates"] = date_iso
    data = _espn_get(url, params=params)
    events = data.get("events", []) or []
    if DEBUG:
        print(f"[ESPN DEBUG] URL: {url} params: {params} events: {len(events)}")
    if not events:
        fb_url = BASE_URL_FALLBACK.format(path=path)
        try:
            data = _espn_get(fb_url, params=params)
            events = data.get("events", []) or []
            if DEBUG:
                print(f"[ESPN DEBUG] Fallback URL: {fb_url} events: {len(events)}")
        except Exception as _:
            pass
    # process events and upsert
    n = 0
    # ... upsert logic, increment n
    if DEBUG:
        print(f"[ESPN DEBUG] Upserted {n} fixtures")
    return n
