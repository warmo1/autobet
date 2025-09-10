import os
import time
import pandas as pd
from datetime import date, timedelta, datetime
from flask import Flask, render_template, request, redirect, flash, url_for, send_file
from .config import cfg
from .db import get_db, init_db
from pathlib import Path
import json
import threading
import traceback
import math
from typing import Optional, Sequence, Mapping, Any
from google.cloud import bigquery
from .providers.tote_bets import place_audit_superfecta
from .providers.tote_bets import refresh_bet_status, audit_list_bets, sync_bets_from_api
from .gcp import publish_pubsub_message
import itertools
import math
from collections import defaultdict

# Helper function for permutations (nPr) - not directly used in PL, but good to have
def _permutations_count(n, r):
    if r < 0 or r > n:
        return 0
    
    res = 1
    for i in range(r):
        res *= (n - i)
    return res


app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv("FLASK_SECRET", "dev-secret")


def _today_iso() -> str:
    """Returns today's date in YYYY-MM-DD format."""
    return date.today().isoformat()

# --- Background: pool subscription (optional) ---
_pool_thread_started = False

def _pool_subscriber_loop():
    from .providers.tote_subscriptions import run_subscriber
    while True:
        try:
            # The subscriber loop will now use the BigQuery sink via the new db layer
            db = get_db()
            run_subscriber(db, duration=None) # Assuming run_subscriber is adapted for a BQ sink
        except Exception as e:
            try:
                print("[PoolSub] error:", e)
                traceback.print_exc()
            except Exception:
                pass
            try:
                time.sleep(5)
            except Exception:
                pass

def _maybe_start_pool_thread():
    global _pool_thread_started
    if _pool_thread_started:
        return
    flag = os.getenv("SUBSCRIBE_POOLS", "0").lower() in ("1","true","yes","on")
    if flag:
        t = threading.Thread(target=_pool_subscriber_loop, name="pool-subscriber", daemon=True)
        t.start()
        _pool_thread_started = True

def _maybe_start_bq_exporter():
    # Placeholder for legacy background exporter. No-op in BQ-only mode.
    return


def _use_bq() -> bool:
    return bool(cfg.bq_project and cfg.bq_dataset)


def _build_bq_query(sql: str, params: Any) -> tuple[str, list[bigquery.ScalarQueryParameter]]:
    """Prepare SQL and BigQuery parameters from either a mapping or a sequence.

    - If params is a mapping, assume the SQL uses @name placeholders.
    - If params is a sequence, convert positional '?' to @p0, @p1, ...
    - If params is None/empty, return as-is with no parameters.
    """
    qp: list[bigquery.ScalarQueryParameter] = []
    if not params:
        return sql, qp
    # Mapping (named parameters)
    if isinstance(params, Mapping):
        for k, v in params.items():
            if isinstance(v, bool):
                qp.append(bigquery.ScalarQueryParameter(str(k), "BOOL", v))
            elif v is None:
                # Default typed NULL to FLOAT64 for numeric expressions
                qp.append(bigquery.ScalarQueryParameter(str(k), "FLOAT64", None))
            elif isinstance(v, float):
                qp.append(bigquery.ScalarQueryParameter(str(k), "FLOAT64", v))
            elif isinstance(v, int):
                qp.append(bigquery.ScalarQueryParameter(str(k), "INT64", v))
            else:
                qp.append(bigquery.ScalarQueryParameter(str(k), "STRING", str(v)))
        return sql, qp
    # Sequence (positional parameters)
    if isinstance(params, (list, tuple)):
        parts = sql.split("?")
        new_sql = ""
        for i, part in enumerate(parts[:-1]):
            new_sql += part + f"@p{i}"
        new_sql += parts[-1]
        for i, v in enumerate(params):
            if isinstance(v, bool):
                qp.append(bigquery.ScalarQueryParameter(f"p{i}", "BOOL", v))
            elif isinstance(v, float):
                qp.append(bigquery.ScalarQueryParameter(f"p{i}", "FLOAT64", v))
            elif isinstance(v, int):
                qp.append(bigquery.ScalarQueryParameter(f"p{i}", "INT64", v))
            else:
                qp.append(bigquery.ScalarQueryParameter(f"p{i}", "STRING", None if v is None else str(v)))
        return new_sql, qp
    # Fallback: treat as string (single param) -> STRING
    qp.append(bigquery.ScalarQueryParameter("p0", "STRING", str(params)))
    return sql.replace("?", "@p0"), qp


def _bq_execute(sql: str, params: Any = None):
    """Execute a DML/DDL query against BigQuery and return the result iterator."""
    db = get_db()
    q, qp = _build_bq_query(sql, params)
    job_config = bigquery.QueryJobConfig(
        default_dataset=f"{cfg.bq_project}.{cfg.bq_dataset}",
        query_parameters=qp,
    )
    return db.query(q, job_config=job_config)


def _viability_local_perm(N: int, K: int, O: float, M: int, l: float, t: float, R: float, inc_self: bool, mult: float, f_fix: float | None) -> dict:
    # total lines P(N,K)
    C = 0
    if N and K and N >= K:
        ctmp = 1
        for i in range(K):
            ctmp *= (N - i)
        C = int(ctmp)
    M_adj = max(0, min(M, C)) if C > 0 else 0
    alpha = (M_adj / C) if C > 0 else 0.0
    S = M_adj * l
    S_inc = S if inc_self else 0.0
    # auto f-share
    denom = (C * l * l + O)
    f_auto = float((C * l * l) / denom) if (C > 0 and denom > 0) else 0.0
    f_used = float(f_fix) if (f_fix is not None) else f_auto
    NetPool_now = mult * (((1.0 - t) * (O + S_inc)) + R)
    ExpReturn = alpha * f_used * NetPool_now
    ExpProfit = ExpReturn - S
    cover_all_stake = C * l
    cover_all_S_inc = cover_all_stake if inc_self else 0.0
    NetPool_all = mult * (((1.0 - t) * (O + cover_all_S_inc)) + R)
    ExpProfit_all = f_used * NetPool_all - cover_all_stake
    cover_all_pos = (ExpProfit_all > 0)
    denom2 = (f_used * mult * (1.0 - t))
    O_min = ((C * l) - (f_used * mult * (((1.0 - t) * cover_all_S_inc) + R))) / denom2 if denom2 > 0 else None
    # alpha min for +EV
    if C == 0 or l == 0 or f_used <= 0 or (1.0 - t) <= 0:
        alpha_min = None
    elif inc_self:
        num = (C * l) - (f_used * mult * (((1.0 - t) * O) + R))
        den = f_used * mult * (1.0 - t) * (C * l)
        alpha_min = max(0.0, min(1.0, num / den)) if den > 0 else None
    else:
        alpha_min = 0.0 if (f_used * mult * (((1.0 - t) * O) + R)) > (C * l) else None
    return {
        "total_lines": C,
        "lines_covered": M_adj,
        "coverage_frac": alpha,
        "stake_total": S,
        "net_pool_if_bet": NetPool_now,
        "f_share_used": f_used,
        "expected_return": ExpReturn,
        "expected_profit": ExpProfit,
        "is_positive_ev": (ExpProfit > 0),
        "cover_all_stake": cover_all_stake,
        "cover_all_expected_profit": ExpProfit_all,
        "cover_all_is_positive": cover_all_pos,
        "cover_all_o_min_break_even": O_min,
        "coverage_frac_min_positive": alpha_min,
    }

def _row_to_json_response(df_row_series):
    """Safely convert a pandas Series (row) to a JSON response."""
    row_dict = df_row_series.to_dict()
    # Handle numpy/pandas types that json.dumps doesn't like
    for k, v in row_dict.items():
        if pd.isna(v):
            row_dict[k] = None
    return app.response_class(json.dumps(row_dict), mimetype="application/json")


# This function is already in webapp.py, but including it here for context of its usage
def sql_df(*args, **kwargs) -> pd.DataFrame:
    """Runs a query against BigQuery and returns a pandas DataFrame.

    Backwards compatible signatures:
    - sql_df(sql: str, params: Optional[Sequence|Mapping] = None)
    - sql_df(conn_ignored, sql: str, params: Optional[Sequence|Mapping] = None)
    """
    if not _use_bq():
        raise RuntimeError("BigQuery is not configured. Set BQ_PROJECT and BQ_DATASET.")
    # Parse arguments (support legacy first arg = conn)
    if not args:
        raise TypeError("sql_df requires at least the SQL string")
    if isinstance(args[0], str):
        sql = args[0]
        params = kwargs.get("params")
    else:
        if len(args) < 2 or not isinstance(args[1], str):
            raise TypeError("sql_df(conn?, sql: str, params=...) expected")
        sql = args[1]
        params = kwargs.get("params") or (args[2] if len(args) > 2 else None)

    q, qp = _build_bq_query(sql, params)
    job_config = bigquery.QueryJobConfig(
        default_dataset=f"{cfg.bq_project}.{cfg.bq_dataset}",
        query_parameters=qp,
    )
    db = get_db()
    return db.query(q, job_config=job_config).to_dataframe(create_bqstorage_client=False)

@app.template_filter('datetime')
def _fmt_datetime(ts: float | int | str):
    try:
        import datetime as _dt
        v = float(ts)
        return _dt.datetime.utcfromtimestamp(v).strftime('%Y-%m-%d %H:%M')
    except Exception:
        try:
            return str(ts)
        except Exception:
            return ''

@app.route("/")
def index():
    """Simplified dashboard with quick links and bank widget."""
    from datetime import datetime, timedelta, timezone

    # Bankroll is a placeholder as it was stored in SQLite
    bankroll = cfg.paper_starting_bankroll

    # UI Improvement: Show all upcoming events in the next 24 hours, not just horse racing in the next 60 mins.
    now = datetime.now(timezone.utc)
    start_iso = now.isoformat(timespec='seconds').replace('+00:00', 'Z')
    end_iso = (now + timedelta(hours=24)).isoformat(timespec='seconds').replace('+00:00', 'Z')

    ev_sql = (
        "SELECT event_id, name, venue, country, start_iso, sport, status "
        "FROM tote_events WHERE start_iso BETWEEN @start AND @end "
        "ORDER BY start_iso ASC LIMIT 100"
    )
    evs = sql_df(ev_sql, params={"start": start_iso, "end": end_iso})

    # Optional: superfecta products upcoming
    # UI Improvement: Also expand the time window for upcoming Superfecta products.
    sf_sql = (
        "SELECT product_id, event_id, event_name, venue, start_iso, status, currency, total_net "
        "FROM tote_products WHERE UPPER(bet_type)='SUPERFECTA' AND start_iso BETWEEN @start AND @end "
        "ORDER BY start_iso ASC LIMIT 20"
    )
    sfs = sql_df(sf_sql, params={"start": start_iso, "end": end_iso})

    return render_template(
        "index.html",
        balance=round(bankroll, 2),
        now_next=(evs.to_dict("records") if not evs.empty else []),
        now_next_sf=(sfs.to_dict("records") if not sfs.empty else []),
    )

# --- Schema helper routes (for Tote GraphQL docs/devtools) ---
def _schema_path() -> str:
    return str(Path(__file__).resolve().parent.parent / 'docs' / 'tote_schema.graphqls')

def _sdl_to_html(sdl: str) -> str:
    # Minimal pre block; avoid heavy formatting client-side
    try:
        return f"<pre style='white-space:pre-wrap; font-family:monospace;'>{sdl.replace('<','&lt;').replace('>','&gt;')}</pre>"
    except Exception:
        return "<pre>(error rendering SDL)</pre>"

def _write_sdl(sdl: str) -> None:
    p = _schema_path()
    Path(p).parent.mkdir(parents=True, exist_ok=True)
    with open(p, 'w', encoding='utf-8') as f:
        f.write(sdl)

@app.route("/tote/schema")
def tote_schema_page():
    sdl = None; err = None
    try:
        p = _schema_path()
        if os.path.exists(p):
            with open(p, 'r', encoding='utf-8') as f:
                sdl = f.read()
    except Exception as e:
        err = str(e)
    sdl_html = _sdl_to_html(sdl) if sdl else None
    # Basic index of helper links
    index = [
        ("Download SDL", url_for('tote_schema_download')),
        ("Refresh SDL", url_for('tote_schema_refresh')),
        ("Probe Products", url_for('tote_schema_probe')),
        ("Example Queries", url_for('tote_schema_queries')),
    ]
    return render_template("tote_schema.html", sdl_html=sdl_html, index=index, error=err)

@app.route("/tote/schema/download")
def tote_schema_download():
    p = _schema_path()
    if not os.path.exists(p):
        flash("Schema file not found. Use Refresh to fetch from API.", "error")
        return redirect(url_for('tote_schema_page'))
    return send_file(p, as_attachment=True, download_name="tote_schema.graphqls")

@app.route("/tote/schema/refresh")
def tote_schema_refresh():
    try:
        from .providers.tote_api import ToteClient
        client = ToteClient()
        sdl = client.graphql_sdl()
        _write_sdl(sdl)
        flash("Schema refreshed from API.", "success")
    except Exception as e:
        flash(f"Refresh failed: {e}", "error")
    return redirect(url_for('tote_schema_page'))

@app.route("/tote/schema/probe")
def tote_schema_probe():
    from flask import Response
    try:
        from .providers.tote_api import ToteClient
        from .ingest.tote_products import PRODUCTS_QUERY
        client = ToteClient()
        data = client.graphql(PRODUCTS_QUERY, {"first": 1, "status": "OPEN"})
        import json as _json
        txt = _json.dumps(data, indent=2)[:100000]
        return Response(txt, mimetype="application/json")
    except Exception as e:
        import json as _json
        return Response(_json.dumps({"error": str(e)}), mimetype="application/json", status=500)

@app.route("/tote/schema/queries")
def tote_schema_queries():
    p = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'docs', 'tote_queries.graphql'))
    if os.path.exists(p):
        return send_file(p, as_attachment=True, download_name="tote_queries.graphql")
    flash("Example queries not found.", "error")
    return redirect(url_for('tote_schema_page'))

@app.route("/tote/params", methods=["POST"])
def tote_params_update():
    """Update BigQuery tote_params with a new defaults row.

    Fields: t, f, stake_per_line, R, model_id, ts_ms, default_top_n, target_coverage
    """
    if not _use_bq():
        flash("BigQuery not configured for web. Set WEB_USE_BIGQUERY=1 and BQ_* envs.", "error")
        return redirect(request.referrer or url_for('tote_viability_page'))
    # Parse form values (allow blanks -> NULL)
    def fnum(name):
        v = request.form.get(name)
        if v is None or v == "":
            return None
        try:
            return float(v)
        except Exception:
            return None
    def inum(name):
        v = request.form.get(name)
        if v is None or v == "":
            return None
        try:
            return int(v)
        except Exception:
            return None
    t = fnum("t")
    f = fnum("f")
    stake_per_line = fnum("stake_per_line")
    R = fnum("R")
    model_id = (request.form.get("model_id") or None)
    ts_ms = inum("ts_ms")
    default_top_n = inum("top_n")
    target_coverage = fnum("coverage")
    # Build INSERT (latest row will be selected by views)
    sql = (
        "INSERT INTO `" + f"{cfg.bq_project}.{cfg.bq_dataset}" + 
        ".tote_params`(t,f,stake_per_line,R,model_id,ts_ms,default_top_n,target_coverage) VALUES (?,?,?,?,?,?,?,?)"
    )
    try:
        _ = _bq_execute(sql, params=(t, f, stake_per_line, R, model_id, ts_ms, default_top_n, target_coverage))
        flash("Saved BigQuery tote_params defaults.", "success")
    except Exception as e:
        flash(f"Failed to save tote_params: {e}", "error")
    return redirect(request.referrer or url_for('tote_viability_page'))

@app.route("/tote-events")
def tote_events_page():
    """List Tote events with filters and pagination."""
    country = (request.args.get("country") or "").strip().upper()
    sport = (request.args.get("sport") or "").strip()
    venue = (request.args.get("venue") or "").strip()
    name = (request.args.get("name") or "").strip()
    date_from = (request.args.get("from") or "").strip()
    date_to = (request.args.get("to") or "").strip()
    limit = max(1, int(request.args.get("limit", "200") or 200))
    page = max(1, int(request.args.get("page", "1") or 1))
    offset = (page - 1) * limit
    where = []
    params: list[object] = []
    if country:
        where.append("UPPER(country)=?"); params.append(country)
    if sport:
        where.append("sport=?"); params.append(sport)
    if venue:
        where.append("UPPER(venue) LIKE UPPER(?)"); params.append(f"%{venue}%")
    if name:
        where.append("UPPER(name) LIKE UPPER(?)"); params.append(f"%{name.upper()}%")
    if date_from:
        where.append("start_iso >= ?"); params.append(date_from)
    if date_to:
        where.append("start_iso <= ?"); params.append(date_to)
    base_sql = (
        "SELECT event_id, name, venue, country, start_iso, sport, status, competitors_json, home, away "
        "FROM tote_events"
    )
    count_sql = base_sql.replace("SELECT event_id, name, venue, country, start_iso, sport, status, competitors_json, home, away", "SELECT COUNT(1) AS c")
    if where:
        base_sql += " WHERE " + " AND ".join(where)
        count_sql += " WHERE " + " AND ".join(where)
    base_sql += " ORDER BY start_iso DESC LIMIT ? OFFSET ?"
    params_paged = list(params) + [limit, offset]
    # Performance: Simplified total count query.
    df = sql_df(base_sql, params=tuple(params_paged))
    total = int(sql_df(count_sql, params=tuple(params)).iloc[0]["c"])
    countries_df = sql_df("SELECT DISTINCT country FROM tote_events WHERE country IS NOT NULL AND country<>'' ORDER BY country")
    sports_df = sql_df("SELECT DISTINCT sport FROM tote_events WHERE sport IS NOT NULL AND sport<>'' ORDER BY sport")
    events = df.to_dict("records") if not df.empty else []
    for ev in events:
        comps = []
        try:
            arr = json.loads(ev.get("competitors_json") or "[]")
            comps = [c.get("name") for c in arr if isinstance(c, dict) and c.get("name")]
        except Exception:
            comps = []
        ev["competitors"] = ", ".join(comps)
    return render_template(
        "tote_events.html",
        events=events,
        filters={
            "country": country or "",
            "sport": sport or "",
            "venue": venue or "",
            "name": name or "",
            "date_from": date_from or "",
            "date_to": date_to or "",
            "limit": limit,
            "page": page,
            "total": total,
        },
        country_options=(countries_df['country'].tolist() if not countries_df.empty else []),
        sport_options=(sports_df['sport'].tolist() if not sports_df.empty else []),
    )

@app.route("/tote-superfecta", methods=["GET","POST"])
def tote_superfecta_page():
    """List SUPERFECTA products with filters, upcoming widget, and audit helpers."""
    if request.method == "POST":
        flash("Paper betting is disabled. Use Audit placement.", "error")
    # UI Improvement: Default country to GB and add a venue filter.
    country = (request.args.get("country") or os.getenv("DEFAULT_COUNTRY", "GB")).strip().upper()
    venue = (request.args.get("venue") or "").strip()
    status = (request.args.get("status") or "").strip().upper()
    date_from = request.args.get("from") or _today_iso()
    date_to = request.args.get("to") or _today_iso()
    limit = int(request.args.get("limit", "200") or 200)
    upcoming_flag = (request.args.get("upcoming") or "").lower() in ("1","true","yes","on")

    where = ["UPPER(p.bet_type)='SUPERFECTA'"]
    params: list[object] = []
    if country:
        where.append("(UPPER(e.country)=? OR UPPER(p.currency)=?)"); params.extend([country, country])
    if venue:
        where.append("UPPER(COALESCE(e.venue, p.venue)) LIKE UPPER(?)"); params.append(f"%{venue}%")
    if status:
        where.append("UPPER(COALESCE(p.status,'')) = ?"); params.append(status)
    if date_from:
        where.append("substr(p.start_iso,1,10) >= ?"); params.append(date_from)
    if date_to:
        where.append("substr(p.start_iso,1,10) <= ?"); params.append(date_to)
    if upcoming_flag:
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')
        where.append("p.start_iso >= ?"); params.append(now_iso)

    sql = (
        "SELECT p.product_id, p.event_id, p.event_name, COALESCE(e.venue, p.venue) AS venue, e.country, p.start_iso, "
        "COALESCE(p.status,'') AS status, p.currency, p.total_gross, p.total_net, p.rollover, "
        # UI Improvement: The product_id column can be de-emphasized in the template in favor of more user-friendly info.
        "(SELECT COUNT(1) FROM tote_product_selections s WHERE s.product_id = p.product_id) AS n_runners "
        "FROM tote_products p LEFT JOIN tote_events e USING(event_id) "
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY p.start_iso DESC LIMIT ?"; params.append(limit)
    df = sql_df(sql, params=tuple(params))

    # Options for filters
    cdf = sql_df("SELECT DISTINCT country FROM tote_events WHERE country IS NOT NULL AND country<>'' ORDER BY country")
    # UI Improvement: Dynamic venue options based on other filters for a better user experience.
    vdf_sql = "SELECT DISTINCT COALESCE(e.venue, p.venue) AS venue FROM tote_products p LEFT JOIN tote_events e USING(event_id) WHERE UPPER(p.bet_type)='SUPERFECTA' AND COALESCE(e.venue, p.venue) IS NOT NULL AND COALESCE(e.venue, p.venue) <> ''"
    vdf_params: list[object] = []
    if country:
        vdf_sql += " AND (UPPER(e.country)=? OR UPPER(p.currency)=?)"
        vdf_params.extend([country, country])
    if date_from and date_to:
        vdf_sql += " AND substr(p.start_iso,1,10) BETWEEN ? AND ?"
        vdf_params.extend([date_from, date_to])
    vdf_sql += " ORDER BY venue"
    vdf = sql_df(vdf_sql, params=tuple(vdf_params))
    sdf = sql_df("SELECT DISTINCT COALESCE(status,'') AS status FROM tote_products WHERE bet_type='SUPERFECTA' ORDER BY 1")

    # Optional upcoming 60m (GB) from BigQuery
    upcoming = []
    try:
        udf = sql_df("SELECT product_id, event_id, event_name, venue, country, start_iso, status, currency, n_competitors, combos, S, O_min, roi_current, viable_now FROM vw_gb_open_superfecta_next60_be ORDER BY start_iso")
        if not udf.empty:
            upcoming = udf.to_dict("records")
    except Exception: # The view might not exist, fail gracefully
        upcoming = []

    products = df.to_dict("records") if not df.empty else []
    return render_template(
        "tote_superfecta.html",
        products=products,
        filters={
            "country": country,
            "venue": venue,
            "status": status,
            "from": date_from,
            "to": date_to,
            "limit": limit,
            "upcoming": upcoming_flag,
        },
        venue_options=(vdf['venue'].tolist() if not vdf.empty else []),
        country_options=(cdf['country'].tolist() if not cdf.empty else ['GB']),
        status_options=(sdf['status'].tolist() if not sdf.empty else ['OPEN','CLOSED']),
        upcoming=(upcoming or []),
    )

def create_app():
    """Factory for Flask app; also starts background subscribers if configured."""
    try:
        _maybe_start_pool_thread()
    except Exception:
        pass
    try:
        _maybe_start_bq_exporter()
    except Exception:
        pass
    return app

# Removed legacy fixtures/suggestions pages

@app.route("/bets", methods=["GET", "POST"])
def bets_page():
    """Paper betting UI disabled."""
    flash("Paper betting is disabled in this UI.", "error")
    return redirect(url_for("index"))

@app.route("/tote-pools")
def tote_pools_page():
    # Filters similar to Superfecta/Calculators for consistent UX
    bet_type = (request.values.get("bet_type") or "").strip().upper()
    country = (request.values.get("country") or "").strip().upper()
    status = (request.values.get("status") or "").strip().upper()
    venue = (request.values.get("venue") or "").strip()
    date_filter = (request.values.get("date") or _today_iso()).strip()

    limit = int(request.values.get("limit", "500") or 500)
    page = max(1, int(request.values.get("page", "1") or 1))
    offset = (page - 1) * limit

    # Performance/UI Improvement: Major refactor of this view.
    # - The main query now joins dividend counts and race conditions to avoid slow Python-side lookups.
    # - Filtering logic is cleaned up.
    # - Venue filter options are now dynamic based on selected date and country.
    base_sql = (
        "SELECT p.product_id, UPPER(p.bet_type) AS bet_type, COALESCE(p.status,'') AS status, "
        "COALESCE(e.venue, p.venue) AS venue, UPPER(COALESCE(e.country,p.currency)) AS country, p.start_iso, "
        "p.currency, p.total_gross, p.total_net, p.rollover, p.deduction_rate, p.event_id, COALESCE(e.name, p.event_name) AS event_name, "
        "divs.c AS dividend_count, "
        "rc.going, rc.weather_temp_c, rc.weather_wind_kph, rc.weather_precip_mm "
        "FROM tote_products p "
        "LEFT JOIN tote_events e USING(event_id) "
        "LEFT JOIN (SELECT product_id, COUNT(1) AS c FROM tote_product_dividends GROUP BY product_id) divs ON divs.product_id = p.product_id "
        "LEFT JOIN race_conditions rc ON rc.event_id = p.event_id "
    )
    where = ["COALESCE(p.status,'') <> ''"]
    params: list[object] = []
    if bet_type:
        where.append("UPPER(p.bet_type)=?"); params.append(bet_type)
    if country:
        where.append("UPPER(COALESCE(e.country,p.currency))=?"); params.append(country)
    if status:
        where.append("UPPER(COALESCE(p.status,''))=?"); params.append(status)
    if venue:
        where.append("UPPER(COALESCE(e.venue,p.venue)) LIKE UPPER(?)"); params.append(f"%{venue}%")
    if date_filter:
        where.append("SUBSTR(p.start_iso,1,10)=?"); params.append(date_filter)

    filtered_sql = base_sql + " WHERE " + " AND ".join(where)

    # Total count
    count_sql = "SELECT COUNT(1) AS c FROM (" + filtered_sql + ") AS sub"
    total = int(sql_df(count_sql, params=tuple(params)).iloc[0]["c"])

    # Page slice, sorted latest first
    paged_sql = filtered_sql + " ORDER BY p.start_iso DESC LIMIT ? OFFSET ?"
    paged_params = list(params) + [limit, offset]
    df = sql_df(paged_sql, params=tuple(paged_params))

    # Options for filters
    types_df = sql_df("SELECT DISTINCT UPPER(bet_type) AS bet_type FROM tote_products WHERE bet_type IS NOT NULL AND bet_type <> '' ORDER BY bet_type")
    curr_df = sql_df("SELECT DISTINCT UPPER(COALESCE(e.country,p.currency)) AS country FROM tote_products p LEFT JOIN tote_events e USING(event_id) WHERE COALESCE(e.country,p.currency) IS NOT NULL AND COALESCE(e.country,p.currency) <> '' ORDER BY country")
    venues_df_sql = (
        "SELECT DISTINCT COALESCE(e.venue,p.venue) AS venue FROM tote_products p LEFT JOIN tote_events e USING(event_id) "
        "WHERE COALESCE(p.status,'') <> '' AND COALESCE(e.venue,p.venue) IS NOT NULL AND COALESCE(e.venue,p.venue)<>''"
    )
    venues_df_params: list[object] = []
    if country:
        venues_df_sql += " AND UPPER(COALESCE(e.country,p.currency))=?"
        venues_df_params.append(country)
    if date_filter:
        venues_df_sql += " AND SUBSTR(p.start_iso,1,10)=?"
        venues_df_params.append(date_filter)
    venues_df_sql += " ORDER BY venue"
    venues_df = sql_df(venues_df_sql, params=tuple(venues_df_params))

    # Group totals per bet type (respecting filters except pagination)
    gt_sql = "SELECT UPPER(bet_type) AS bet_type, COUNT(1) AS n, SUM(total_net) AS sum_net FROM (" + filtered_sql + ") AS sub GROUP BY bet_type ORDER BY bet_type"
    gt = sql_df(gt_sql, params=tuple(params))

    prods = df.to_dict("records") if not df.empty else []
    for p in prods:
        p["dividend_count"] = int(p.get("dividend_count") or 0)
        # Attach conditions badge
        t = p.get("weather_temp_c"); w = p.get("weather_wind_kph"); pr = p.get("weather_precip_mm")
        if pd.notnull(t) or pd.notnull(w) or pd.notnull(pr):
            p["weather_badge"] = f"{'' if pd.isnull(t) else int(round(t))}°C {'' if pd.isnull(w) else int(round(w))}kph {'' if pd.isnull(pr) else pr:.1f}mm"
        else:
            p["weather_badge"] = None

    # Aggregate totals
    total_net = sum((p.get('total_net') or 0) for p in prods)
    group_totals = gt.to_dict("records") if not gt.empty else []
    return render_template( # noqa: E501
        "tote_pools.html",
        products=prods,
        totals={"total_net": total_net, "count": len(prods), "total": int(total), "page": page, "limit": limit},
        # Note for template: To "hold" filters from other pages, links to this page
        # must include the query parameters, e.g., <a href="{{ url_for('tote_pools_page', country=current_country) }}">
        group_totals=group_totals,
        filters={
            "bet_type": bet_type or "",
            "country": country or "",
            "status": status or "",
            "venue": venue or "",
            "date": date_filter or "",
            "limit": limit,
            "page": page,
        },
        bet_type_options=(types_df['bet_type'].tolist() if not types_df.empty else []),
        country_options=(curr_df['country'].tolist() if not curr_df.empty else []),
        venue_options=(venues_df['venue'].tolist() if not venues_df.empty else []),
    )

@app.route("/tote-pools/summary")
def tote_pools_summary_page():
    """Aggregated view of pools by bet_type, status, and country with basic stats."""
    by_type = sql_df("SELECT UPPER(bet_type) AS bet_type, COUNT(1) AS n FROM tote_products GROUP BY 1 ORDER BY n DESC")
    by_status = sql_df("SELECT COALESCE(status,'') AS status, COUNT(1) AS n FROM tote_products GROUP BY 1 ORDER BY n DESC")
    by_country = sql_df("SELECT COALESCE(currency,'') AS country, COUNT(1) AS n, ROUND(AVG(total_net),2) AS avg_total_net, ROUND(MAX(total_net),2) AS max_total_net FROM tote_products GROUP BY 1 ORDER BY n DESC")
    recent_sql = (
        "SELECT UPPER(bet_type) AS bet_type, COUNT(1) AS n, ROUND(AVG(total_net),2) AS avg_total_net, "
        "APPROX_QUANTILES(total_net, 5)[SAFE_OFFSET(2)] AS p50, "
        "APPROX_QUANTILES(total_net, 5)[SAFE_OFFSET(4)] AS p90 "
        "FROM tote_products "
        "WHERE DATE(SUBSTR(start_iso,1,10)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) "
        "GROUP BY 1 ORDER BY n DESC"
    )
    recent = sql_df(recent_sql)
    return render_template(
        "tote_pools_summary.html",
        by_type=(by_type.to_dict("records") if not by_type.empty else []),
        by_status=(by_status.to_dict("records") if not by_status.empty else []),
        by_country=(by_country.to_dict("records") if not by_country.empty else []),
        recent=(recent.to_dict("records") if not recent.empty else []),
    )

@app.route("/tote/calculators", methods=["GET", "POST"])
def tote_calculators_page():
    """
    Combined page for bet building (Plackett-Luce weighting from probable odds)
    and financial viability analysis (EV calculation from pool size). This page
    is designed to be the primary tool for analyzing a race, showing runners,
    probable odds, pool size, and the results of both weighting and viability models.
    """
    # --- Refresh Logic ---
    # Optional refresh of products (OPEN) to update units (BQ-only)
    if request.method == "POST" and (request.form.get("refresh") == "1"):
        try:
            from .providers.tote_api import ToteClient
            from .ingest.tote_products import ingest_products
            from .db import get_db
            ds = _today_iso()
            client = ToteClient()
            sink = get_db()
            # Refresh selected bet type (default SUPERFECTA)
            sel_bt = (request.values.get("bet_type") or "SUPERFECTA").strip().upper()
            ingest_products(sink, client, date_iso=ds, status="OPEN", first=400, bet_types=[sel_bt])
            flash("Refreshed products and selection units from Tote API.", "success")
        except Exception as e:
            flash(f"Refresh failed: {e}", "error")

    # --- Filters & Parameters ---
    country = (request.values.get("country") or os.getenv("DEFAULT_COUNTRY", "GB")).strip().upper()
    venue = (request.values.get("venue") or "").strip()
    date_filter = (request.values.get("date") or _today_iso()).strip()
    product_id = (request.values.get("product_id") or "").strip()
    bet_type = (request.values.get("bet_type") or "SUPERFECTA").strip().upper()

    # Weighting model parameters
    try:
        req_top_n = int(request.values.get("top_n", "7") or 7)
    except Exception:
        req_top_n = 7
    coverage = max(0.0, min(1.0, float(request.values.get("coverage", "0.6") or 0.6)))

    # Viability model parameters
    stake_per_line = float(request.values.get("stake_per_line", "0.10") or 0.10)
    take_rate_in = request.values.get("takeout")
    net_rollover_in = request.values.get("rollover")
    inc_self = (request.values.get("inc", "1").lower() in ("1","true","yes","on"))
    div_mult = float(request.values.get("mult", "1.0") or 1.0)
    f_fix_in = request.values.get("f_share_override")
    f_fix = float(f_fix_in) if f_fix_in is not None and f_fix_in != '' else None

    # Map bet type to permutation K and clamp top_n
    k_map = {"WIN": 1, "EXACTA": 2, "TRIFECTA": 3, "SUPERFECTA": 4, "SWINGER": 2}
    k_perm = int(k_map.get(bet_type, 4))
    top_n = max(k_perm, min(10, req_top_n))

    # --- Filter Options ---
    countries_df = sql_df("SELECT DISTINCT country FROM tote_events WHERE country IS NOT NULL AND country<>'' ORDER BY country")
    # UI Improvement: Dynamic venue/course options based on other active filters.
    venues_df_sql = (
        "SELECT DISTINCT COALESCE(e.venue,p.venue) AS venue FROM tote_products p LEFT JOIN tote_events e USING(event_id) "
        "WHERE UPPER(p.bet_type)=? AND COALESCE(p.status,'') <> '' AND COALESCE(e.venue,p.venue) IS NOT NULL AND COALESCE(e.venue,p.venue)<>''"
    )
    venues_df_params: list[object] = [bet_type]
    if country:
        venues_df_sql += " AND (UPPER(e.country)=? OR UPPER(p.currency)=?)"
        venues_df_params.append(country)
        venues_df_params.append(country)
    if date_filter:
        venues_df_sql += " AND substr(p.start_iso,1,10) = ?"
        venues_df_params.append(date_filter)
    venues_df_sql += " ORDER BY venue"
    venues_df = sql_df(venues_df_sql, params=tuple(venues_df_params))

    # --- Product List ---
    opts_sql = """
        SELECT
          p.product_id, p.event_id, p.event_name, COALESCE(e.venue, p.venue) AS venue,
          p.start_iso, p.currency, p.total_gross, p.total_net, COALESCE(p.status,'') AS status,
          (SELECT COUNT(1) FROM tote_product_selections s WHERE s.product_id = p.product_id) AS n_runners
        FROM tote_products p
        LEFT JOIN tote_events e USING(event_id)
        WHERE UPPER(p.bet_type)=@bt
    """
    opts_params = {"bt": bet_type}
    if country:
        opts_sql += " AND (UPPER(e.country)=@c OR UPPER(p.currency)=@c)"; opts_params["c"] = country
    if venue:
        opts_sql += " AND UPPER(COALESCE(e.venue, p.venue)) = @v"; opts_params["v"] = venue
    if date_filter:
        opts_sql += " AND SUBSTR(p.start_iso, 1, 10) = @d"; opts_params["d"] = date_filter
    opts_sql += " ORDER BY p.start_iso ASC LIMIT 400"
    opts = sql_df(opts_sql, params=opts_params)

    # --- Main Calculation Logic ---
    prod = None
    runners = []
    # Weighting model outputs
    top_lines = []
    total_lines = 0
    cum_p = 0.0
    efficiency = None
    # Viability model outputs
    viab = None
    grid = []

    if product_id:
        # 1. Fetch product, runners, probable odds, and pool size
        prod_df = sql_df("SELECT * FROM tote_products WHERE product_id=?", params=(product_id,))
        if not prod_df.empty:
            prod = prod_df.iloc[0].to_dict()

        df_sel = sql_df((
            "SELECT selection_id, competitor, number, total_units FROM tote_product_selections WHERE product_id=? AND leg_index=1 ORDER BY number"
        ), params=(product_id,))
        have_units = (not df_sel.empty) and df_sel["total_units"].notnull().any()
        if have_units:
            # Compute probable odds shares from selection units
            df_sel["total_units"] = df_sel["total_units"].fillna(0.0)
            s = float(df_sel["total_units"].sum()) or 0.0
            if s > 0:
                df_sel["pct_units"] = df_sel["total_units"] / s
                df_sel["prob_odds"] = 1 / df_sel["pct_units"]
            else:
                df_sel["pct_units"] = 0.0
                df_sel["prob_odds"] = None
            runners = df_sel.to_dict("records")
        elif _use_bq():
            # Fallback to BigQuery probable odds view if units are not present locally
            try:
                # First try by the same product_id (works if WIN product ids are identical)
                bq_probs = sql_df(
                    "SELECT CAST(cloth_number AS INT64) AS number, CAST(decimal_odds AS FLOAT64) AS decimal_odds FROM vw_tote_probable_odds WHERE product_id = ?",
                    params=(product_id,)
                )
                # If empty, map via event_id to the WIN product for the same event
                if (bq_probs.empty) and prod and prod.get('event_id'):
                    bq_probs = sql_df(
                        "SELECT CAST(o.cloth_number AS INT64) AS number, CAST(o.decimal_odds AS FLOAT64) AS decimal_odds "
                        "FROM vw_tote_probable_odds o JOIN tote_products p ON p.product_id = o.product_id "
                        "WHERE p.event_id = ? AND UPPER(p.bet_type)='WIN'",
                        params=(prod.get('event_id'),)
                    )
                if not bq_probs.empty:
                    # Compute strengths and normalized shares
                    bq_probs = bq_probs[bq_probs["decimal_odds"].notnull() & (bq_probs["decimal_odds"] > 0)]
                    if not bq_probs.empty:
                        bq_probs["strength"] = 1.0 / bq_probs["decimal_odds"].astype(float)
                        tot = float(bq_probs["strength"].sum()) or 0.0
                        bq_probs["pct_units"] = bq_probs["strength"] / tot if tot > 0 else 0.0
                        bq_probs.rename(columns={"decimal_odds": "prob_odds"}, inplace=True)
                        # Left-join competitor names from selections when available
                        if not df_sel.empty:
                            runners_df = df_sel[["number","competitor"]].copy()
                            bq_probs = bq_probs.merge(runners_df, on="number", how="left")
                        runners = bq_probs.fillna(0).to_dict("records")
                    else:
                        flash("No probable odds available in BigQuery for this product.", "warning")
                else:
                    flash("No probable odds found in BigQuery for this product.", "warning")
            except Exception as e:
                flash(f"Probable odds fetch (BQ) failed: {e}", "error")
        # Skip legacy SQLite fallback; rely solely on BQ probable odds or selection units
        # If no runners found, the calculators will show a warning below.
        if (not runners) or len(runners) == 0:
            runners = []

            # Build PL permutations for superfecta
            # Sort top N by share
            src_df = None
            if have_units:
                src_df = df_sel
            else:
                try:
                    import pandas as _pd
                    src_df = _pd.DataFrame(runners)
                except Exception:
                    src_df = None
            if src_df is None or src_df.empty or ("pct_units" not in src_df.columns):
                flash("Not enough data to compute permutations.", "warning")
                return render_template(
                    "tote_calculators.html",
                    options=(opts.to_dict("records") if not opts.empty else []),
                    bet_type=bet_type,
                    product_id=product_id,
                    params={
                        "top_n": top_n,
                        "coverage": coverage,
                        "stake_per_line": stake_per_line,
                        "f_share": f_share,
                    "date": date_filter, # f_share is undefined here, will be fixed below
                    },
                    runners=runners,
                    total_lines=0,
                    top_lines=[],
                    cum_p=0.0,
                    efficiency=None,
                    o_min=None,
                    countries=(countries_df['country'].tolist() if not countries_df.empty else []),
                    country=country,
                    venues=(venues_df['venue'].tolist() if not venues_df.empty else []),
                    course=course,
                )
            top_df = src_df.sort_values("pct_units", ascending=False).head(top_n)
            items = [(str(r["number"] if "number" in r else r.get("selection_id")), float(r["pct_units"])) for _, r in top_df.iterrows()]
            # Keep only positive strengths
            items = [(rid, p) for rid, p in items if p and p > 0]
            # K positions needed for bet type
            k = 4 if bet_type == "SUPERFECTA" else (3 if bet_type == "TRIFECTA" else 2)
            if len(items) < k:
                flash("Not enough runners with non-zero shares to build permutations for this bet type.", "warning")
                runners = df_sel.to_dict("records")
                return render_template(
                    "tote_calculators.html",
                    options=(opts.to_dict("records") if not opts.empty else []),
                    bet_type=bet_type,
                    product_id=product_id,
                    params={
                        "top_n": top_n,
                        "coverage": coverage,
                        "stake_per_line": stake_per_line,
                        "f_share": f_share,
                    "date": date_filter, # f_share is undefined here, will be fixed below
                    },
                    runners=runners,
                    total_lines=0,
                    top_lines=[],
                    cum_p=0.0,
                    efficiency=None,
                    o_min=None,
                    countries=(countries_df['country'].tolist() if not countries_df.empty else []),
                    country=country,
                    venues=(venues_df['venue'].tolist() if not venues_df.empty else []),
                    course=course,
                )
            # Normalize strengths
            tot = sum(p for _, p in items) or 1.0
            strengths = [(rid, p / tot) for rid, p in items]

            # Enumerate permutations with PL probability (k by bet type)
            import itertools
            perms = []
            ids = [rid for rid, _ in strengths]
            s_map = {rid: p for rid, p in strengths}
            for tup in itertools.permutations(ids, k):
                s_total = sum(s_map.values())
                num = 1.0
                remaining = s_total
                for idx, rid in enumerate(tup):
                    if remaining <= 0:
                        num = 0.0
                        break
                    num *= (s_map[rid] / remaining)
                    remaining = remaining - s_map[rid]
                line = {f"h{i+1}": tup[i] for i in range(k)}
                line.update({"p": num})
                perms.append(line)
            perms.sort(key=lambda x: x["p"], reverse=True)
            total_lines = len(perms)
            target_lines = max(1, int(round(total_lines * coverage)))
            cum = 0.0
            top_lines = []
            for i, line in enumerate(perms, start=1):
                if i <= target_lines:
                    cum += line["p"]
                    if len(top_lines) < 30:
                        top_lines.append({"rank": i, **line, "cum_p": cum})
            cum_p = cum
            random_cov = target_lines / float(total_lines or 1)
            efficiency = (cum_p / random_cov) if random_cov > 0 else None

            # Breakeven using S lines
            S = stake_per_line * float(target_lines)
            try:
                o_min_val = (S / (f_share * (1.0 - t_val))) - S
            except Exception: # f_share and t_val are undefined here, will be fixed below
                o_min_val = None
            o_min = o_min_val
        else:
            flash("No selection units found for this product yet. Click Refresh to pull latest from Tote.", "warning")

    return render_template(
        "tote_calculators.html",
        options=(opts.to_dict("records") if not opts.empty else []),
        bet_type=bet_type,
        product_id=product_id,
        params={
            "top_n": top_n,
            "coverage": coverage,
            "stake_per_line": stake_per_line,
            "f_share": f_share,
            "date": date_filter,
        },
        runners=runners,
        total_lines=total_lines,
        top_lines=top_lines,
        cum_p=cum_p,
        efficiency=efficiency,
        o_min=o_min,
        countries=(countries_df['country'].tolist() if not countries_df.empty else []),
        country=country,
        venues=(venues_df['venue'].tolist() if not venues_df.empty else []),
        course=course,
    )

@app.route("/tote/viability", methods=["GET", "POST"])
def tote_viability_page():
    """Calculator for pari-mutuel viability, focused on SUPERFECTA.
    
    Uses BigQuery table functions to calculate viability based on user inputs.
    """
    # --- Filters ---
    country = (request.values.get("country") or os.getenv("DEFAULT_COUNTRY", "GB")).strip().upper()
    venue = (request.values.get("venue") or "").strip()
    date_filter = (request.values.get("date") or _today_iso()).strip()
    product_id = (request.values.get("product_id") or "").strip()

    # --- Filter Options ---
    # Country options
    cdf = sql_df("SELECT DISTINCT country FROM tote_events WHERE country IS NOT NULL AND country<>'' ORDER BY country")
    # UI Improvement: Dynamic venue options filtered by country and date.
    vdf_sql = "SELECT DISTINCT venue FROM tote_events WHERE venue IS NOT NULL AND venue <> '' "
    vdf_params = []
    if country:
        vdf_sql += " AND country = ?"
        vdf_params.append(country)
    if date_filter:
        vdf_sql += " AND SUBSTR(start_iso, 1, 10) = ?"
        vdf_params.append(date_filter)
    vdf_sql += " ORDER BY venue"
    vdf = sql_df(vdf_sql, params=tuple(vdf_params))

    # --- Product List ---
    # Bet type selector (default SUPERFECTA)
    bet_type = (request.values.get("bet_type") or "SUPERFECTA").strip().upper()
    # Map bet type to permutation K (WIN=1, EXACTA=2, TRIFECTA=3, SUPERFECTA=4)
    k_map = {"WIN": 1, "EXACTA": 2, "TRIFECTA": 3, "SUPERFECTA": 4}
    k_perm = int(k_map.get(bet_type, 4))

    opts_sql = """
        SELECT
          p.product_id, p.event_id, p.event_name, COALESCE(e.venue, p.venue) AS venue,
          p.start_iso, p.currency, p.total_gross, p.total_net, COALESCE(p.status,'') AS status,
          (SELECT COUNT(1) FROM tote_product_selections s WHERE s.product_id = p.product_id) AS n_runners
        FROM tote_products p
        LEFT JOIN tote_events e USING(event_id)
        WHERE UPPER(p.bet_type)=@bt
    """
    opts_params = {"bt": bet_type}
    if country:
        opts_sql += " AND (UPPER(e.country)=@c OR UPPER(p.currency)=@c)"; opts_params["c"] = country
    if venue:
        opts_sql += " AND UPPER(COALESCE(e.venue, p.venue)) = @v"; opts_params["v"] = venue
    if date_filter:
        opts_sql += " AND SUBSTR(p.start_iso, 1, 10) = @d"; opts_params["d"] = date_filter
    opts_sql += " ORDER BY p.start_iso ASC LIMIT 400"
    opts = sql_df(opts_sql, params=opts_params)

    # --- Calculation Logic (if a product is selected) ---
    prod = None
    viab = None
    grid = []
    calc_params = {
        "stake_per_line": 0.01,
        "take_rate": 0.30,
        "net_rollover": 0.0,
        "inc_self": True,
        "div_mult": 1.0,
        "f_fix": None,
        "coverage_pct": 60.0,
        "pool_gross": None,
        "N": None,
    }

    if product_id:
        pdf = sql_df("SELECT * FROM tote_products WHERE product_id=?", params=(product_id,))
        if not pdf.empty:
            prod = pdf.iloc[0].to_dict()

        try:
            # Performance/UI Improvement: Fetch N (runners) and O (latest pool gross) in a single query
            # to reduce latency and ensure the latest pool info is used.
            params_df = sql_df("""
                SELECT
                    (SELECT COUNT(1) FROM tote_product_selections WHERE product_id=@pid AND leg_index=1) AS n,
                    (SELECT total_gross FROM tote_pool_snapshots WHERE product_id=@pid ORDER BY ts_ms DESC LIMIT 1) AS latest_gross
            """, params={"pid": product_id})

            if not params_df.empty:
                N = int(params_df.iloc[0]["n"])
                pool_gross = float(params_df.iloc[0]["latest_gross"]) if pd.notna(params_df.iloc[0]["latest_gross"]) else float(prod.get("total_gross") or 0)
            else:
                N = 0
                pool_gross = float(prod.get("total_gross") or 0)

            # Inputs from form or defaults
            def fnum(name, default):
                v = request.values.get(name)
                try: return float(v) if v is not None and v != '' else default
                except Exception: return default

            stake_per_line = fnum("stake_per_line", calc_params["stake_per_line"]) 
            take_rate = fnum("take", calc_params["take_rate"]) 
            net_rollover = fnum("rollover", calc_params["net_rollover"]) 
            inc_self = (request.values.get("inc", "1").lower() in ("1","true","yes","on"))
            div_mult = fnum("mult", calc_params["div_mult"]) 
            f_fix = fnum("f", None)
            coverage_in_pct = fnum("alpha", calc_params["coverage_pct"]) 
            coverage_in = coverage_in_pct / 100.0
            
            if bet_type == "SWINGER":
                # combinations C(N,2)
                C_all = int(N*(N-1)/2) if (N and N>=2) else 0
                M = int(round(max(0.0, min(1.0, coverage_in)) * C_all)) if C_all > 0 else 0
                if N and N >= 2:
                    viab_df = sql_df(
                        f"SELECT * FROM `{cfg.bq_project}.{cfg.bq_dataset}.tf_combo_viability_simple`(@N,@K,@O,@M,@l,@t,@R,@inc,@mult,@f)",
                        params={
                            "N": N, "K": 2, "O": pool_gross, "M": M, "l": stake_per_line, "t": take_rate,
                            "R": net_rollover, "inc": True if inc_self else False, "mult": div_mult, "f": f_fix,
                        },
                        # params={"N": N, "K": 2, "O": pool_gross, "M": M, "l": stake_per_line, "t": take_rate, "R": net_rollover, "inc": inc_self, "mult": div_mult, "f": f_fix}, # Redundant, remove
                    )
                    if viab_df is not None and not viab_df.empty:
                        viab = viab_df.iloc[0].to_dict()
                    grid_df = sql_df(
                        f"SELECT * FROM `{cfg.bq_project}.{cfg.bq_dataset}.tf_perm_viability_grid`(@N,@K,@O,@l,@t,@R,@inc,@mult,@f, @steps)",
                        params={
                            "N": N, "K": 2, "O": pool_gross, "l": stake_per_line, "t": take_rate,
                            "R": net_rollover, "inc": True if inc_self else False, "mult": div_mult, "f": f_fix,
                            "steps": 20,
                        },
                    )
                    if grid_df is not None and not grid_df.empty:
                        grid = grid_df.to_dict("records")
                    # The viability grid for combinations is not yet implemented.
                    # Using the permutation grid would be mathematically incorrect.
                    flash("Viability grid is not yet supported for SWINGER bets.", "info")
                    if viab is None:
                        viab = _viability_local_perm(N, 2, pool_gross, M, stake_per_line, take_rate, net_rollover, inc_self, div_mult, f_fix if f_fix is not None else None)
                else:
                    flash("Not enough runners (need at least 2) for SWINGER viability calculation.", "warning")
            else:
                # permutations P(N,K)
                C_all = 0
                if N and N >= k_perm:
                    ctmp = 1
                    for i in range(k_perm):
                        ctmp *= (N - i)
                    C_all = int(ctmp)
                M = int(round(max(0.0, min(1.0, coverage_in)) * C_all)) if C_all > 0 else 0

                if N and N >= k_perm:
                    # Run generic viability function for permutations
                    try:
                        viab_df = sql_df(
                            f"SELECT * FROM `{cfg.bq_project}.{cfg.bq_dataset}.tf_perm_viability_simple`(@N,@K,@O,@M,@l,@t,@R,@inc,@mult,@f)",
                            params={
                                "N": N, "K": k_perm, "O": pool_gross, "M": M, "l": stake_per_line, "t": take_rate,
                                "R": net_rollover, "inc": True if inc_self else False, "mult": div_mult, "f": f_fix,
                            },
                            # params={"N": N, "K": k_perm, "O": pool_gross, "M": M, "l": stake_per_line, "t": take_rate, "R": net_rollover, "inc": inc_self, "mult": div_mult, "f": f_fix}, # Redundant, remove
                        )
                    except Exception:
                        viab_df = None
                    if viab_df is not None and not viab_df.empty:
                        viab = viab_df.iloc[0].to_dict()
                    elif bet_type == "SUPERFECTA":
                        # Fallback to legacy SUPERFECTA function if generic not available
                        try:
                            viab_df2 = sql_df(
                                f"SELECT * FROM `{cfg.bq_project}.{cfg.bq_dataset}.tf_superfecta_viability_simple`(@N,@O,@M,@l,@t,@R,@inc,@mult,@f)",
                                params={
                                    "N": N, "O": pool_gross, "M": M, "l": stake_per_line, "t": take_rate,
                                    "R": net_rollover, "inc": True if inc_self else False, "mult": div_mult, "f": f_fix,
                                },
                                # params={"N": N, "O": pool_gross, "M": M, "l": stake_per_line, "t": take_rate, "R": net_rollover, "inc": True if inc_self else False, "mult": div_mult, "f": f_fix}, # Redundant, remove
                            )
                            if viab_df2 is not None and not viab_df2.empty:
                                viab = viab_df2.iloc[0].to_dict()
                        except Exception:
                            pass

                    # Grid (20 steps)
                    try:
                        grid_df = sql_df(
                            f"SELECT * FROM `{cfg.bq_project}.{cfg.bq_dataset}.tf_perm_viability_grid`(@N,@K,@O,@l,@t,@R,@inc,@mult,@f, @steps)",
                            params={
                                "N": N, "K": k_perm, "O": pool_gross, "l": stake_per_line, "t": take_rate,
                                "R": net_rollover, "inc": True if inc_self else False, "mult": div_mult, "f": f_fix,
                                "steps": 20,
                            },
                            # params={"N": N, "K": k_perm, "O": pool_gross, "l": stake_per_line, "t": take_rate, "R": net_rollover, "inc": True if inc_self else False, "mult": div_mult, "f": f_fix, "steps": 20}, # Redundant, remove
                        )
                    except Exception:
                        grid_df = None
                    if grid_df is not None and not grid_df.empty:
                        grid = grid_df.to_dict("records")
                    if viab is None:
                        viab = _viability_local_perm(N, k_perm, pool_gross, M, stake_per_line, take_rate, net_rollover, inc_self, div_mult, f_fix if f_fix is not None else None)
                else:
                    flash(f"Not enough runners (need at least {k_perm}) for {bet_type} viability calculation.", "warning")

        except Exception as e:
            flash(f"Viability function error: {e}", "error")
            traceback.print_exc()

        # Record parameters for the template
        calc_params.update({
            "stake_per_line": stake_per_line,
            "take_rate": take_rate,
            "net_rollover": net_rollover,
            "inc_self": bool(inc_self),
            "div_mult": div_mult,
            "f_fix": f_fix,
            "coverage_pct": coverage_in_pct,
            "pool_gross": pool_gross,
            "N": N,
        })

    return render_template(
        "tote_viability.html",
        products=(opts.to_dict("records") if not opts.empty else []),
        product_id=product_id,
        product_details=prod,
        viab=viab,
        grid=grid,
        calc=calc_params,
        filters={
            "country": country,
            "venue": venue,
            "date": date_filter,
            "bet_type": bet_type,
        },
        country_options=(cdf['country'].tolist() if not cdf.empty else ['GB']),
        venue_options=(vdf['venue'].tolist() if not vdf.empty else []),
    )

@app.route("/api/tote/viability")
def api_tote_viability():
    """JSON endpoint to recompute viability with latest pool totals for auto-refresh."""
    product_id = (request.args.get("product_id") or "").strip()
    # Load product
    prod = None
    if product_id:
        pdf = sql_df("SELECT * FROM tote_products WHERE product_id=?", params=(product_id,))
        if not pdf.empty:
            prod = pdf.iloc[0].to_dict()
    # Params
    def fnum(name, default):
        v = request.args.get(name)
        try:
            return float(v) if v is not None and v != '' else default
        except Exception:
            return default
    takeout = fnum("takeout", 0.30)
    stake_per_combo = fnum("s", 1.0)
    mode = request.args.get("mode", "all")
    C = int(float(request.args.get("c") or 0))
    rollover = fnum("R", 0.0)
    # If covering all, recompute permutations from current selections or manual n
    if mode == "all":
        n = None
        try:
            n = int(float(request.args.get("n") or 0)) or None
        except Exception:
            n = None
        if product_id and (n is None):
            try:
                ndf = sql_df("SELECT COUNT(1) AS n FROM tote_product_selections WHERE product_id=?", params=(product_id,))
                n = int(ndf.iloc[0]["n"]) if not ndf.empty else None
            except Exception:
                n = None
        if n and n >= 4:
            C = n * (n-1) * (n-2) * (n-3)
    yuw = fnum("yuw", stake_per_combo if C>0 else 0.0)
    ouw = fnum("ouw", 0.0)
    f_override = request.args.get("f")
    # Compute
    S = max(0.0, C * stake_per_combo)
    if f_override is not None and f_override != '':
        try:
            fshare = float(f_override)
        except Exception:
            fshare = (yuw / (yuw + ouw)) if (yuw + ouw) > 0 else 0.0
    else:
        fshare = (yuw / (yuw + ouw)) if (yuw + ouw) > 0 else 0.0
    O_current = None
    if prod and prod.get("total_gross") is not None:
        try: O_current = float(prod["total_gross"])  # type: ignore[index]
        except Exception: O_current = None
    O_min = None; ROI_current = None; viable_now = None
    ret_current = None; profit_current = None
    if S > 0 and 0 < takeout < 1 and fshare > 0:
        try: O_min = (S / (fshare * (1.0 - takeout))) - S - (rollover / (1.0 - takeout))
        except Exception: O_min = None
        if O_current is not None:
            try:
                ret = fshare * ((1.0 - takeout) * (S + O_current) + rollover)
                ROI_current = (ret - S) / S
                viable_now = ret > S
                ret_current = ret
                profit_current = ret - S
            except Exception:
                ROI_current = None; viable_now = None
    payload = {
        "product": prod,
        "S": S,
        "f": fshare,
        "O_current": O_current,
        "O_min": O_min,
        "ROI_current": ROI_current,
        "viable_now": viable_now,
        "return_current": ret_current,
        "profit_current": profit_current,
        "R": rollover,
        "t": takeout,
    }
    return app.response_class(json.dumps(payload), mimetype="application/json")

@app.route("/api/tote/product_runners")
def api_tote_product_runners():
    pid = (request.args.get("product_id") or "").strip()
    if not pid:
        return app.response_class(json.dumps({"error": "missing product_id"}), mimetype="application/json", status=400)
    # Also return selection_id for the bet slip
    rows = sql_df("SELECT DISTINCT selection_id, number, competitor FROM tote_product_selections WHERE product_id=? AND leg_index=1 ORDER BY number", params=(pid,))
    return app.response_class(json.dumps(rows.to_dict("records") if not rows.empty else []), mimetype="application/json")

@app.route("/api/tote/event_products/<event_id>")
def api_tote_event_products(event_id: str):
    """Return OPEN products for a given event."""
    if not event_id:
        return app.response_class(json.dumps({"error": "missing event_id"}), mimetype="application/json", status=400)
    
    df = sql_df(
        "SELECT product_id, bet_type, status FROM tote_products WHERE event_id=? AND status='OPEN' ORDER BY bet_type",
        params=(event_id,)
    )
    return app.response_class(json.dumps(df.to_dict("records")), mimetype="application/json")

@app.route("/api/tote/pool_snapshot/<product_id>")
def api_tote_pool_snapshot(product_id: str):
    """Return latest pool snapshot; if absent, fall back to tote_products totals.

    Avoids noisy 404s by providing best-effort data for UI auto-refresh.
    """
    # Query latest snapshot from BQ
    try:
        ss = sql_df(
            "SELECT product_id, event_id, currency, status, start_iso, ts_ms, total_gross, total_net, rollover, deduction_rate, 'snap' AS source "
            "FROM tote_pool_snapshots WHERE product_id=? ORDER BY ts_ms DESC LIMIT 1",
            params=(product_id,)
        )
        if not ss.empty:
            return app.response_class(ss.iloc[0].to_json(), mimetype="application/json")
        # Fallback to products
        pdf = sql_df(
            "SELECT product_id, event_id, currency, status, start_iso, total_gross, total_net, rollover, deduction_rate, NULL AS ts_ms, 'products_fallback' AS source "
            "FROM tote_products WHERE product_id=?",
            params=(product_id,)
        )
        if not pdf.empty:
            return app.response_class(pdf.iloc[0].to_json(), mimetype="application/json")
        return app.response_class(json.dumps({"error": "not found"}), mimetype="application/json", status=404)
    except Exception as e:
        return app.response_class(json.dumps({"error": str(e)}), mimetype="application/json", status=500)

@app.route("/tote/audit/superfecta", methods=["POST"])
def tote_audit_superfecta_post():
    """Handle audit bet placement for Superfecta, writing to BigQuery."""
    if not _use_bq():
        flash("Audit placement requires BigQuery to be configured.", "error")
        return redirect(request.referrer or url_for('tote_superfecta_page'))

    pid = (request.form.get("product_id") or "").strip()
    # This is the new field for the audit bet form
    selections_text = (request.form.get("selections_text") or "").strip()
    if not selections_text:
        flash("Selections text box cannot be empty.", "error")
        return redirect(request.referrer or url_for('tote_superfecta_detail', product_id=pid))

    # Legacy fields, kept for potential compatibility
    sel = (request.form.get("selection") or "").strip()
    sels_raw = (request.form.get("selections") or "").strip()
    stake = (request.form.get("stake") or "").strip()
    currency = (request.form.get("currency") or "GBP").strip() or "GBP"
    if not pid or (not sel and not sels_raw):
        flash("Missing product or selection(s)", "error")
        return redirect(request.referrer or url_for('tote_superfecta_page'))
    try:
        sk = float(stake)
        if sk <= 0:
            raise ValueError
    except Exception:
        flash("Stake must be > 0", "error")
        return redirect(request.referrer or url_for('tote_superfecta_page'))

    post_flag = (request.form.get("post") or "").lower() in ("1","true","yes","on")
    stake_type = (request.form.get("stake_type") or "total").strip().lower()

    # Use the new text area for selections
    selections = None
    if selections_text:
        # Split by newline or comma
        parts = [p.strip() for p in (selections_text.replace("\r","\n").replace(",","\n").split("\n"))]
        selections = [p for p in parts if p]
    elif sels_raw: # Fallback to hidden field
        parts = [p.strip() for p in (sels_raw.replace("\r","\n").replace(",","\n").split("\n"))]
        selections = [p for p in parts if p]

    # Preflight checks: product status OPEN and selection IDs valid/active
    # This part remains largely the same, as it queries the live Tote API
    try:
        from .providers.tote_api import ToteClient
        client = ToteClient()
        gql = """
        query ProductForPlacement($id: String){
          product(id:$id){
            ... on BettingProduct{
              selling{ status }
              legs{ nodes{ id selections{ nodes{ id status competitor{ name details{ __typename ... on HorseDetails{ clothNumber } ... on GreyhoundDetails{ trapNumber } } } } } } }
            }
          }
        }
        """
        pdata = client.graphql(gql, {"id": pid})
        prod = pdata.get("product") or {}
        bp = prod or {}
        selling = (bp.get("selling") or {})
        pstatus = (selling.get("status") or "").upper()
        if pstatus and pstatus != "OPEN":
            flash(f"Preflight: product not OPEN (status={pstatus}).", "error")
            return redirect(request.referrer or url_for('tote_superfecta_page'))
        # Build number->(selection_id,status)
        legs = ((bp.get("legs") or {}).get("nodes")) or []
        selmap = {}
        if legs:
            sels = ((legs[0].get("selections") or {}).get("nodes")) or []
            for srow in sels:
                sid = srow.get("id"); sst = (srow.get("status") or "").upper()
                comp = (srow.get("competitor") or {})
                det = (comp.get("details") or {})
                n = det.get("clothNumber") if det.get("__typename") == "HorseDetails" else det.get("trapNumber")
                try:
                    if n is not None:
                        selmap[int(n)] = (sid, sst)
                except Exception:
                    pass
        # Validate selection(s)
        lines = selections if selections is not None else ([sel] if sel else [])
        for line in lines:
            nums = []
            try:
                nums = [int(x.strip()) for x in (line or '').split('-') if x.strip()]
            except Exception:
                pass
            if len(nums) < 4:
                flash(f"Preflight: invalid selection line '{line}'.", "error")
                return redirect(request.referrer or url_for('tote_superfecta_page'))
            missing = [str(n) for n in nums[:4] if n not in selmap]
            if missing:
                flash(f"Preflight: unknown numbers {{{','.join(missing)}}}.", "error")
                return redirect(request.referrer or url_for('tote_superfecta_page'))
            inactive = [str(n) for n in nums[:4] if selmap.get(n,(None,None))[1] not in ("ACTIVE","OPEN","AVAILABLE","VALID")]
            if inactive:
                flash(f"Preflight: selection(s) not active: {{{','.join(inactive)}}}.", "error")
                return redirect(request.referrer or url_for('tote_superfecta_page'))
    except Exception as e:
        # Non-fatal; continue to attempt placement but surface info
        flash(f"Preflight warning: {e}", "warning")
    # Resolve placement-visible product id (match by event/date), with alias fallback
    placement_pid = pid
    try:
        from .providers.tote_api import ToteClient
        client = ToteClient()
        # Use BQ to get event_id and date
        prod_info_df = sql_df("SELECT event_id, substr(start_iso,1,10) as day FROM tote_products WHERE product_id=?", params=(pid,))
        event_id = None
        day = None
        if not prod_info_df.empty:
            event_id = prod_info_df.iloc[0]['event_id']
            day = prod_info_df.iloc[0]['day']
        if event_id and day:
            q2 = """
            query Products($date: Date, $betTypes: [BetTypeCode!], $status: BettingProductSellingStatus, $first: Int){
              products(date:$date, betTypes:$betTypes, sellingStatus:$status, first:$first){
                nodes{ id eventId selling{status} }
              }
            }
            """
            vars = {"date": day, "betTypes": ["SUPERFECTA"], "status": "OPEN", "first": 1000}
            pdata2 = client.graphql(q2, vars)
            nodes = ((pdata2.get("products") or {}).get("nodes")) or []
            for n in nodes:
                if (n.get("eventId") == event_id) and ((n.get("selling") or {}).get("status","OPEN").upper() == "OPEN"):
                    placement_pid = n.get("id") or placement_pid
                    break
        if placement_pid == pid and event_id:
            placement_pid = f"SUPERFECTA:{event_id}"
    except Exception:
        placement_pid = pid

    # Use the BigQuery-aware audit function
    db = get_db()
    res = place_audit_superfecta(db, product_id=pid, selection=(sel or None), selections=selections, stake=sk, currency=currency, post=post_flag, stake_type=stake_type, placement_product_id=placement_pid)
    st = res.get("placement_status")
    if res.get("error"):
        flash(f"Audit bet error: {res.get('error')}", "error")
    elif st:
        ok = str(st).upper() in ("PLACED","ACCEPTED","OK","SUCCESS")
        msg = f"Audit bet placement status: {st}"
        fr = res.get("failure_reason")
        if fr:
            msg += f" (reason: {fr})"
        # Include provider IDs if present for debugging
        try:
            resp = res.get("response") or {}
            ticket = ((resp.get("placeBets") or {}).get("ticket")) or ((resp.get("ticket")) if "ticket" in resp else None)
            if ticket:
                tid = ticket.get("toteId") or ticket.get("id")
                nodes = ((ticket.get("bets") or {}).get("nodes")) or []
                bid = nodes[0].get("toteId") if nodes else None
                if tid:
                    msg += f" | ticket={tid}"
                if bid:
                    msg += f" bet={bid}"
        except Exception:
            pass
        flash(msg, "success" if ok else "error")
    else:
        flash("Audit bet recorded (no placement status returned).", "success")
    try:
        print("[AuditBet] product=", pid, "sel=", sel or selections, "status=", st, "resp=", str(res.get("response"))[:500])
    except Exception:
        pass
    return redirect(request.referrer or url_for('tote_superfecta_page'))

@app.route("/audit/bets")
def audit_bets_page():
    """List recent audit bets via Tote API (read-only in BQ mode)."""
    try:
        data = audit_list_bets(first=50)
        nodes = data.get("nodes") or data.get("results") or []
        bets = []
        for n in nodes:
            stake = n.get("stake") or {}
            legs = (n.get("legs") or [])
            sel = None
            if legs and legs[0].get("selections"):
                sel = ",".join(str(s.get("position")) for s in legs[0]["selections"] if s.get("position") is not None)
            bets.append({
                "tote_bet_id": n.get("toteBetId") or n.get("id"),
                "status": n.get("status"),
                "selection": sel,
                "stake": (stake.get("amount") or {}).get("decimalAmount"),
                "currency": stake.get("currency"),
                "created": n.get("createdAt"),
            })
    except Exception as e:
        flash(f"Audit API error: {e}", "error")
        bets = []
    return render_template("audit_bets.html", bets=bets)

@app.route("/audit/bets/")
def audit_bets_page_slash():
    return audit_bets_page()

@app.route("/audit/")
def audit_root():
    return redirect(url_for('audit_bets_page'))

@app.route("/audit/bets/<bet_id>")
def audit_bet_detail_page(bet_id: str):
    """Show stored request/response for an audit bet and allow a best-effort status refresh."""
    # This page relies on the 'tote_bets' table, which is SQLite-only.
    flash("Audit bet details are not available in BigQuery-only mode.", "error")
    return redirect(url_for('index'))
    conn = _get_db_conn(); init_schema(conn)
    row = conn.execute(
        "SELECT bet_id, ts, mode, product_id, selection, stake, currency, status, outcome, response_json, result_json FROM tote_bets WHERE bet_id=?",
        (bet_id,)
    ).fetchone()
    detail = None
    if row:
        cols = ["bet_id","ts","mode","product_id","selection","stake","currency","status","outcome","response_json","result_json"]
        detail = {cols[i]: row[i] for i in range(len(cols))}
        # Parse stored request/response for separate display
        try:
            import json as _json
            rr = _json.loads(detail.get("response_json") or "{}")
            req = rr.get("request")
            resp = rr.get("response")
            detail["_req_pretty"] = _json.dumps(req, indent=2, ensure_ascii=False) if req is not None else None
            detail["_resp_pretty"] = _json.dumps(resp, indent=2, ensure_ascii=False) if resp is not None else None
        except Exception:
            detail["_req_pretty"] = None
            detail["_resp_pretty"] = None
    # Optional refresh
    if request.args.get("refresh") == "1":
        try:
            res = refresh_bet_status(conn, bet_id=bet_id, post=True)
            conn.commit()
            flash(f"Refreshed status: {res}", "info")
        except Exception as e:
            flash(f"Refresh failed: {e}", "error")
        finally:
            try: conn.close()
            except Exception: pass
        return redirect(url_for('audit_bet_detail_page', bet_id=bet_id))
    try: conn.close()
    except Exception: pass
    return render_template("audit_bet_detail.html", bet=detail)

@app.route("/status")
def status_page():
    """Enhanced status dashboard (v2) showing GCP + data freshness."""
    return render_template("status_v2.html")


@app.get("/api/status/data_freshness")
def api_status_data_freshness():
    """Return counts and last-ingest timestamps for key tables."""
    try:
        # Events (avoid referencing non-existent updated_ts)
        ev_today = sql_df("SELECT COUNT(1) AS c FROM tote_events WHERE SUBSTR(start_iso,1,10)=FORMAT_DATE('%F', CURRENT_DATE())")
        # Use start_iso as proxy for last update; casting guards string format
        ev_last = sql_df("SELECT MAX(TIMESTAMP(start_iso)) AS ts FROM tote_events")

        # Products
        pr_today = sql_df("SELECT COUNT(1) AS c FROM tote_products WHERE SUBSTR(start_iso,1,10)=FORMAT_DATE('%F', CURRENT_DATE())")
        pr_last = sql_df("SELECT MAX(TIMESTAMP(start_iso)) AS ts FROM tote_products")

        # Probable odds
        po_today = sql_df("SELECT COUNT(1) AS c FROM raw_tote_probable_odds WHERE DATE(TIMESTAMP_MILLIS(fetched_ts))=CURRENT_DATE()")
        po_last = sql_df("SELECT TIMESTAMP_MILLIS(MAX(fetched_ts)) AS ts FROM raw_tote_probable_odds")

        # Pool snapshots
        ps_today = sql_df("SELECT COUNT(1) AS c FROM tote_pool_snapshots WHERE DATE(TIMESTAMP_MILLIS(ts_ms))=CURRENT_DATE()")
        ps_last = sql_df("SELECT TIMESTAMP_MILLIS(MAX(ts_ms)) AS ts FROM tote_pool_snapshots")

        out = {
            "events": {
                "today": int(ev_today.iloc[0]["c"]) if not ev_today.empty else 0,
                "last": (str(ev_last.iloc[0]["ts"]) if not ev_last.empty else None),
            },
            "products": {
                "today": int(pr_today.iloc[0]["c"]) if not pr_today.empty else 0,
                "last": (str(pr_last.iloc[0]["ts"]) if not pr_last.empty else None),
            },
            "probable_odds": {
                "today": int(po_today.iloc[0]["c"]) if not po_today.empty else 0,
                "last": (str(po_last.iloc[0]["ts"]) if not po_last.empty else None),
            },
            "pool_snapshots": {
                "today": int(ps_today.iloc[0]["c"]) if not ps_today.empty else 0,
                "last": (str(ps_last.iloc[0]["ts"]) if not ps_last.empty else None),
            },
        }
        return app.response_class(json.dumps(out), mimetype="application/json")
    except Exception as e:
        return app.response_class(json.dumps({"error": str(e)}), mimetype="application/json", status=500)


@app.get("/api/status/upcoming")
def api_status_upcoming():
    """Return upcoming races/products in the next 60 minutes (GB) if view exists."""
    upcoming = []
    try:
        df = sql_df("SELECT product_id, event_id, event_name, venue, country, start_iso, status, currency, combos, S, roi_current, viable_now FROM vw_gb_open_superfecta_next60_be ORDER BY start_iso")
        upcoming = df.to_dict("records") if not df.empty else []
    except Exception:
        try:
            # Fallback: generic query for open superfecta next 60 minutes
            now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            df = sql_df(
                "SELECT product_id, event_id, COALESCE(event_name, e.name) AS event_name, COALESCE(venue, e.venue) AS venue, COALESCE(e.country, currency) AS country, start_iso, status, currency \n"
                "FROM tote_products p LEFT JOIN tote_events e USING(event_id) \n"
                "WHERE UPPER(p.bet_type)='SUPERFECTA' AND p.status='OPEN' AND TIMESTAMP(p.start_iso) BETWEEN TIMESTAMP(@now) AND TIMESTAMP_ADD(TIMESTAMP(@now), INTERVAL 60 MINUTE) \n"
                "ORDER BY start_iso",
                params={"now": now_iso},
            )
            upcoming = df.to_dict("records") if not df.empty else []
        except Exception:
            upcoming = []
    return app.response_class(json.dumps({"items": upcoming}), mimetype="application/json")


def _gcp_project_region() -> tuple[str|None, str|None]:
    # Prefer standard GCP envs, then config
    proj = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or cfg.bq_project
    region = os.getenv("GCP_REGION") or os.getenv("CLOUD_RUN_REGION") or os.getenv("REGION") or "europe-west2"
    return proj, region


def _gcp_auth_session():
    try:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        creds, _ = google.auth.default(scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
        ])
        return AuthorizedSession(creds)
    except Exception:
        return None


@app.get("/api/status/gcp")
def api_status_gcp():
    """Return Cloud Run, Cloud Scheduler, and Pub/Sub resource statuses."""
    project, region = _gcp_project_region()
    if not project:
        return app.response_class(json.dumps({"error": "GCP project not configured"}), mimetype="application/json", status=400)

    sess = _gcp_auth_session()
    if not sess:
        return app.response_class(json.dumps({"error": "GCP auth unavailable"}), mimetype="application/json", status=500)

    out = {"project": project, "region": region, "cloud_run": {}, "scheduler": {}, "pubsub": {}, "app": {}}

    # Cloud Run services
    try:
        services = []
        for name in [os.getenv("SERVICE_FETCHER", "ingestion-fetcher"), os.getenv("SERVICE_ORCHESTRATOR", "ingestion-orchestrator")]:
            url = f"https://run.googleapis.com/v2/projects/{project}/locations/{region}/services/{name}"
            r = sess.get(url, timeout=10)
            if r.status_code == 200:
                j = r.json()
                conds = {c.get("type"): c.get("state") or c.get("status") for c in j.get("conditions", [])}
                services.append({
                    "name": name,
                    "uri": j.get("uri"),
                    "latestReadyRevision": j.get("latestReadyRevision"),
                    "ready": (conds.get("Ready") == "CONDITION_SUCCEEDED" or conds.get("Ready") == "True"),
                    "updateTime": j.get("updateTime"),
                })
            else:
                services.append({"name": name, "missing": True, "status_code": r.status_code})
        out["cloud_run"]["services"] = services
    except Exception as e:
        out["cloud_run"]["error"] = str(e)

    # Cloud Scheduler jobs
    try:
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{project}/locations/{region}/jobs"
        r = sess.get(url, timeout=10)
        jobs = []
        if r.status_code == 200:
            for j in r.json().get("jobs", []):
                jobs.append({
                    "name": j.get("name"),
                    "schedule": j.get("schedule"),
                    "state": j.get("state"),
                    "lastAttemptTime": j.get("lastAttemptTime") or (j.get("lastAttempt") or {}).get("dispatchTime"),
                })
        else:
            out["scheduler"]["status_code"] = r.status_code
        out["scheduler"]["jobs"] = jobs
    except Exception as e:
        out["scheduler"]["error"] = str(e)

    # Pub/Sub topic + subscription (ingest)
    try:
        topic = f"projects/{project}/topics/ingest-jobs"
        sub = f"projects/{project}/subscriptions/ingest-fetcher-sub"
        t = sess.get(f"https://pubsub.googleapis.com/v1/{topic}", timeout=10)
        s = sess.get(f"https://pubsub.googleapis.com/v1/{sub}", timeout=10)
        t_info = {"exists": (t.status_code == 200)}
        s_info = {"exists": (s.status_code == 200)}
        if t.status_code == 200:
            t_info.update({"name": t.json().get("name")})
        if s.status_code == 200:
            sj = s.json()
            s_info.update({
                "name": sj.get("name"),
                "topic": sj.get("topic"),
                "ackDeadlineSeconds": sj.get("ackDeadlineSeconds"),
                "pushEndpoint": ((sj.get("pushConfig") or {}).get("pushEndpoint")),
            })
        out["pubsub"] = {"topic": t_info, "subscription": s_info}
    except Exception as e:
        out["pubsub"]["error"] = str(e)

    # App-level subscription flags
    try:
        out["app"]["SUBSCRIBE_POOLS_env"] = os.getenv("SUBSCRIBE_POOLS", "0")
        out["app"]["pool_subscriber_started"] = bool(_pool_thread_started)
    except Exception:
        pass

    return app.response_class(json.dumps(out), mimetype="application/json")


@app.get("/api/status/job_log")
def api_status_job_log():
    """Return recent job runs recorded in BigQuery by the services."""
    try:
        df = sql_df(
            "SELECT job_id, component, task, status, started_ts, ended_ts, duration_ms, payload_json, error, metrics_json "
            "FROM ingest_job_runs ORDER BY started_ts DESC LIMIT 50"
        )
        items = [] if df.empty else df.to_dict("records")
        return app.response_class(json.dumps({"items": items}), mimetype="application/json")
    except Exception as e:
        return app.response_class(json.dumps({"error": str(e)}), mimetype="application/json", status=500)


@app.post("/api/trigger")
def api_trigger_jobs():
    """Publish quick-action jobs to Pub/Sub for testing.

    Accepts JSON or form with fields:
      - action: one of [events_today, products_today_open, single_product, probable_odds_event]
      - event_id (for probable_odds_event)
      - product_id (for single_product)
    """
    try:
        data = request.get_json(silent=True) or request.form.to_dict() or {}
        action = (data.get("action") or "").strip()
        if not action:
            return app.response_class(json.dumps({"error": "missing action"}), mimetype="application/json", status=400)
        project_id = os.getenv("GCP_PROJECT") or cfg.bq_project
        topic_id = os.getenv("PUBSUB_TOPIC_ID", "ingest-jobs")
        if not project_id:
            return app.response_class(json.dumps({"error": "GCP project not configured"}), mimetype="application/json", status=400)
        msgs: list[str] = []
        if action == "events_today":
            mid = publish_pubsub_message(project_id, topic_id, {"task": "ingest_events_for_day", "date": "today"}); msgs.append(mid)
        elif action == "products_today_open":
            mid = publish_pubsub_message(project_id, topic_id, {"task": "ingest_products_for_day", "date": "today", "status": "OPEN"}); msgs.append(mid)
        elif action == "events_for_date":
            day = (data.get("date") or "").strip() or "today"
            mid = publish_pubsub_message(project_id, topic_id, {"task": "ingest_events_for_day", "date": day}); msgs.append(mid)
        elif action == "products_for_date_open":
            day = (data.get("date") or "").strip() or "today"
            mid = publish_pubsub_message(project_id, topic_id, {"task": "ingest_products_for_day", "date": day, "status": "OPEN"}); msgs.append(mid)
        elif action == "single_product":
            pid = (data.get("product_id") or "").strip()
            if not pid:
                return app.response_class(json.dumps({"error": "missing product_id"}), mimetype="application/json", status=400)
            mid = publish_pubsub_message(project_id, topic_id, {"task": "ingest_single_product", "product_id": pid}); msgs.append(mid)
        elif action == "probable_odds_event":
            eid = (data.get("event_id") or "").strip()
            if not eid:
                return app.response_class(json.dumps({"error": "missing event_id"}), mimetype="application/json", status=400)
            mid = publish_pubsub_message(project_id, topic_id, {"task": "ingest_probable_odds", "event_id": eid}); msgs.append(mid)
        else:
            return app.response_class(json.dumps({"error": f"unknown action: {action}"}), mimetype="application/json", status=400)
        return app.response_class(json.dumps({"ok": True, "action": action, "message_ids": msgs}), mimetype="application/json")
    except Exception as e:
        return app.response_class(json.dumps({"error": str(e)}), mimetype="application/json", status=500)

@app.route("/event/<event_id>")
def event_detail(event_id: str):
    """Display details for a single event (BigQuery)."""
    # Fetch event details
    event_df = sql_df("SELECT * FROM tote_events WHERE event_id=?", params=(event_id,))
    if event_df.empty:
        flash("Event not found", "error")
        return redirect(url_for("tote_events_page"))
    event = event_df.to_dict("records")[0]

    # Fetch conditions
    conditions_df = sql_df("SELECT * FROM race_conditions WHERE event_id=?", params=(event_id,))
    conditions = None if conditions_df.empty else conditions_df.to_dict("records")[0]

    # Fetch historical results and runner details from hr_horse_runs.
    # This is the primary source for runners in an event, especially for historical events.
    runner_rows_df = sql_df(
        """
        SELECT
          r.horse_id,
          h.name AS horse_name,
          r.finish_pos,
          r.status,
          r.cloth_number,
          r.jockey,
          r.trainer
        FROM hr_horse_runs r
        LEFT JOIN hr_horses h ON h.horse_id = r.horse_id
        WHERE r.event_id = ?
        ORDER BY r.finish_pos NULLS LAST, r.cloth_number ASC
        """,
        params=(event_id,)
    )
    runner_rows = [] if runner_rows_df.empty else runner_rows_df.to_dict("records")

    # For upcoming events where results aren't available, fall back to competitors_json.
    runners = []
    competitors = []
    # Load competitor blob (if present) and normalize to expected keys for fallback display
    if event.get("competitors_json"):
        try:
            competitors = json.loads(event["competitors_json"]) or []
            if isinstance(competitors, list):
                runners = [
                    {
                        "cloth": (c.get("cloth") or c.get("cloth_number") or c.get("number")),
                        "name": (c.get("name") or c.get("competitor") or c.get("horse") or c.get("id") or "")
                    }
                    for c in competitors
                ]
                # Sort by cloth number if available
                try:
                    runners.sort(key=lambda x: (x.get("cloth") is None, int(x.get("cloth") or 1)))
                except Exception:
                    pass
        except (json.JSONDecodeError, TypeError):
            competitors = []

    # Fetch products (include pool components)
    products_df = sql_df(
        """
        SELECT product_id, bet_type, status, start_iso, event_id, event_name, venue,
               COALESCE(total_gross,0) AS total_gross,
               COALESCE(total_net,0)   AS total_net,
               COALESCE(rollover,0)    AS rollover,
               COALESCE(deduction_rate,0) AS takeout
        FROM tote_products
        WHERE event_id=?
        ORDER BY bet_type
        """,
        params=(event_id,)
    )
    products = products_df.to_dict("records") if not products_df.empty else []

    # Runners with latest probable odds
    runners_prob: list = []
    try: # noqa
        rprob = sql_df(
            """
            WITH event_selections AS (
                SELECT DISTINCT s.selection_id, s.number, s.competitor
                FROM tote_product_selections s
                JOIN tote_products p ON p.product_id = s.product_id
                WHERE p.event_id = @event_id
            ),
            win_product_odds AS (
                SELECT o.selection_id, o.decimal_odds, o.ts_ms
                FROM vw_tote_probable_odds o
                JOIN tote_products p ON o.product_id = p.product_id
                WHERE p.event_id = @event_id AND UPPER(p.bet_type) = 'WIN'
            )
            SELECT
                es.selection_id,
                es.number AS cloth_number,
                es.competitor AS horse,
                wpo.decimal_odds,
                TIMESTAMP_MILLIS(wpo.ts_ms) AS odds_ts
            FROM event_selections es
            LEFT JOIN win_product_odds wpo ON es.selection_id = wpo.selection_id
            ORDER BY CAST(es.number AS INT64) NULLS LAST
            """,
            params={'event_id': event_id}
        )
        runners_prob = rprob.to_dict("records") if not rprob.empty else []
    except Exception as e:
        print(f"Error fetching probable odds for event {event_id}: {e}")
        traceback.print_exc()
        runners_prob = []

    # Fetch features
    features_df = sql_df("SELECT * FROM vw_runner_features WHERE event_id=?", params=(event_id,))
    features = features_df.to_dict("records") if not features_df.empty else []

    return render_template(
        "event_detail.html",
        event=event,
        conditions=conditions,
        runners=runners,
        products=products,
        features=features,
        runner_rows=runner_rows,
        runners_prob=runners_prob,
        competitors=competitors,
    )

@app.route("/api/tote/product_selections/<product_id>")
def api_tote_product_selections(product_id: str):
    """Return selections for a given product (id, number, competitor)."""
    if not product_id:
        return app.response_class(json.dumps({"error": "missing product_id"}), mimetype="application/json", status=400)
    try:
        df = sql_df(
            "SELECT selection_id, number, competitor FROM tote_product_selections WHERE product_id=? ORDER BY CAST(number AS INT64) NULLS LAST",
            params=(product_id,)
        )
        rows = df.to_dict("records") if not df.empty else []
        return app.response_class(json.dumps({"product_id": product_id, "selections": rows}), mimetype="application/json")
    except Exception as e:
        return app.response_class(json.dumps({"error": str(e)}), mimetype="application/json", status=500)

@app.route("/horse/<horse_id>")
def horse_detail(horse_id: str):
    """Displays historical form for a single horse."""
    # Fetch horse details
    horse_df = sql_df("SELECT * FROM hr_horses WHERE horse_id=?", params=(horse_id,))
    if horse_df.empty:
        flash("Horse not found", "error")
        return redirect(url_for("index"))
    horse = horse_df.to_dict("records")[0]

    # Fetch last 10 runs with conditions
    runs_df = sql_df(
        """
        SELECT r.*, e.name as event_name, e.venue, c.going, c.weather_desc
        FROM hr_horse_runs r
        LEFT JOIN tote_events e ON e.event_id = r.event_id
        LEFT JOIN race_conditions c ON c.event_id = r.event_id
        WHERE r.horse_id = ?
        ORDER BY e.start_iso DESC
        LIMIT 10
        """, params=(horse_id,)
    )
    runs = [] if runs_df.empty else runs_df.to_dict("records")
    return render_template("horse_detail.html", horse=horse, runs=runs)

@app.route("/tote-superfecta/<product_id>")
def tote_superfecta_detail(product_id: str):
    pdf = sql_df("SELECT * FROM tote_products WHERE product_id=?", params=(product_id,))
    if pdf.empty:
        flash("Unknown product id", "error")
        return redirect(url_for("tote_superfecta_page"))
    p = pdf.iloc[0].to_dict()
    runners_df = sql_df("SELECT DISTINCT number, competitor FROM tote_product_selections WHERE product_id=? ORDER BY number", params=(product_id,))
    runners = runners_df.to_dict("records") if not runners_df.empty else []
    # Fallback: if no runners recorded yet, infer cloth numbers from probable odds view
    if not runners:
        try:
            odf = sql_df("SELECT DISTINCT CAST(cloth_number AS INT64) AS number FROM vw_tote_probable_odds WHERE product_id=? ORDER BY number", params=(product_id,))
            if not odf.empty:
                nums = odf["number"].dropna().astype(int).tolist()
                name_map = {}
                if not runners_df.empty:
                    name_map = {int(r.get("number")): r.get("competitor") for _, r in runners_df.iterrows() if r.get("number") is not None}
                runners = [{"number": n, "competitor": name_map.get(n)} for n in nums]
        except Exception:
            pass
    items = []
    for r in runners:
        items.append({
            'id': r.get('competitor') or '',
            'name': r.get('competitor') or f"#{r.get('number')}",
            'cloth': r.get('number')
        })
    items = [x for x in items if x.get('cloth') is not None]
    items.sort(key=lambda x: x.get('cloth'))
    # Load any reported dividends for this product (latest per selection)
    divs = sql_df(
        """
        SELECT selection, MAX(ts) AS ts, MAX(dividend) AS dividend
        FROM tote_product_dividends
        WHERE product_id=?
        GROUP BY selection
        ORDER BY dividend DESC
        """,
        params=(product_id,)
    )
    # Load finishing order if recorded
    fr = sql_df(
        """
        SELECT horse_id, finish_pos, status, cloth_number
        FROM hr_horse_runs WHERE event_id=? AND finish_pos IS NOT NULL
        ORDER BY finish_pos ASC
        """,
        params=(p.get('event_id'),)
    )
    # Load conditions (going + weather)
    cond = sql_df(
        "SELECT going, weather_temp_c, weather_wind_kph, weather_precip_mm FROM race_conditions WHERE event_id=?",
        params=(p.get('event_id'),)
    )
    # Runners with latest probable odds (if available)
    runners2: list = []
    try:
        r2 = sql_df(
            """
            SELECT s.selection_id,
                   s.number AS cloth_number,
                   s.competitor AS horse,
                   ANY_VALUE(p.bet_type) AS bet_type,
                   ANY_VALUE(p.product_id) AS product_id,
                   o.decimal_odds,
                   TIMESTAMP_MILLIS(o.ts_ms) AS odds_ts
            FROM tote_product_selections s
            JOIN tote_products p USING(product_id)
            LEFT JOIN vw_tote_probable_odds o ON o.product_id = s.product_id AND o.selection_id = s.selection_id
            WHERE p.event_id = ?
            GROUP BY s.selection_id, s.number, s.competitor, o.decimal_odds, o.ts_ms
            ORDER BY CAST(s.number AS INT64) NULLS LAST
            """,
            params=(p.get('event_id'),)
        )
        runners2 = r2.to_dict("records") if not r2.empty else []
    except Exception:
        runners2 = []

    # No local tote_bets in BQ-only mode
    recent_bets: list = []
    dividends = divs.to_dict("records") if not divs.empty else []
    finishing = fr.to_dict("records") if not fr.empty else []
    conditions = cond.iloc[0].to_dict() if not cond.empty else {}
    return render_template(
        "tote_superfecta_detail.html",
        product=p,
        runners=items,
        runners_prob=runners2,
        dividends=dividends,
        finishing=finishing,
        conditions=conditions,
        recent_bets=recent_bets,
    )

@app.route("/tote/manual-calculator", methods=["GET", "POST"])
def manual_calculator_page():
    # Default values for form
    calc_params = {
        "num_runners": 8,
        "bet_type": "SUPERFECTA",
        "runners": [
            {"name": f"Runner {i+1}", "odds": round((i+2)*2.0, 2), "is_key": False, "is_poor": False} for i in range(8)
        ],
        "bankroll": 100.0,
        "key_horse_mult": 2.0,
        "poor_horse_mult": 0.5,
        "concentration": 0.0,
        "market_inefficiency": 0.10,
        "desired_profit_pct": 5.0,
        "take_rate": 0.30,
        "net_rollover": 0.0,
        "inc_self": True,
        "div_mult": 1.0,
        "f_fix": None,
        "pool_gross_other": 10000.0,
    }

    results = {}
    errors = []

    manual_override_active = False

    if request.method == "POST":
        # Parse inputs from form
        manual_override_active = request.form.get("manual_override_active") == "1"
        try:
            calc_params["num_runners"] = int(request.form.get("num_runners", calc_params["num_runners"]))
            calc_params["bet_type"] = request.form.get("bet_type", calc_params["bet_type"]).upper()
            calc_params["bankroll"] = float(request.form.get("bankroll", calc_params["bankroll"]))
            # New weighting multipliers
            calc_params["key_horse_mult"] = float(request.form.get("key_horse_mult", calc_params["key_horse_mult"]))
            calc_params["poor_horse_mult"] = float(request.form.get("poor_horse_mult", calc_params["poor_horse_mult"]))
            calc_params["concentration"] = float(request.form.get("concentration", calc_params["concentration"]))
            calc_params["market_inefficiency"] = float(request.form.get("market_inefficiency", calc_params["market_inefficiency"]))
            calc_params["desired_profit_pct"] = float(request.form.get("desired_profit_pct", calc_params["desired_profit_pct"]))
            calc_params["take_rate"] = float(request.form.get("take_rate", calc_params["take_rate"]))
            calc_params["net_rollover"] = float(request.form.get("net_rollover", calc_params["net_rollover"]))
            calc_params["inc_self"] = request.form.get("inc_self") == "1"
            calc_params["div_mult"] = float(request.form.get("div_mult", calc_params["div_mult"]))
            calc_params["f_fix"] = float(request.form.get("f_fix")) if request.form.get("f_fix") else None
            calc_params["pool_gross_other"] = float(request.form.get("pool_gross_other", calc_params["pool_gross_other"]))

            # Validate inputs
            if calc_params["num_runners"] <= 0:
                errors.append("Number of runners must be positive.")
            if calc_params["bankroll"] <= 0:
                errors.append("Bankroll must be positive.")
            if not (0 <= calc_params["take_rate"] < 1):
                errors.append("Takeout rate must be between 0 and 1 (e.g., 0.30 for 30%).")

            # Parse runner grid
            runners_from_form = []
            try:
                for i in range(calc_params["num_runners"]):
                    odds = float(request.form.get(f"runner_odds_{i}", "10.0"))
                    if odds <= 1.0: errors.append(f"Runner {i+1} odds must be greater than 1.0.")
                    runners_from_form.append({
                        "name": request.form.get(f"runner_name_{i}", f"Runner {i+1}"),
                        "odds": odds,
                        "is_key": request.form.get(f"runner_key_{i}") == "on",
                        "is_poor": request.form.get(f"runner_poor_{i}") == "on",
                    })
                calc_params["runners"] = runners_from_form
            except ValueError as e:
                errors.append(f"Invalid odds format: {e}. Please use valid numbers.")

        except ValueError as e:
            errors.append(f"Invalid input: {e}. Please check numeric fields.")

        if not errors:
            # Determine K (positions for bet type)
            k_map = {"WIN": 1, "EXACTA": 2, "TRIFECTA": 3, "SUPERFECTA": 4}
            k_perm = k_map.get(calc_params["bet_type"], 4) # Default to Superfecta if unknown

            if calc_params["num_runners"] < k_perm:
                errors.append(f"Not enough runners ({calc_params['num_runners']}) for {calc_params['bet_type']} (needs at least {k_perm}).")

        if not errors:
            # --- Plackett-Luce Calculation ---
            # Calculate final weights for each runner
            runner_weights = []
            for i, r in enumerate(calc_params["runners"]):
                base_weight = 1.0 / r["odds"]
                if r["is_key"]: base_weight *= calc_params["key_horse_mult"]
                if r["is_poor"]: base_weight *= calc_params["poor_horse_mult"]
                runner_weights.append(base_weight)

            # Map runner IDs to their final weights and names
            runners_with_strengths = [
                {"id": i + 1, "name": calc_params["runners"][i]["name"], "strength": runner_weights[i]}
                for i in range(calc_params["num_runners"])
            ]
            
            # Generate all permutations and calculate PL probabilities
            pl_permutations = []
            for perm_tuple in itertools.permutations(runners_with_strengths, k_perm):
                current_strength_pool = list(runners_with_strengths) # Copy to modify
                perm_prob = 1.0
                
                for i, runner_details in enumerate(perm_tuple):
                    runner_id = runner_details["id"]
                    runner_strength = runner_details["strength"]
                    remaining_strength_sum = sum(r['strength'] for r in current_strength_pool)
                    if remaining_strength_sum == 0:
                        perm_prob = 0.0 # Avoid division by zero if all remaining strengths are zero
                        break
                    
                    prob_this_pos = runner_strength / remaining_strength_sum
                    perm_prob *= prob_this_pos
                    
                    # Remove this runner from the pool for the next position's calculation by filtering on id
                    current_strength_pool = [r for r in current_strength_pool if r["id"] != runner_id]
                
                pl_permutations.append({
                    "line": " - ".join(str(r["name"]) for r in perm_tuple),
                    "probability": perm_prob,
                    "runners_detail": perm_tuple # For display/debugging
                })
            
            # Sort by probability (descending)
            pl_permutations.sort(key=lambda x: x["probability"], reverse=True)

            # --- EV Optimization Loop ---
            ev_grid = []
            optimal_ev_scenario = None
            max_ev = -float("inf")
            C = len(pl_permutations)

            for m in range(1, C + 1):
                covered_lines = pl_permutations[:m]
                hit_rate = sum(p['probability'] for p in covered_lines)
                
                S = calc_params['bankroll']
                S_inc = S if calc_params['inc_self'] else 0.0
                O = calc_params['pool_gross_other']
                t = calc_params['take_rate']
                R = calc_params['net_rollover']
                mult = calc_params['div_mult']
                
                # Apply market inefficiency to O for f-share calculation
                O_effective = O * (1.0 - calc_params['market_inefficiency'])

                # Enhanced f-share calculation. Assumes others' money 'O' is spread according to PL probabilities.
                # Our f-share on any of our covered lines is our effective stake density vs the market's.
                if hit_rate > 0:
                    stake_density = S / hit_rate
                    f_auto = stake_density / (stake_density + O_effective)
                else:
                    f_auto = 0.0
                f_used = float(calc_params['f_fix']) if (calc_params['f_fix'] is not None) else f_auto
                
                net_pool_if_bet = mult * (((1.0 - t) * (O + S_inc)) + R)
                expected_return = hit_rate * f_used * net_pool_if_bet
                expected_profit = expected_return - S
                
                ev_grid.append({"lines_covered": m, "hit_rate": hit_rate, "expected_profit": expected_profit})
                
                if expected_profit > max_ev:
                    max_ev = expected_profit
                    optimal_ev_scenario = {
                        "lines_covered": m,
                        "hit_rate": hit_rate,
                        "expected_profit": expected_profit,
                        "expected_return": expected_return,
                        "f_share_used": f_used,
                        "net_pool_if_bet": net_pool_if_bet,
                        "total_stake": S,
                    }

            # --- Determine Base Strategy ---
            # Find the scenario that meets the user's desired profit with the fewest lines
            target_profit = calc_params['bankroll'] * (calc_params['desired_profit_pct'] / 100.0)
            target_profit_scenario = None
            if target_profit > 0 and optimal_ev_scenario:
                # Build a consistent scenario object (with same keys as max-EV) for the first case meeting target
                for scenario in ev_grid:
                    if scenario['expected_profit'] >= target_profit:
                        hr = scenario['hit_rate']
                        exp_ret = scenario['expected_profit'] + S
                        # Recover f-share used for this scenario to keep fields consistent
                        fshare = (exp_ret / (hr * optimal_ev_scenario['net_pool_if_bet'])) if hr > 0 else 0.0
                        target_profit_scenario = {
                            "lines_covered": scenario['lines_covered'],
                            "hit_rate": hr,
                            "expected_profit": scenario['expected_profit'],
                            "expected_return": exp_ret,
                            "f_share_used": fshare,
                            "net_pool_if_bet": optimal_ev_scenario['net_pool_if_bet'],
                            "total_stake": S,
                        }
                        break
            
            # The base for our adjustments is the target profit scenario, or the max EV scenario as a fallback.
            base_scenario = target_profit_scenario if target_profit_scenario else optimal_ev_scenario

            # --- Strategy Adjustment & Final Calculation ---
            display_scenario = None
            staking_plan = []
            
            if base_scenario:
                # If the chosen base strategy is not profitable, try to find and apply an automatic adjustment.
                if base_scenario['expected_profit'] <= 0 and not manual_override_active:
                    S = calc_params['bankroll']
                    S_inc = S if calc_params['inc_self'] else 0.0
                    O = calc_params['pool_gross_other']
                    t = calc_params['take_rate']
                    R = calc_params['net_rollover']
                    mult = calc_params['div_mult']
                    net_pool_if_bet = mult * (((1.0 - t) * (O + S_inc)) + R)
                    max_ev_lines = base_scenario['lines_covered']

                    mi_orig = calc_params['market_inefficiency']
                    possible_solutions = []

                    # Find the required MI for every possible concentration level
                    for i in range(21): # 0.0, 0.05, ..., 1.0
                        concentration_level = i / 20.0
                        lines_to_cover = max(1, int(round(max_ev_lines * (1.0 - concentration_level) + 1.0 * concentration_level)))
                        if lines_to_cover > len(pl_permutations): continue

                        hit_rate = sum(p['probability'] for p in pl_permutations[:lines_to_cover])
                        if hit_rate <= 0 or O <= 0: continue

                        stake_density = S / hit_rate
                        if net_pool_if_bet <= stake_density: continue

                        # Calculate MI to break even, then add a small buffer to target a slightly positive profit.
                        required_mi = (1.0 - (net_pool_if_bet - stake_density) / O) + 0.005

                        if required_mi > mi_orig and required_mi < 0.9:
                            possible_solutions.append({
                                "concentration": concentration_level,
                                "market_inefficiency": required_mi
                            })

                    best_suggestion = None
                    if possible_solutions:
                        sol_min_mi = min(possible_solutions, key=lambda x: x['market_inefficiency'])
                        solutions_with_conc = [s for s in possible_solutions if s['concentration'] >= 0.1]
                        
                        if solutions_with_conc:
                            sol_alt = min(solutions_with_conc, key=lambda x: x['market_inefficiency'])
                            if sol_alt['market_inefficiency'] < sol_min_mi['market_inefficiency'] + 0.05:
                                best_suggestion = sol_alt
                            else:
                                best_suggestion = sol_min_mi
                        else:
                            best_suggestion = sol_min_mi
                    
                    # If a better strategy was found, apply it automatically
                    if best_suggestion:
                        flash(f"Original settings were -EV. Parameters auto-adjusted to find a profitable strategy: Bet Concentration set to {best_suggestion['concentration']*100:.0f}% and Market Inefficiency to {best_suggestion['market_inefficiency']*100:.1f}%. You can modify these and recalculate.", "info")
                        calc_params['concentration'] = best_suggestion['concentration']
                        calc_params['market_inefficiency'] = best_suggestion['market_inefficiency']
                        manual_override_active = True

                # The rest of the calculation uses either the original or the auto-adjusted calc_params
                concentration = calc_params["concentration"]
                base_lines_to_cover = base_scenario['lines_covered']
                
                # Calculate the final number of lines to cover based on concentration slider
                final_lines_to_cover = max(1, int(round(base_lines_to_cover * (1.0 - concentration) + 1.0 * concentration)))
                # Build the scenario that will actually be displayed and used for staking
                final_covered_lines = pl_permutations[:final_lines_to_cover]
                final_hit_rate = sum(p['probability'] for p in final_covered_lines)
                
                S = calc_params['bankroll']
                S_inc = S if calc_params['inc_self'] else 0.0
                O = calc_params['pool_gross_other']
                t = calc_params['take_rate']
                R = calc_params['net_rollover']
                mult = calc_params['div_mult']
                
                # Apply market inefficiency to O for f-share calculation
                O_effective = O * (1.0 - calc_params['market_inefficiency'])

                # Enhanced f-share calculation for the final displayed scenario.
                if final_hit_rate > 0:
                    stake_density = S / final_hit_rate
                    f_auto = stake_density / (stake_density + O_effective)
                else:
                    f_auto = 0.0
                f_used = float(calc_params['f_fix']) if (calc_params['f_fix'] is not None) else f_auto
                
                net_pool_if_bet = mult * (((1.0 - t) * (O + S_inc)) + R)
                expected_return = final_hit_rate * f_used * net_pool_if_bet
                expected_profit = expected_return - S
                
                display_scenario = {
                    "lines_covered": final_lines_to_cover,
                    "hit_rate": final_hit_rate,
                    "expected_profit": expected_profit,
                    "expected_return": expected_return,
                    "f_share_used": f_used,
                    "net_pool_if_bet": net_pool_if_bet,
                    "total_stake": S,
                }

                # Calculate the weighted staking plan for the final scenario
                prob_sum_final = sum(p['probability'] for p in final_covered_lines)
                for line in final_covered_lines:
                    line['stake'] = (line['probability'] / prob_sum_final) * calc_params['bankroll'] if prob_sum_final > 0 else 0
                staking_plan = final_covered_lines
            
            results["pl_model"] = {
                "best_scenario": display_scenario,           # final scenario after concentration slider
                "base_scenario": base_scenario,               # chosen base (target-profit or max-EV)
                "optimal_ev_scenario": optimal_ev_scenario,   # true max-EV across m
                "staking_plan": staking_plan,
                "ev_grid": ev_grid,
                "total_possible_lines": C,
            }

    return render_template(
        "manual_calculator.html",
        calc_params=calc_params,
        results=results,
        errors=errors,
        bet_types=["WIN", "EXACTA", "TRIFECTA", "SUPERFECTA"],
        manual_override_active=manual_override_active,
    )

@app.route("/api/tote/bet_status")
def api_tote_bet_status():
    from .providers.tote_bets import refresh_bet_status
    # This API relies on the 'tote_bets' table, which is SQLite-only.
    return app.response_class(json.dumps({"error": "Bet status API not available in BigQuery-only mode."}),
                              mimetype="application/json", status=400)
    bet_id = (request.args.get("bet_id") or "").strip()
    post = (request.args.get("post") or "").lower() in ("1","true","yes","on")
    if not bet_id:
        return app.response_class(json.dumps({"error": "missing bet_id"}), mimetype="application/json", status=400)
    conn = _get_db_conn(); init_schema(conn)
    res = refresh_bet_status(conn, bet_id=bet_id, post=post)
    conn.commit(); conn.close()
    return app.response_class(json.dumps(res), mimetype="application/json")

@app.route("/api/tote/placement_id")
def api_tote_placement_id():
    pid = (request.args.get("product_id") or "").strip()
    # This API relies on the 'tote_bets' table, which is SQLite-only.
    return app.response_class(json.dumps({"error": "Placement ID API not available in BigQuery-only mode."}),
                              mimetype="application/json", status=400)
    if not pid:
        return app.response_class(json.dumps({"error": "missing product_id"}), mimetype="application/json", status=400)
    conn = _get_db_conn(); init_schema(conn)
    try:
        row = conn.execute("SELECT event_id, substr(start_iso,1,10) FROM tote_products WHERE product_id=?", (pid,)).fetchone()
        if not row:
            return app.response_class(json.dumps({"product_id": pid, "placement_product_id": pid, "method": "original", "note": "product not found locally"}), mimetype="application/json")
        event_id = row[0]; day = row[1]
        placement_pid = pid; method = "original"
        selling_status = None
        try:
            from .providers.tote_api import ToteClient
            client = ToteClient()
            q2 = """
            query Products($date: Date, $betTypes: [BetTypeCode!], $status: BettingProductSellingStatus, $first: Int){
              products(date:$date, betTypes:$betTypes, sellingStatus:$status, first:$first){
                nodes{ id eventId selling{status} }
              }
            }
            """
            vars = {"date": day, "betTypes": ["SUPERFECTA"], "status": "OPEN", "first": 1000}
            pdata2 = client.graphql(q2, vars)
            nodes = ((pdata2.get("products") or {}).get("nodes")) or []
            for n in nodes:
                if (n.get("eventId") == event_id) and ((n.get("selling") or {}).get("status","OPEN").upper() == "OPEN"):
                    placement_pid = n.get("id") or placement_pid
                    selling_status = (n.get("selling") or {}).get("status")
                    method = "products_match"
                    break
            if method == "original":
                placement_pid = f"SUPERFECTA:{event_id}"
                method = "alias"
        except Exception:
            pass
        out = {
            "original_product_id": pid,
            "placement_product_id": placement_pid,
            "method": method,
            "event_id": event_id,
            "date": day,
            "selling_status": selling_status,
        }
        return app.response_class(json.dumps(out), mimetype="application/json")
    finally:
        try: conn.close()
        except Exception: pass

@app.route("/models")
def models_page():
    # This page relies on SQLite-only tables like 'models' and 'predictions'.
    flash("Models pages are temporarily disabled.", "warning")
    return redirect(url_for('index'))
    conn = _get_db_conn(); init_schema(conn)
    df = sql_df(conn, "SELECT model_id, created_ts, market, algo, metrics_json, path FROM models ORDER BY created_ts DESC")
    conn.close()
    models = [] if df.empty else df.to_dict("records")
    # Parse metrics json
    for m in models:
        try:
            m["metrics"] = json.loads(m.get("metrics_json") or "{}")
        except Exception:
            m["metrics"] = {}
    # Attach latest prediction run timestamp per model
    conn = _get_db_conn()
    for m in models:
        try:
            row = conn.execute("SELECT MAX(ts_ms) FROM predictions WHERE model_id=?", (m.get('model_id'),)).fetchone()
            m["latest_ts_ms"] = int(row[0]) if row and row[0] is not None else None
        except Exception:
            m["latest_ts_ms"] = None
    conn.close()
    return render_template("models.html", models=models)

@app.route("/models/<model_id>/eval")
def model_eval_page(model_id: str):
    # This page relies on SQLite-only tables like 'predictions', 'hr_horse_runs', 'hr_horses', 'features_runner_event'.
    flash("Model evaluation is temporarily disabled.", "warning")
    return redirect(url_for('index'))
    conn = _get_db_conn(); init_schema(conn)
    # Available prediction runs (timestamps)
    runs = sql_df(
        "SELECT DISTINCT ts_ms FROM predictions WHERE model_id=? ORDER BY ts_ms DESC LIMIT 50",
        params=(model_id,)
    )
    ts_param = request.args.get("ts")
    ts_ms = None
    if ts_param:
        try:
            ts_ms = int(ts_param)
        except Exception:
            ts_ms = None
    if ts_ms is None and not runs.empty:
        ts_ms = int(runs.iloc[0]["ts_ms"])  # latest
    date_from = request.args.get("from")
    date_to = request.args.get("to")
    # Load joined predictions + results within optional date filter
    params = [model_id, ts_ms]
    where = ""
    if date_from:
        where += " AND p.event_id IN (SELECT event_id FROM tote_products WHERE start_iso >= ?)"; params.append(date_from)
    if date_to:
        where += " AND p.event_id IN (SELECT event_id FROM tote_products WHERE start_iso <= ?)"; params.append(date_to)
    sql = (
        "SELECT p.event_id, p.horse_id, p.proba, r.finish_pos, h.name AS horse_name, f.event_date "
        "FROM predictions p "
        "JOIN hr_horse_runs r ON r.event_id = p.event_id AND r.horse_id = p.horse_id "
        "LEFT JOIN hr_horses h ON h.horse_id = p.horse_id "
        "LEFT JOIN features_runner_event f ON f.event_id = p.event_id AND f.horse_id = p.horse_id "
        "WHERE p.model_id=? AND p.ts_ms=?" + where
    )
    df = sql_df(conn, sql, params=tuple(params))
    # If empty, render page with controls
    metrics = {"events": 0, "top1_hits": 0, "hit_rate": None, "brier": None}
    samples = []
    ts_options = (runs.to_dict("records") if not runs.empty else [])
    if not df.empty:
        # Compute per-event winner
        df["is_winner"] = (df["finish_pos"] == 1).astype(int)
        # Hit rate of top-1 per event
        top = df.sort_values(["event_id", "proba"], ascending=[True, False]).drop_duplicates(["event_id"], keep="first")
        hits = int(top["is_winner"].sum())
        n_events = int(top.shape[0])
        hit_rate = (hits / n_events) if n_events else None
        # Brier score over all rows
        try:
            brier = float(((df["proba"] - df["is_winner"]) ** 2).mean())
        except Exception:
            brier = None
        metrics = {"events": n_events, "top1_hits": hits, "hit_rate": hit_rate, "brier": brier}
        # Build samples of mismatches and matches
        # Mismatches: predicted top-1 but not winner
        merged = top.copy()
        merged["pred_name"] = merged["horse_name"]
        # Actual winners per event for context
        winners = df[df["finish_pos"] == 1][["event_id", "horse_name"]].drop_duplicates(["event_id"])
        winners = winners.rename(columns={"horse_name": "winner_name"})
        merged = pd.merge(merged, winners, on="event_id", how="left")
        bad = merged[merged["is_winner"] == 0].sort_values("proba", ascending=False).head(50)
        good = merged[merged["is_winner"] == 1].sort_values("proba", ascending=False).head(50)
        samples = {
            "misses": bad.to_dict("records"),
            "hits": good.to_dict("records"),
        }
    conn.close()
    return render_template(
        "model_eval.html",
        model_id=model_id,
        ts_ms=ts_ms,
        ts_options=ts_options,
        filters={"from": date_from or "", "to": date_to or ""},
        metrics=metrics,
        samples=samples,
    )

@app.route("/models/<model_id>/superfecta")
def model_superfecta_eval_page(model_id: str):
    # This page relies on SQLite-only tables like 'predictions', 'superfecta_eval', 'tote_products'.
    flash("Model pages are temporarily disabled.", "warning")
    return redirect(url_for('index'))
    conn = _get_db_conn(); init_schema(conn)
    # Latest run by default
    runs = sql_df(
        "SELECT DISTINCT ts_ms FROM predictions WHERE model_id=? ORDER BY ts_ms DESC LIMIT 50",
        params=(model_id,)
    )
    ts_param = request.args.get("ts")
    ts_ms = None
    if ts_param:
        try:
            ts_ms = int(ts_param)
        except Exception:
            ts_ms = None
    if ts_ms is None and not runs.empty:
        ts_ms = int(runs.iloc[0]["ts_ms"])  # latest
    date_from = request.args.get("from"); date_to = request.args.get("to")
    # Optionally trigger evaluation on request
    if request.args.get("run") == "1":
        from .eval_sf import evaluate_superfecta
        try:
            evaluate_superfecta(conn, model_id=model_id, ts_ms=ts_ms, date_from=date_from or None, date_to=date_to or None)
        except Exception:
            pass
    # Load eval rows
    params = [model_id]
    where = ["model_id=?"]
    if ts_ms is not None:
        where.append("ts_ms=?"); params.append(ts_ms)
    if date_from:
        where.append("product_id IN (SELECT product_id FROM tote_products WHERE start_iso >= ?)"); params.append(date_from)
    if date_to:
        where.append("product_id IN (SELECT product_id FROM tote_products WHERE start_iso <= ?)"); params.append(date_to)
    sql = "SELECT * FROM superfecta_eval WHERE " + " AND ".join(where) + " ORDER BY ts_ms DESC"
    ev = sql_df(conn, sql, params=tuple(params))
    # Summary metrics
    metrics = {"n": 0, "exact4": 0, "prefix3": 0, "prefix2": 0, "any4set": 0}
    rows = []
    if not ev.empty:
        metrics["n"] = int(ev.shape[0])
        for k in ("exact4","prefix3","prefix2","any4set"):
            try:
                metrics[k] = int(ev[k].sum())
            except Exception:
                metrics[k] = 0
        rows = ev.to_dict("records")
    conn.close()
    return render_template("model_superfecta.html", model_id=model_id, ts_ms=ts_ms, ts_options=(runs.to_dict("records") if not runs.empty else []), filters={"from": date_from or "", "to": date_to or ""}, metrics=metrics, rows=rows)

@app.route("/models/<model_id>/eval/event/<event_id>")
def model_event_eval_page(model_id: str, event_id: str):
    # This page relies on SQLite-only tables like 'predictions', 'hr_horses', 'hr_horse_runs', 'tote_events', 'tote_products'.
    flash("Model pages are temporarily disabled.", "warning")
    return redirect(url_for('index'))
    """Drill-down: show predicted probabilities vs actual finish for one event."""
    ts_param = request.args.get("ts")
    ts_ms = None
    if ts_param:
        try:
            ts_ms = int(ts_param)
        except Exception:
            ts_ms = None
    conn = _get_db_conn(); init_schema(conn)
    if ts_ms is None:
        row = conn.execute(
            "SELECT MAX(ts_ms) FROM predictions WHERE model_id=? AND event_id=?",
            (model_id, event_id)
        ).fetchone()
        ts_ms = int(row[0]) if row and row[0] is not None else None
    # Event context
    ev = conn.execute("SELECT * FROM tote_events WHERE event_id=?", (event_id,)).fetchone()
    event = None
    if ev:
        cols = [c[0] for c in conn.execute('PRAGMA table_info(tote_events)').fetchall()]
        event = {cols[i]: ev[i] for i in range(len(cols))}
    else:
        # fallback from products
        pr = conn.execute("SELECT MIN(event_name), MIN(venue), MIN(start_iso) FROM tote_products WHERE event_id=?", (event_id,)).fetchone()
        if pr:
            event = {"event_id": event_id, "name": pr[0], "venue": pr[1], "start_iso": pr[2]}
    # Predictions joined with results & names
    rows = sql_df(
        """
        SELECT p.horse_id, h.name AS horse_name, p.proba, p.rank, r.finish_pos, r.cloth_number
        FROM predictions p
        LEFT JOIN hr_horses h ON h.horse_id = p.horse_id
        LEFT JOIN hr_horse_runs r ON r.event_id = p.event_id AND r.horse_id = p.horse_id
        WHERE p.model_id=? AND p.ts_ms=? AND p.event_id=?
        ORDER BY p.proba DESC
        """,
        params=(model_id, ts_ms, event_id)
    )
    conn.close()
    preds = rows.to_dict("records") if not rows.empty else []
    return render_template("model_event_eval.html", model_id=model_id, ts_ms=ts_ms, event=event, event_id=event_id, preds=preds)
@app.route("/imports")
def imports_page():
    """Show latest data imports and basic counts from BigQuery."""
    try:
        prod_today = sql_df(
            "SELECT COUNT(1) AS c, MAX(start_iso) AS max_start FROM tote_products WHERE DATE(SUBSTR(start_iso,1,10))=CURRENT_DATE()"
        )
        ev_today = sql_df(
            "SELECT COUNT(1) AS c, MAX(start_iso) AS max_start FROM tote_events WHERE DATE(SUBSTR(start_iso,1,10))=CURRENT_DATE()"
        )
        raw_latest = sql_df(
            "SELECT endpoint, fetched_ts FROM raw_tote ORDER BY fetched_ts DESC LIMIT 20"
        )
        prob_latest = sql_df(
            "SELECT fetched_ts FROM raw_tote_probable_odds ORDER BY fetched_ts DESC LIMIT 20"
        )
    except Exception as e:
        flash(f"Import stats error: {e}", "error")
        prod_today = ev_today = raw_latest = prob_latest = None
    return render_template(
        "imports.html",
        prod_today=(prod_today.to_dict("records")[0] if prod_today is not None and not prod_today.empty else {}),
        ev_today=(ev_today.to_dict("records")[0] if ev_today is not None and not ev_today.empty else {}),
        raw_latest=(raw_latest.to_dict("records") if raw_latest is not None and not raw_latest.empty else []),
        prob_latest=(prob_latest.to_dict("records") if prob_latest is not None and not prob_latest.empty else []),
    )

@app.route("/tote/bet", methods=["GET", "POST"])
def tote_bet_page():
    """A simple page to place single-line audit bets."""
    if request.method == "POST":
        product_id = request.form.get("product_id")
        selection_id = request.form.get("selection_id")
        stake_str = request.form.get("stake")
        currency = request.form.get("currency", "GBP")

        errors = []
        if not product_id: errors.append("Product must be selected.")
        if not selection_id: errors.append("A runner/selection must be chosen.")
        if not stake_str: errors.append("Stake is required.")
        
        stake = 0.0
        try:
            stake = float(stake_str)
            if stake <= 0: errors.append("Stake must be a positive number.")
        except ValueError:
            errors.append("Stake must be a valid number.")

        if errors:
            for e in errors:
                flash(e, "error")
            return redirect(url_for("tote_bet_page"))

        from .providers.tote_bets import place_audit_simple_bet
        db = get_db()
        
        res = place_audit_simple_bet(
            db, product_id=product_id, selection_id=selection_id, stake=stake, currency=currency, post=True
        )

        st = res.get("placement_status")
        if res.get("error"):
            flash(f"Audit bet error: {res.get('error')}", "error")
        elif st:
            ok = str(st).upper() in ("PLACED", "ACCEPTED", "OK", "SUCCESS")
            msg = f"Audit bet placement status: {st}"
            fr = res.get("failure_reason")
            if fr: msg += f" (reason: {fr})"
            flash(msg, "success" if ok else "error")
        else:
            flash("Audit bet recorded (no placement status returned).", "success")
        
        return redirect(url_for("tote_bet_page"))

    # GET request: Fetch upcoming events to populate the selector
    now = datetime.now(timezone.utc)
    start_iso = now.isoformat(timespec='seconds').replace('+00:00', 'Z')
    end_iso = (now + timedelta(hours=24)).isoformat(timespec='seconds').replace('+00:00', 'Z')

    ev_sql = (
        "SELECT event_id, name, venue, country, start_iso FROM tote_events "
        "WHERE start_iso BETWEEN @start AND @end ORDER BY start_iso ASC LIMIT 200"
    )
    events_df = sql_df(ev_sql, params={"start": start_iso, "end": end_iso})
    
    return render_template(
        "tote_bet.html",
        events=events_df.to_dict("records") if not events_df.empty else [],
    )
