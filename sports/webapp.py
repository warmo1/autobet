from __future__ import annotations
import os
import time
import pandas as pd
from datetime import date, timedelta, datetime
from flask import Flask, render_template, request, redirect, flash, url_for, send_file, Response
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
from .providers.pl_calcs import calculate_pl_strategy, calculate_pl_from_perms
import itertools
import math
import requests

# Simple in-process TTL cache for sql_df results
_SQLDF_CACHE_LOCK = threading.Lock()
_SQLDF_CACHE: dict[tuple, tuple[float, pd.DataFrame]] = {}

def _sqldf_cache_key(final_sql: str, params: Any) -> tuple:
    """Build a stable cache key from SQL, params, and active dataset settings."""
    proj = cfg.bq_project or ""
    ds = cfg.bq_dataset or ""
    try:
        if isinstance(params, Mapping):
            # Normalize mapping by sorting keys
            kv = tuple(sorted((str(k), None if v is None else (float(v) if isinstance(v, (int, float)) else str(v))) for k, v in params.items()))
        elif isinstance(params, (list, tuple)):
            kv = tuple(None if v is None else (float(v) if isinstance(v, (int, float)) else str(v)) for v in params)
        elif params is None:
            kv = ()
        else:
            kv = (str(params),)
    except Exception:
        # Fallback to stringifying if any issue
        kv = (str(params),)
    return (proj, ds, final_sql, kv)

def _sqldf_cache_get(key: tuple) -> Optional[pd.DataFrame]:
    if not cfg.web_sqldf_cache_enabled:
        return None
    now = time.time()
    with _SQLDF_CACHE_LOCK:
        ent = _SQLDF_CACHE.get(key)
        if not ent:
            return None
        exp, df = ent
        if exp < now:
            # expired; drop
            try:
                del _SQLDF_CACHE[key]
            except Exception:
                pass
            return None
        # Return a deep copy to avoid mutation-by-callers
        try:
            return df.copy(deep=True)
        except Exception:
            return df

def _sqldf_cache_set(key: tuple, df: pd.DataFrame, ttl: int) -> None:
    if not cfg.web_sqldf_cache_enabled or ttl <= 0:
        return
    exp = time.time() + ttl
    with _SQLDF_CACHE_LOCK:
        # Capacity guard: purge expired then trim if needed
        if len(_SQLDF_CACHE) >= max(16, cfg.web_sqldf_cache_max_entries):
            now = time.time()
            # Remove expired entries first
            expired = [k for k, (e, _) in _SQLDF_CACHE.items() if e < now]
            for k in expired:
                _SQLDF_CACHE.pop(k, None)
            # If still over capacity, drop oldest entries
            if len(_SQLDF_CACHE) >= cfg.web_sqldf_cache_max_entries:
                oldest = sorted(_SQLDF_CACHE.items(), key=lambda it: it[1][0])[: max(1, len(_SQLDF_CACHE) - cfg.web_sqldf_cache_max_entries + 1)]
                for k, _ in oldest:
                    _SQLDF_CACHE.pop(k, None)
        # Store a deep copy to isolate cache
        try:
            _SQLDF_CACHE[key] = (exp, df.copy(deep=True))
        except Exception:
            _SQLDF_CACHE[key] = (exp, df)

def _sql_is_readonly(sql: str) -> bool:
    """Best-effort check that a query is read-only (SELECT/WITH).

    Strips leading comments and whitespace, then checks the first token.
    """
    try:
        s = sql.strip()
        # Remove simple leading line comments
        lines = []
        for line in s.splitlines():
            ls = line.lstrip()
            if ls.startswith("--"):
                # skip pure comment lines
                continue
            lines.append(line)
        s2 = "\n".join(lines).strip().upper()
        # Handle parentheses around CTEs
        while s2.startswith("("):
            s2 = s2[1:].lstrip()
        return s2.startswith("SELECT") or s2.startswith("WITH")
    except Exception:
        return False

from .realtime import bus as event_bus


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
            # Pass the local SSE event bus so the subscriber can publish UI updates
            run_subscriber(db, duration=None, event_callback=event_bus.publish)
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

@app.route('/stream')
def stream():
    """Server-Sent Events (SSE) endpoint."""
    # Get topics from client, e.g. /stream?topic=pool_total_changed&topic=product_status_changed
    # Accept either repeated topic params (?topic=...) or a comma-separated 'topics' param
    topics = request.args.getlist('topic')
    if not topics:
        topics_csv = (request.args.get('topics') or '').strip()
        if topics_csv:
            topics = [t for t in topics_csv.split(',') if t]
    if not topics:
        return Response("No topics specified.", status=400, mimetype='text/plain')

    def event_generator():
        sub = event_bus.subscribe(topics)
        try:
            while True:
                item = sub.get(timeout=25)
                if item is None:
                    yield ": keep-alive\n\n"
                else:
                    channel, data = item
                    yield f"event: {channel}\ndata: {json.dumps(data)}\n\n"
        finally:
            event_bus.unsubscribe(sub)
    
    return Response(event_generator(), mimetype='text/event-stream')


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
    # auto f-share (fix units): approximate others' stake on a single permutation as O/C
    # Your stake on a single permutation is l -> f_auto = l / (l + O/C) = (C*l) / (O + C*l)
    denom = (O + C * l)
    f_auto = float((C * l) / denom) if (C > 0 and denom > 0) else 0.0
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

    # Optional safety: enforce read-only queries from web layer
    enforce_readonly = kwargs.get("read_only", True)
    if enforce_readonly and not _sql_is_readonly(sql):
        raise PermissionError("sql_df only allows SELECT/WITH queries from the web layer.")

    cache_ttl = kwargs.get("cache_ttl")
    if cache_ttl is None:
        cache_ttl = max(0, int(cfg.web_sqldf_cache_ttl_s))

    q, qp = _build_bq_query(sql, params)
    # Cache by final SQL + original params signature
    ck = _sqldf_cache_key(q, params)
    if cache_ttl > 0:
        hit = _sqldf_cache_get(ck)
        if hit is not None:
            return hit

    job_config = bigquery.QueryJobConfig(
        default_dataset=f"{cfg.bq_project}.{cfg.bq_dataset}",
        query_parameters=qp,
    )
    db = get_db()
    it = db.query(q, job_config=job_config)
    # Prefer BQ Storage API if enabled; graceful fallback
    use_bqs = bool(cfg.bq_use_storage_api)
    try:
        df = it.to_dataframe(create_bqstorage_client=use_bqs)
    except Exception:
        df = it.to_dataframe(create_bqstorage_client=False)

    # Cache the full dataframe
    _sqldf_cache_set(ck, df, cache_ttl)
    return df

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

@app.template_filter('f0')
def _fmt_f0(v):
    """Format a value as 0 decimal float; safe for Decimal/str/None."""
    try:
        return f"{float(v):.0f}"
    except Exception:
        return "0"

@app.template_filter('f2')
def _fmt_f2(v):
    """Format a value as 2 decimal float; safe for Decimal/str/None."""
    try:
        return f"{float(v):.2f}"
    except Exception:
        return "0.00"

@app.template_filter('pct1')
def _fmt_pct1(v):
    """Format 0-1 (or 0-100) as percentage with 1 decimal place."""
    try:
        x = float(v)
        if x > 1.0:
            x = x / 100.0
        return f"{x*100.0:.1f}%"
    except Exception:
        return "—"

@app.route("/")
def index():
    """Simplified dashboard with quick links and bank widget."""
    from datetime import datetime, timedelta, timezone

    # Bankroll placeholder sourced from config
    bankroll = cfg.paper_starting_bankroll

    # UI Improvement: Show all upcoming events in the next 24 hours, not just horse racing in the next 60 mins.
    now = datetime.now(timezone.utc)
    start_iso = now.isoformat(timespec='seconds').replace('+00:00', 'Z')
    end_iso = (now + timedelta(hours=24)).isoformat(timespec='seconds').replace('+00:00', 'Z')

    ev_sql = (
        "SELECT event_id, name, venue, country, start_iso, sport, status "
        "FROM `autobet-470818.autobet.tote_events` WHERE start_iso BETWEEN @start AND @end "
        "ORDER BY start_iso ASC LIMIT 100"
    )
    evs = sql_df(ev_sql, params={"start": start_iso, "end": end_iso})

    # Upcoming Superfecta (prefer 60m GB view with breakeven); graceful fallback if missing.
    try:
        sfs = sql_df(
            "SELECT product_id, event_id, event_name, venue, country, start_iso, status, currency, COALESCE(total_net,0.0) AS total_net " +
            "FROM `autobet-470818.autobet.vw_gb_open_superfecta_next60_be` ORDER BY start_iso LIMIT 20",
            cache_ttl=0,
        )
    except Exception:
        # Fallback: next 60 minutes using latest totals view
        sf_fallback = (
            "SELECT p.product_id, p.event_id, p.event_name, COALESCE(e.venue,p.venue) AS venue, "
            "UPPER(COALESCE(e.country,p.currency)) AS country, p.start_iso, COALESCE(p.status,'') AS status, p.currency, COALESCE(p.total_net,0.0) AS total_net "
            "FROM `autobet-470818.autobet.vw_products_latest_totals` p LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id) "
            "WHERE UPPER(p.bet_type)='SUPERFECTA' "
            "AND TIMESTAMP(p.start_iso) BETWEEN CURRENT_TIMESTAMP() AND TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE) "
            "AND UPPER(COALESCE(e.country,p.currency))='GB' "
            "ORDER BY p.start_iso ASC LIMIT 20"
        )
        sfs = sql_df(sf_fallback, cache_ttl=10)

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
        where.append("SUBSTR(start_iso,1,10) >= ?"); params.append(date_from)
    if date_to:
        where.append("SUBSTR(start_iso,1,10) <= ?"); params.append(date_to)
    base_sql = ( # noqa
        "SELECT event_id, name, venue, country, start_iso, sport, status, competitors_json, home, away "
        "FROM `autobet-470818.autobet.tote_events`"
    )
    count_sql = base_sql.replace("SELECT event_id, name, venue, country, start_iso, sport, status, competitors_json, home, away", "SELECT COUNT(1) AS c")
    if where:
        base_sql += " WHERE " + " AND ".join(where)
        count_sql += " WHERE " + " AND ".join(where)
    # Sort by most recent first by default, unless a 'from' date is given to see upcoming.
    order_by = " ORDER BY start_iso DESC"
    if date_from:
        order_by = " ORDER BY start_iso ASC"
    base_sql += order_by + " LIMIT ? OFFSET ?"
    params_paged = list(params) + [limit, offset]
    # Performance: Simplified total count query.
    # Bypass cache for paged results to avoid any unexpected truncation/staleness
    df = sql_df(base_sql, params=tuple(params_paged), cache_ttl=0)
    total = int(sql_df(count_sql, params=tuple(params), cache_ttl=0).iloc[0]["c"])
    countries_df = sql_df("SELECT DISTINCT country FROM `autobet-470818.autobet.tote_events` WHERE country IS NOT NULL AND country<>'' ORDER BY country")
    sports_df = sql_df("SELECT DISTINCT sport FROM `autobet-470818.autobet.tote_events` WHERE sport IS NOT NULL AND sport<>'' ORDER BY sport")
    venues_df = sql_df("SELECT DISTINCT venue FROM `autobet-470818.autobet.tote_events` WHERE venue IS NOT NULL AND venue<>'' ORDER BY venue")
    events = df.to_dict("records") if not df.empty else []
    for ev in events:
        comps = []
        try: # noqa
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
        venue_options=(venues_df['venue'].tolist() if not venues_df.empty else []),
    )

@app.route("/tote-superfecta", methods=["GET","POST"])
def tote_superfecta_page():
    """List SUPERFECTA products with filters, upcoming widget, and audit helpers."""
    if request.method == "POST":
        flash("Paper betting is disabled. Use Audit placement.", "error")
    # UI Improvement: Default country to GB and add a venue filter.
    country = (request.args.get("country") or os.getenv("DEFAULT_COUNTRY", "GB")).strip().upper()
    venue = (request.args.get("venue") or "").strip()
    # Default to OPEN status unless explicitly filtered to something else.
    status = (request.args.get("status") or "OPEN").strip().upper()
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
        "(SELECT COUNT(1) FROM `autobet-470818.autobet.tote_product_selections` s WHERE s.product_id = p.product_id) AS n_runners "
        "FROM `autobet-470818.autobet.vw_products_latest_totals` p LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id) "
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY p.start_iso ASC LIMIT ?"; params.append(limit)
    try:
        df = sql_df(sql, params=tuple(params))
    except Exception:
        # Fallback to raw tote_products if view not present
        sql2 = sql.replace("FROM `autobet-470818.autobet.vw_products_latest_totals` p", "FROM `autobet-470818.autobet.tote_products` p")
        df = sql_df(sql2, params=tuple(params))

    # Options for filters
    cdf = sql_df("SELECT DISTINCT country FROM `autobet-470818.autobet.tote_events` WHERE country IS NOT NULL AND country<>'' ORDER BY country")
    # UI Improvement: Dynamic venue options based on other filters for a better user experience.
    vdf_sql = "SELECT DISTINCT COALESCE(e.venue, p.venue) AS venue FROM `autobet-470818.autobet.tote_products` p LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id) WHERE UPPER(p.bet_type)='SUPERFECTA' AND COALESCE(e.venue, p.venue) IS NOT NULL AND COALESCE(e.venue, p.venue) <> ''"
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

@app.route("/api/admin/refresh_product_status", methods=["GET","POST"])
def api_admin_refresh_product_status():
    """Backfill product selling status from Tote API into BigQuery.

    - If `event_id` is provided (form/query/json), refresh only that event.
    - Else refresh by `date` (YYYY-MM-DD, default today UTC) using the paginated
      products(date:) query.

    This writes rows into `tote_product_status_log` via the sink, which mirrors the
    latest status onto `tote_products.status` used by the UI/views.
    """
    try:
        payload = request.get_json(silent=True) or {}
    except Exception:
        payload = {}
    date_str = (payload.get("date") or request.values.get("date") or _today_iso()).strip()
    event_id = (payload.get("event_id") or request.values.get("event_id") or "").strip()
    first = int(payload.get("first") or request.values.get("first") or 500)

    try:
        from .providers.tote_api import ToteClient
        client = ToteClient()
    except Exception as e:
        return app.response_class(json.dumps({"ok": False, "error": f"ToteClient init failed: {e}"}), mimetype="application/json", status=500)

    import time as _t
    ts_ms = int(_t.time() * 1000)
    rows: list[dict] = []

    try:
        if event_id:
            q = (
                "query GetEvent($id: String){\n"
                "  event(id:$id){ products{ nodes{ id ... on BettingProduct { selling{ status } } } } }\n"
                "}"
            )
            data = client.graphql(q, {"id": event_id})
            nodes = (((data.get("event") or {}).get("products") or {}).get("nodes")) or []
            for n in nodes:
                pid = n.get("id"); st = (((n.get("selling") or {}).get("status")) or "")
                if pid and st:
                    rows.append({"product_id": str(pid), "ts_ms": ts_ms, "status": str(st)})
        else:
            q = (
                "query GetProducts($date: Date, $first: Int, $after: String){\n"
                "  products(date:$date, first:$first, after:$after){ pageInfo{ hasNextPage endCursor }\n"
                "    nodes{ id ... on BettingProduct { selling{ status } } } }\n"
                "}"
            )
            after = None
            while True:
                vars = {"date": date_str, "first": int(first)}
                if after:
                    vars["after"] = after
                data = client.graphql(q, vars)
                prod = (data.get("products") or {})
                nodes = (prod.get("nodes") or [])
                for n in nodes:
                    pid = n.get("id"); st = (((n.get("selling") or {}).get("status")) or "")
                    if pid and st:
                        rows.append({"product_id": str(pid), "ts_ms": ts_ms, "status": str(st)})
                pi = (prod.get("pageInfo") or {})
                if pi.get("hasNextPage") and pi.get("endCursor"):
                    after = pi.get("endCursor"); continue
                break
    except Exception as e:
        return app.response_class(json.dumps({"ok": False, "error": f"Tote API error: {e}"}), mimetype="application/json", status=502)

    if not rows:
        return app.response_class(json.dumps({"ok": True, "updated": 0}), mimetype="application/json")
    try:
        db = get_db()
        db.upsert_tote_product_status_log(rows)
        return app.response_class(json.dumps({"ok": True, "updated": len(rows), "date": date_str or None, "event_id": event_id or None}), mimetype="application/json")
    except Exception as e:
        return app.response_class(json.dumps({"ok": False, "error": f"BQ upsert failed: {e}"}), mimetype="application/json", status=500)

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
    # - Venue filter options are now dynamic based on selected date and country. # noqa
    base_sql = ( # noqa
        "SELECT p.product_id, UPPER(p.bet_type) AS bet_type, COALESCE(p.status,'') AS status, "
        "COALESCE(e.venue, p.venue) AS venue, UPPER(COALESCE(e.country,p.currency)) AS country, p.start_iso, "
        "p.currency, "
        "COALESCE(SAFE_CAST(p.total_gross AS FLOAT64), 0.0) AS total_gross, "
        "COALESCE(SAFE_CAST(p.total_net AS FLOAT64), 0.0) AS total_net, "
        "COALESCE(SAFE_CAST(p.rollover AS FLOAT64), 0.0) AS rollover, "
        "COALESCE(SAFE_CAST(p.deduction_rate AS FLOAT64), 0.0) AS deduction_rate, "
        "p.event_id, COALESCE(e.name, p.event_name) AS event_name, "
        "divs.c AS dividend_count, " +
        "rc.going, rc.weather_temp_c, rc.weather_wind_kph, rc.weather_precip_mm " +
        "FROM `autobet-470818.autobet.vw_products_latest_totals` p " +
        "LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id) " +
        "LEFT JOIN (SELECT product_id, COUNT(1) AS c FROM `autobet-470818.autobet.tote_product_dividends` GROUP BY product_id) divs ON divs.product_id = p.product_id " +
        "LEFT JOIN `autobet-470818.autobet.race_conditions` rc ON rc.event_id = p.event_id "
    )
    where = ["COALESCE(p.status,'') <> ''"]
    params: list[object] = []
    if bet_type: # noqa
        where.append("UPPER(p.bet_type)=?"); params.append(bet_type)
    if country: # noqa
        where.append("UPPER(COALESCE(e.country,p.currency))=?"); params.append(country)
    if status: # noqa
        where.append("UPPER(COALESCE(p.status,''))=?"); params.append(status)
    if venue: # noqa
        where.append("UPPER(COALESCE(e.venue,p.venue)) LIKE UPPER(?)"); params.append(f"%{venue}%")
    if date_filter:
        where.append("SUBSTR(p.start_iso,1,10)=?"); params.append(date_filter)

    filtered_sql = base_sql + " WHERE " + " AND ".join(where)

    # Total count
    count_sql = "SELECT COUNT(1) AS c FROM (" + filtered_sql + ") AS sub"
    total = int(sql_df(count_sql, params=tuple(params)).iloc[0]["c"])

    # Page slice, ascending (earliest at top)
    paged_sql = filtered_sql + " ORDER BY p.start_iso ASC LIMIT ? OFFSET ?"
    paged_params = list(params) + [limit, offset]
    df = sql_df(paged_sql, params=tuple(paged_params))

    # Options for filters # noqa
    types_df = sql_df("SELECT DISTINCT TRIM(UPPER(bet_type)) AS bet_type FROM `autobet-470818.autobet.tote_products` WHERE bet_type IS NOT NULL AND TRIM(bet_type) <> '' ORDER BY bet_type")
    curr_df = sql_df("SELECT DISTINCT UPPER(COALESCE(e.country,p.currency)) AS country FROM `autobet-470818.autobet.tote_products` p LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id) WHERE COALESCE(e.country,p.currency) IS NOT NULL AND COALESCE(e.country,p.currency) <> '' ORDER BY country")
    venues_df_sql = (
        "SELECT DISTINCT COALESCE(e.venue,p.venue) AS venue FROM `autobet-470818.autobet.tote_products` p LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id) "
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

    # Group totals per bet type (respecting filters except pagination) # noqa
    gt_sql = "SELECT UPPER(bet_type) AS bet_type, COUNT(1) AS n, SUM(total_net) AS sum_net FROM (" + filtered_sql + ") AS sub GROUP BY bet_type ORDER BY bet_type"
    gt = sql_df(gt_sql, params=tuple(params))

    prods = df.to_dict("records") if not df.empty else []
    # Aggregate totals before formatting values for display
    total_net = sum(float(p.get('total_net') or 0.0) for p in prods)
    for p in prods: # noqa
        p["dividend_count"] = int(p.get("dividend_count") or 0)
        # Guard against bad data for deduction_rate (should be a fraction)
        if p.get("deduction_rate") and p.get("deduction_rate") > 1.0:
            p["deduction_rate"] /= 100.0
        # Attach conditions badge
        t = p.get("weather_temp_c"); w = p.get("weather_wind_kph"); pr = p.get("weather_precip_mm")
        if pd.notnull(t) or pd.notnull(w) or pd.notnull(pr):
            p["weather_badge"] = f"{'' if pd.isnull(t) else int(round(t))}°C {'' if pd.isnull(w) else int(round(w))}kph {'' if pd.isnull(pr) else pr:.1f}mm"
        else:
            p["weather_badge"] = None
        # Do not pre-format numeric fields; templates handle formatting.
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
    by_type = sql_df("SELECT UPPER(bet_type) AS bet_type, COUNT(1) AS n FROM `autobet-470818.autobet.tote_products` GROUP BY 1 ORDER BY n DESC")
    by_status = sql_df("SELECT COALESCE(status,'') AS status, COUNT(1) AS n FROM `autobet-470818.autobet.tote_products` GROUP BY 1 ORDER BY n DESC")
    by_country = sql_df("SELECT COALESCE(currency,'') AS country, COUNT(1) AS n, ROUND(AVG(total_net),2) AS avg_total_net, ROUND(MAX(total_net),2) AS max_total_net FROM `autobet-470818.autobet.tote_products` GROUP BY 1 ORDER BY n DESC")
    recent_sql = (
        "SELECT UPPER(bet_type) AS bet_type, COUNT(1) AS n, ROUND(AVG(total_net),2) AS avg_total_net, "
        "APPROX_QUANTILES(total_net, 5)[SAFE_OFFSET(2)] AS p50, "
        "APPROX_QUANTILES(total_net, 5)[SAFE_OFFSET(4)] AS p90 "
        "FROM `autobet-470818.autobet.tote_products` "
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
    f_share = f_fix  # Use override for f_share if provided

    # Map bet type to permutation K and clamp top_n
    k_map = {"WIN": 1, "EXACTA": 2, "TRIFECTA": 3, "SUPERFECTA": 4, "SWINGER": 2}
    k_perm = int(k_map.get(bet_type, 4))
    top_n = max(k_perm, min(10, req_top_n))

    # --- Filter Options ---
    countries_df = sql_df("SELECT DISTINCT country FROM `autobet-470818.autobet.tote_events` WHERE country IS NOT NULL AND country<>'' ORDER BY country")
    # UI Improvement: Dynamic venue/course options based on other active filters.
    venues_df_sql = (
        "SELECT DISTINCT COALESCE(e.venue,p.venue) AS venue FROM `autobet-470818.autobet.tote_products` p LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id) "
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
          (SELECT COUNT(1) FROM `autobet-470818.autobet.tote_product_selections` s WHERE s.product_id = p.product_id) AS n_runners
        FROM `autobet-470818.autobet.vw_products_latest_totals` p
        LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id)
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
        prod_df = sql_df("SELECT * FROM `autobet-470818.autobet.tote_products` WHERE product_id=?", params=(product_id,))
        if not prod_df.empty:
            prod = prod_df.iloc[0].to_dict()

        # Define t_val for breakeven calculation, using product's deduction_rate as default
        t_default = float(prod.get("deduction_rate") or 0.30) if prod else 0.30
        t_val = float(take_rate_in) if take_rate_in not in (None, "") else t_default
        # Define R_val for breakeven calculation
        R_val = float(net_rollover_in) if net_rollover_in not in (None, "") else (float(prod.get("rollover") or 0.0) if prod else 0.0)

        df_sel = sql_df((
            "SELECT selection_id, competitor, number, total_units FROM `autobet-470818.autobet.tote_product_selections` WHERE product_id=? AND leg_index=1 ORDER BY number"
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
                    "SELECT CAST(cloth_number AS INT64) AS number, CAST(decimal_odds AS FLOAT64) AS decimal_odds FROM `autobet-470818.autobet.vw_tote_probable_odds` WHERE product_id = ?",
                    params=(product_id,)
                )
                # If empty, map via event_id to the WIN product for the same event
                if (bq_probs.empty) and prod and prod.get('event_id'):
                    bq_probs = sql_df(
                        "SELECT CAST(o.cloth_number AS INT64) AS number, CAST(o.decimal_odds AS FLOAT64) AS decimal_odds "
                        "FROM `autobet-470818.autobet.vw_tote_probable_odds` o JOIN `autobet-470818.autobet.tote_products` p ON p.product_id = o.product_id "
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
        # Skip legacy local fallback; rely solely on BigQuery probable odds or selection units
        # If no runners found, the calculators will show a warning below.
        # Prefer BigQuery permutation function for SUPERFECTA
        used_bq_perms = False
        if _use_bq() and bet_type == "SUPERFECTA":
            try:
                # Prefer the *_any function which works with predictions or odds-derived strengths
                perms_df = sql_df(
                    f"SELECT * FROM `{cfg.bq_project}.{cfg.bq_dataset}.tf_superfecta_perms_any`(@pid, @top_n)",
                    params={"pid": product_id, "top_n": top_n},
                    cache_ttl=0,
                )
                if (perms_df is None) or perms_df.empty:
                    # Fallback to the horse_any variant if available (uses event-level WIN odds mapping)
                    try:
                        perms_df = sql_df(
                            f"SELECT * FROM `{cfg.bq_project}.{cfg.bq_dataset}.tf_superfecta_perms_horse_any`(@pid, @top_n)",
                            params={"pid": product_id, "top_n": top_n},
                            cache_ttl=0,
                        )
                    except Exception:
                        perms_df = None
                if perms_df is not None and not perms_df.empty:
                    # Sort by probability descending
                    perms_df = perms_df.sort_values("p", ascending=False).reset_index(drop=True)
                    total_lines = int(len(perms_df))
                    target_lines = max(1, int(round(total_lines * coverage)))
                    # Compute cumulative coverage and take top 30 to show
                    cum_vals = perms_df["p"].cumsum()
                    cum_p = float(cum_vals.iloc[target_lines - 1]) if target_lines <= total_lines else float(cum_vals.iloc[-1])
                    rows = []
                    show_n = min(30, total_lines)
                    for i in range(show_n):
                        r = perms_df.iloc[i]
                        rows.append({
                            "rank": i + 1,
                            "h1": r.get("h1"),
                            "h2": r.get("h2"),
                            "h3": r.get("h3"),
                            "h4": r.get("h4"),
                            "p": float(r.get("p") or 0.0),
                            "cum_p": float(cum_vals.iloc[i]),
                        })
                    top_lines = rows
                    random_cov = target_lines / float(total_lines or 1)
                    efficiency = (cum_p / random_cov) if random_cov > 0 else None
                    # Breakeven using S lines
                    S = stake_per_line * float(target_lines)
                    try:
                        if f_share is not None and f_share > 0 and (1.0 - t_val) > 0:
                            o_min_val = (S / (f_share * (1.0 - t_val))) - S
                        else:
                            o_min_val = None
                    except Exception:
                        o_min_val = None
                    o_min = o_min_val
                    used_bq_perms = True
            except Exception as e:
                # Fall through to local calculation if BQ fails
                try:
                    flash(f"BigQuery permutation function error: {e}", "warning")
                except Exception:
                    pass

        # Fallback: local PL enumeration (only if BQ not used)
        if not used_bq_perms:
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
                        "date": date_filter,
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
                    venue=venue,
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
                        "date": date_filter,
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
                    venue=venue,
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
                # Safe calculation for O_min
                if f_share is not None and f_share > 0 and (1.0 - t_val) > 0:
                    o_min_val = (S / (f_share * (1.0 - t_val))) - S
                else:
                    o_min_val = None
            except Exception:
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
        venue=venue,
    )

@app.route("/api/models/backtest")
def api_models_backtest():
    """Run the BigQuery backtest and return JSON summary + sample rows.

    Query params:
      start: YYYY-MM-DD (default: 2024-01-01)
      end:   YYYY-MM-DD (default: today)
      top_n: int (default: 10)
      coverage: float (0..1, default: 0.60)
      limit: int (default: 200)
    """
    if not _use_bq():
        return app.response_class(json.dumps({"error": "BigQuery not configured"}), mimetype="application/json", status=400)
    try:
        start = (request.args.get("start") or "2024-01-01").strip()
        end = (request.args.get("end") or _today_iso()).strip()
        try:
            top_n = int(request.args.get("top_n") or 10)
        except Exception:
            top_n = 10
        try:
            coverage = float(request.args.get("coverage") or 0.60)
        except Exception:
            coverage = 0.60
        try:
            limit = max(1, int(request.args.get("limit") or 200))
        except Exception:
            limit = 200

        # Use the horse_any backtest that works with predictions or odds-derived strengths
        tf = f"`{cfg.bq_project}.{cfg.bq_dataset}.tf_sf_backtest_horse_any`"
        rows_df = sql_df(
            f"SELECT * FROM {tf}(@start,@end,@top_n,@cov) ORDER BY start_iso LIMIT @lim",
            params={"start": start, "end": end, "top_n": top_n, "cov": coverage, "lim": limit},
            cache_ttl=0,
        )
        # Summary metrics
        sum_df = sql_df(
            f"""
            WITH r AS (
              SELECT * FROM {tf}(@start,@end,@top_n,@cov)
            )
            SELECT
              COUNT(*) AS races,
              COUNTIF(winner_rank IS NOT NULL) AS with_winner_rank,
              COUNTIF(hit_at_coverage) AS hits_at_cov,
              SAFE_DIVIDE(COUNTIF(hit_at_coverage), NULLIF(COUNT(*),0)) AS hit_rate
            FROM r
            """,
            params={"start": start, "end": end, "top_n": top_n, "cov": coverage},
            cache_ttl=0,
        )
        payload = {
            "params": {"start": start, "end": end, "top_n": top_n, "coverage": coverage},
            "summary": ({} if sum_df.empty else sum_df.iloc[0].to_dict()),
            "rows": ([] if rows_df.empty else rows_df.to_dict("records")),
        }
        return app.response_class(json.dumps(payload), mimetype="application/json")
    except Exception as e:
        return app.response_class(json.dumps({"error": str(e)}), mimetype="application/json", status=500)

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
    cdf = sql_df("SELECT DISTINCT country FROM `autobet-470818.autobet.tote_events` WHERE country IS NOT NULL AND country<>'' ORDER BY country")
    # UI Improvement: Dynamic venue options filtered by country and date.
    vdf_sql = "SELECT DISTINCT venue FROM `autobet-470818.autobet.tote_events` WHERE venue IS NOT NULL AND venue <> '' "
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
          p.product_id, p.event_id, COALESCE(e.name, p.event_name) AS event_name,
          COALESCE(e.venue, p.venue) AS venue,
          p.start_iso, p.currency,
          COALESCE(SAFE_CAST(p.total_gross AS FLOAT64), 0.0) AS total_gross,
          COALESCE(SAFE_CAST(p.total_net AS FLOAT64), 0.0) AS total_net,
          COALESCE(p.status,'') AS status,
          (SELECT COUNT(1) FROM `autobet-470818.autobet.tote_product_selections` s WHERE s.product_id = p.product_id) AS n_runners
        FROM `autobet-470818.autobet.vw_products_latest_totals` p
        LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id)
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
        pdf = sql_df("SELECT * FROM vw_products_latest_totals WHERE product_id=?", params=(product_id,))
        if not pdf.empty:
            prod = pdf.iloc[0].to_dict()

        try:
            # Performance/UI Improvement: Fetch N (runners) and O (latest pool gross) in a single query
            # to reduce latency and ensure the latest pool info is used.
            params_df = sql_df("""
                SELECT
                    (SELECT COUNT(1) FROM `autobet-470818.autobet.tote_product_selections` WHERE product_id=@pid AND leg_index=1) AS n,
                    (SELECT total_gross FROM `autobet-470818.autobet.tote_pool_snapshots` WHERE product_id=@pid ORDER BY ts_ms DESC LIMIT 1) AS latest_gross
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
            # Handle takeout rate, which is a percentage in the form but a fraction in code
            take_rate = fnum("take", calc_params["take_rate"] * 100.0) / 100.0
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
        pdf = sql_df("SELECT * FROM `autobet-470818.autobet.vw_products_latest_totals` WHERE product_id=?", params=(product_id,))
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
                ndf = sql_df("SELECT COUNT(1) AS n FROM `autobet-470818.autobet.tote_product_selections` WHERE product_id=?", params=(product_id,))
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
    rows = sql_df("SELECT DISTINCT selection_id, number, competitor FROM `autobet-470818.autobet.tote_product_selections` WHERE product_id=? AND leg_index=1 ORDER BY number", params=(pid,))
    return app.response_class(json.dumps(rows.to_dict("records") if not rows.empty else []), mimetype="application/json")

@app.route("/api/tote/event_products/<event_id>")
def api_tote_event_products(event_id: str):
    """Return OPEN/SELLING products for a given event.

    Uses latest-totals view when possible; if results look incomplete (e.g., only WIN),
    fetches from Tote GraphQL to enrich the list so the bet page can show all types.
    """
    if not event_id:
        return app.response_class(json.dumps({"error": "missing event_id"}), mimetype="application/json", status=400)

    # Prefer live API for freshest product selling status; fallback to BQ if unavailable
    rows: list[dict] = []
    try:
        from .providers.tote_api import ToteClient
        client = ToteClient()
        q = (
            "query GetEvent($id: String){\n"
            "  event(id:$id){ products{ nodes{ id ... on BettingProduct { betType{ code } selling{ status } } } } }\n"
            "}"
        )
        data = client.graphql(q, {"id": event_id})
        nodes = (((data.get("event") or {}).get("products") or {}).get("nodes")) or []
        for n in nodes:
            bt = (((n.get("betType") or {}).get("code")) or "").upper()
            st = (((n.get("selling") or {}).get("status")) or "")
            pid = n.get("id")
            if bt and pid and (st or "").upper() in ("OPEN","SELLING"):
                rows.append({"product_id": pid, "bet_type": bt, "status": st})
        rows = sorted(rows, key=lambda r: (r.get('bet_type') or ''))
    except Exception:
        # Fallback to BQ view, then base table, relaxing status if needed
        try:
            vdf = sql_df(
                "SELECT product_id, bet_type, COALESCE(status,'') AS status FROM `autobet-470818.autobet.vw_products_latest_totals` "
                "WHERE event_id=? AND UPPER(COALESCE(status,'')) IN ('OPEN','SELLING') ORDER BY bet_type",
                params=(event_id,), cache_ttl=0,
            )
            rows = ([] if vdf.empty else vdf.to_dict("records"))
        except Exception:
            rows = []
        if not rows:
            try:
                tdf = sql_df(
                    "SELECT product_id, bet_type, COALESCE(status,'') AS status FROM `autobet-470818.autobet.tote_products` "
                    "WHERE event_id=? AND UPPER(COALESCE(status,'')) IN ('OPEN','SELLING') ORDER BY bet_type",
                    params=(event_id,), cache_ttl=0,
                )
                rows = ([] if tdf.empty else tdf.to_dict("records"))
            except Exception:
                rows = []
        if not rows:
            try:
                anydf = sql_df(
                    "SELECT product_id, bet_type, COALESCE(status,'') AS status FROM `autobet-470818.autobet.tote_products` "
                    "WHERE event_id=? ORDER BY bet_type",
                    params=(event_id,), cache_ttl=0,
                )
                rows = ([] if anydf.empty else anydf.to_dict("records"))
            except Exception:
                rows = []

    return app.response_class(json.dumps(rows or []), mimetype="application/json")

@app.route("/api/tote/pool_snapshot/<product_id>")
def api_tote_pool_snapshot(product_id: str):
    """Return latest pool snapshot; if absent, fall back to tote_products totals.

    Avoids noisy 404s by providing best-effort data for UI auto-refresh.
    """
    # Query latest snapshot from BQ
    try:
        ss = sql_df(
            "SELECT product_id, event_id, currency, status, start_iso, ts_ms, total_gross, total_net, rollover, deduction_rate, 'snap' AS source "
            "FROM `autobet-470818.autobet.tote_pool_snapshots` WHERE product_id=? ORDER BY ts_ms DESC LIMIT 1",
            params=(product_id,),
            cache_ttl=0,
        )
        if not ss.empty:
            return app.response_class(ss.iloc[0].to_json(), mimetype="application/json")
        # Fallback to products
        pdf = sql_df(
            "SELECT product_id, event_id, currency, status, start_iso, total_gross, total_net, rollover, deduction_rate, NULL AS ts_ms, 'products_fallback' AS source "
            "FROM `autobet-470818.autobet.vw_products_latest_totals` WHERE product_id=?",
            params=(product_id,),
            cache_ttl=0,
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
    mode = request.form.get("mode", "audit")
    if mode != "live":
        mode = "audit"
    # Gate live placement behind env flag
    if mode == "live" and (os.getenv("TOTE_LIVE_ENABLED", "0").lower() not in ("1","true","yes","on")):
        flash("Live betting disabled (TOTE_LIVE_ENABLED not set). Using audit mode.", "warning")
        mode = "audit"
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

    # Explicitly create a ToteClient with the correct endpoint based on the mode.
    # This ensures audit bets are sent to the audit endpoint, preventing "Failed to fund ticket" errors.
    from .providers.tote_api import ToteClient
    client_for_placement = ToteClient()
    if mode == 'audit':
        # Use configured audit endpoint if provided; otherwise leave as default
        if cfg.tote_audit_graphql_url:
            client_for_placement.base_url = cfg.tote_audit_graphql_url.strip()
    elif mode == 'live':
        # Use configured live endpoint as-is
        if cfg.tote_graphql_url:
            client_for_placement.base_url = cfg.tote_graphql_url.strip()

    # NOTE: Assuming place_audit_superfecta accepts a 'client' argument to use for the API call.
    res = place_audit_superfecta(db, mode=mode, product_id=pid, selection=(sel or None), selections=selections, stake=sk, currency=currency, post=post_flag, stake_type=stake_type, placement_product_id=placement_pid, client=client_for_placement)
    st = res.get("placement_status")
    err_msg = res.get("error")
    if err_msg:
        if "Failed to fund ticket" in str(err_msg):
            flash("Live bet placement failed: Insufficient funds in the Tote account.", "error")
        else:
            flash(f"Audit bet error: {err_msg}", "error")
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
    """List recent audit bets from the local BigQuery table with filters."""
    if not _use_bq():
        flash("Audit bets page requires BigQuery.", "error")
        return redirect(url_for('index'))

    # Filters
    country = (request.args.get("country") or "").strip().upper()
    venue = (request.args.get("venue") or "").strip()
    limit = max(1, int(request.args.get("limit", "100") or 100))
    page = max(1, int(request.args.get("page", "1") or 1))
    offset = (page - 1) * limit

    # Build query
    where = []
    params: dict[str, Any] = {}
    if country:
        where.append("UPPER(COALESCE(e.country, p.currency)) = @country")
        params["country"] = country
    if venue:
        where.append("UPPER(COALESCE(e.venue, p.venue)) LIKE UPPER(@venue)")
        params["venue"] = f"%{venue}%"

    base_sql = """
        SELECT
          b.bet_id, b.ts_ms, b.mode, b.status, b.selection, b.stake, b.currency, b.response_json,
          p.event_id,
          p.event_name,
          p.start_iso,
          COALESCE(e.venue, p.venue) AS venue,
          COALESCE(e.country, p.currency) AS country
        FROM tote_audit_bets b
        LEFT JOIN tote_products p ON b.product_id = p.product_id
        LEFT JOIN tote_events e ON p.event_id = e.event_id
    """
    count_sql = "SELECT COUNT(1) AS c FROM tote_audit_bets b LEFT JOIN tote_products p ON b.product_id = p.product_id LEFT JOIN tote_events e ON p.event_id = e.event_id"

    if where:
        where_clause = " WHERE " + " AND ".join(where)
        base_sql += where_clause
        count_sql += where_clause

    total = 0
    live_bets = []
    audit_bets = []
    try:
        # Total count
        total_df = sql_df(count_sql, params=params)
        total = int(total_df.iloc[0]['c']) if not total_df.empty else 0

        # Paged query
        paged_sql = base_sql + " ORDER BY b.ts_ms DESC LIMIT @limit OFFSET @offset"
        params_paged = {**params, "limit": limit, "offset": offset}
        df = sql_df(paged_sql, params=params_paged)
        
        all_bets = []
        for _, row in df.iterrows():
            bet_data = row.to_dict()
            bet_data['created'] = bet_data.get('ts_ms')
            # Provide ISO string for client-side local time rendering
            try:
                ts_ms_val = int(bet_data.get('ts_ms')) if bet_data.get('ts_ms') is not None else None
                if ts_ms_val is not None:
                    bet_data['created_iso'] = datetime.utcfromtimestamp(ts_ms_val/1000.0).strftime('%Y-%m-%dT%H:%M:%SZ')
                else:
                    bet_data['created_iso'] = None
            except Exception:
                bet_data['created_iso'] = None
            bet_data['tote_bet_id'] = None  # Default

            try:
                if row.get('response_json'):
                    resp_data = json.loads(row['response_json'])
                    resp = resp_data.get('response', {})
                    if resp:
                        ticket = (resp.get("placeBets") or {}).get("ticket")
                        if ticket and isinstance(ticket, dict):
                            nodes = (ticket.get("bets") or {}).get("nodes")
                            if nodes and isinstance(nodes, list) and nodes:
                                bet_data['tote_bet_id'] = nodes[0].get('toteId')
                        results = (resp.get("placeBets") or {}).get("results")
                        if results and isinstance(results, list) and results:
                            bet_data['tote_bet_id'] = results[0].get('toteBetId')
            except (json.JSONDecodeError, TypeError, KeyError, IndexError):
                pass
            all_bets.append(bet_data)

        live_bets = [b for b in all_bets if b.get('mode') == 'live']
        audit_bets = [b for b in all_bets if b.get('mode') != 'live']

    except Exception as e:
        if 'Not found: Table' in str(e):
            flash("The 'tote_audit_bets' table was not found in BigQuery. Place an audit bet to create it.", "info")
        else:
            flash(f"Error fetching audit bets from BigQuery: {e}", "error")
            traceback.print_exc()
        live_bets = []
        audit_bets = []

    # Options for filters
    try:
        countries_df = sql_df("SELECT DISTINCT country FROM tote_events WHERE country IS NOT NULL AND country<>'' ORDER BY country")
        venues_df = sql_df("SELECT DISTINCT venue FROM tote_events WHERE venue IS NOT NULL AND venue<>'' ORDER BY venue")
    except Exception:
        countries_df = pd.DataFrame()
        venues_df = pd.DataFrame()

    return render_template(
        "audit_bets.html",
        live_bets=live_bets,
        audit_bets=audit_bets,
        filters={
            "country": country, "venue": venue,
            "limit": limit, "page": page, "total": total,
        },
        country_options=(countries_df['country'].tolist() if not countries_df.empty else []),
        venue_options=(venues_df['venue'].tolist() if not venues_df.empty else []),
    )

@app.route("/audit/bets/")
def audit_bets_page_slash():
    return audit_bets_page()
@app.route("/audit/")
def audit_root():
    return redirect(url_for('audit_bets_page'))

@app.route("/audit/bets/<bet_id>")
def audit_bet_detail_page(bet_id: str):
    """Show stored request/response for an audit bet and allow a best-effort status refresh."""
    if not _use_bq():
        flash("Audit bet details require BigQuery.", "error")
        return redirect(url_for('audit_bets_page'))

    detail = None
    try:
        df = sql_df("SELECT * FROM tote_audit_bets WHERE bet_id = ?", params=(bet_id,))
        if not df.empty:
            detail = df.iloc[0].to_dict()
            # Compute ISO for local time rendering
            try:
                ts_ms_val = int(detail.get('ts_ms')) if detail.get('ts_ms') is not None else None
                if ts_ms_val is not None:
                    detail['_created_iso'] = datetime.utcfromtimestamp(ts_ms_val/1000.0).strftime('%Y-%m-%dT%H:%M:%SZ')
                else:
                    detail['_created_iso'] = None
            except Exception:
                detail['_created_iso'] = None
            # Parse stored request/response for pretty printing in the template
            try:
                rr = json.loads(detail.get("response_json") or "{}")
                req = rr.get("request")
                resp = rr.get("response")
                detail["_req_pretty"] = json.dumps(req, indent=2, ensure_ascii=False) if req is not None else None
                detail["_resp_pretty"] = json.dumps(resp, indent=2, ensure_ascii=False) if resp is not None else None
            except Exception:
                detail["_req_pretty"] = detail.get("response_json")  # fallback to raw
                detail["_resp_pretty"] = None
    except Exception as e:
        flash(f"Error fetching bet detail: {e}", "error")

    if detail is None:
        flash(f"Audit bet with ID '{bet_id}' not found.", "error")
        return redirect(url_for('audit_bets_page'))

    return render_template("audit_bet_detail.html", bet=detail)

@app.route("/status")
def status_page():
    """Enhanced status dashboard (v2) showing GCP + data freshness."""
    return render_template("status_v2.html")


@app.get("/api/status/data_freshness")
def api_status_data_freshness():
    """Return counts and last-ingest timestamps for key tables."""
    try:
        # Events: count by start date; last ingest from job runs
        ev_today = sql_df("SELECT COUNT(1) AS c FROM tote_events WHERE SUBSTR(start_iso,1,10)=FORMAT_DATE('%F', CURRENT_DATE())")
        ev_last = sql_df(
            "SELECT TIMESTAMP_MILLIS(MAX(ended_ts)) AS ts FROM ingest_job_runs WHERE task IN ('ingest_events_for_day','ingest_events_range') AND status='OK'"
        )

        # Products: count by start date; last ingest from job runs
        pr_today = sql_df("SELECT COUNT(1) AS c FROM tote_products WHERE SUBSTR(start_iso,1,10)=FORMAT_DATE('%F', CURRENT_DATE())")
        pr_last = sql_df(
            "SELECT TIMESTAMP_MILLIS(MAX(ended_ts)) AS ts FROM ingest_job_runs WHERE task IN ('ingest_products_for_day','ingest_single_product') AND status='OK'"
        )

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


@app.get("/api/status/qc")
def api_status_qc():
    """Return QC gaps and counts from QC views."""
    try:
        out: dict[str, Any] = {}
        # Missing runner numbers today
        try:
            df = sql_df("SELECT COUNT(DISTINCT product_id) AS c FROM vw_qc_today_missing_runner_numbers")
            out["missing_runner_numbers"] = int(df.iloc[0]["c"]) if not df.empty else 0
        except Exception:
            out["missing_runner_numbers"] = None
        # Missing bet rules
        try:
            df = sql_df("SELECT COUNT(1) AS c FROM vw_qc_missing_bet_rules")
            out["missing_bet_rules"] = int(df.iloc[0]["c"]) if not df.empty else 0
        except Exception:
            out["missing_bet_rules"] = None
        # Probable odds coverage (average across products)
        try:
            df = sql_df("SELECT AVG(coverage) AS avg_cov FROM vw_qc_probable_odds_coverage")
            out["probable_odds_avg_cov"] = float(df.iloc[0]["avg_cov"]) if not df.empty else None
        except Exception:
            out["probable_odds_avg_cov"] = None
        # Today's GB Superfecta missing snapshots
        try:
            df = sql_df("SELECT COUNT(1) AS c FROM vw_qc_today_gb_sf_missing_snapshots")
            out["gb_sf_missing_snapshots"] = int(df.iloc[0]["c"]) if not df.empty else 0
        except Exception:
            out["gb_sf_missing_snapshots"] = None
        return app.response_class(json.dumps(out), mimetype="application/json")
    except Exception as e:
        return app.response_class(json.dumps({"error": str(e)}), mimetype="application/json", status=500)


@app.get("/api/status/upcoming")
def api_status_upcoming():
    """Return upcoming races/products in the next 4 hours if view exists."""
    upcoming = []
    try:
        # Prefer 240m GB Superfecta view if available
        df = sql_df("SELECT product_id, event_id, event_name, venue, country, start_iso, status, currency, combos, S, roi_current, viable_now FROM vw_gb_open_superfecta_next240_be ORDER BY start_iso")
        upcoming = df.to_dict("records") if not df.empty else []
    except Exception:
        try:
            # Secondary fallback: older 60m view name
            df = sql_df("SELECT product_id, event_id, event_name, venue, country, start_iso, status, currency, combos, S, roi_current, viable_now FROM vw_gb_open_superfecta_next60_be ORDER BY start_iso")
            upcoming = df.to_dict("records") if not df.empty else []
        except Exception:
            # Final fallback: generic query for open products in the next 4 hours
            now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            df = sql_df(
                "SELECT p.product_id, p.event_id, COALESCE(p.event_name, e.name) AS event_name, e.sport, COALESCE(p.venue, e.venue) AS venue, COALESCE(e.country, p.currency) AS country, p.start_iso, p.status, p.currency, qc.avg_cov "
                "FROM tote_products p "
                "LEFT JOIN tote_events e USING(event_id) "
                "LEFT JOIN vw_qc_probable_odds_coverage qc ON p.product_id = qc.product_id "
                "WHERE p.status='OPEN' AND TIMESTAMP(p.start_iso) BETWEEN TIMESTAMP(@now) AND TIMESTAMP_ADD(TIMESTAMP(@now), INTERVAL 4 HOUR) "
                "ORDER BY p.start_iso",
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

    # Cloud Run services (list all in region)
    try:
        services: list[dict] = []
        url = f"https://run.googleapis.com/v2/projects/{project}/locations/{region}/services"
        r = sess.get(url, timeout=10)
        if r.status_code == 200:
            for j in (r.json().get("services") or []):
                # Normalize conditions map across API variants
                conds = {c.get("type"): c.get("state") or c.get("status") for c in j.get("conditions", [])}
                ok_vals = {"CONDITION_SUCCEEDED", "True", True}
                # Some list responses omit the top-level Ready condition. Consider the
                # service ready if either Ready succeeded OR both RoutesReady and
                # ConfigurationsReady succeeded. As a final hint, the presence of
                # latestReadyRevision typically implies readiness of the latest deploy.
                ready_from_subconds = (conds.get("RoutesReady") in ok_vals) and (conds.get("ConfigurationsReady") in ok_vals)
                ready_from_ready = (conds.get("Ready") in ok_vals)
                ready_flag = bool(ready_from_ready or ready_from_subconds or bool(j.get("latestReadyRevision")))
                services.append({
                    "name": (j.get("name") or "").split("/")[-1],
                    "uri": j.get("uri"),
                    "latestReadyRevision": j.get("latestReadyRevision"),
                    "ready": ready_flag,
                    "updateTime": j.get("updateTime"),
                    "conditions": j.get("conditions", []),
                })
        else:
            out["cloud_run"]["status_code"] = r.status_code
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
    event_df = sql_df("SELECT * FROM tote_events WHERE event_id=?", params=(event_id,), cache_ttl=0)
    if event_df.empty:
        # If event not found, try to get basic info from tote_products
        flash("Event not found", "error")
        return redirect(url_for("tote_events_page"))
    event = event_df.to_dict("records")[0]

    # Fetch conditions
    conditions_df = sql_df("SELECT * FROM race_conditions WHERE event_id=?", params=(event_id,), cache_ttl=0)
    conditions = None if conditions_df.empty else conditions_df.to_dict("records")[0]

    # Fetch historical results and runner details from hr_horse_runs.
    # This is the primary source for runners in an event, especially for historical events with results.
    runner_rows_df = sql_df(
        """
        SELECT
          r.horse_id,
          COALESCE(h.name, r.horse_id) AS horse_name, -- Use horse_id if name is null
          r.finish_pos,
          r.status,
          r.cloth_number,
          r.jockey,
          r.trainer
        FROM hr_horse_runs r
        LEFT JOIN hr_horses h ON h.horse_id = r.horse_id -- Join to get horse name
        WHERE r.event_id = ?
        ORDER BY r.finish_pos NULLS LAST, r.cloth_number ASC
        """,
        params=(event_id,),
        cache_ttl=0,
    )
    runner_rows = runner_rows_df.to_dict("records") if not runner_rows_df.empty else []

    # Populate 'runners' list for display. Prioritize hr_horse_runs, then competitors_json.
    runners = []
    if runner_rows:
        # Use data from hr_horse_runs if available
        for r in runner_rows:
            runners.append({
                "cloth": r.get("cloth_number"),
                "name": r.get("horse_name"),
                "finish_pos": r.get("finish_pos"),
                "status": r.get("status"),
            })
    elif event.get("competitors_json"): # Fallback to competitors_json if no hr_horse_runs
        try: # noqa
            competitors_json_data = json.loads(event["competitors_json"]) or []
            if isinstance(competitors_json_data, list):
                runners = [] # Reset runners if using fallback
                for c in competitors_json_data: # Fix: was iterating over 'competitors' which was not defined
                    # Accept multiple possible keys for cloth/trap numbers
                    cloth = (
                        c.get("cloth") or c.get("cloth_number") or c.get("clothNumber") or c.get("trapNumber") or c.get("number")
                    )
                    name = c.get("name") or c.get("competitor") or c.get("horse")
                    if not name:
                        name = f"Runner #{cloth}" if cloth else None
                    if name:
                        runners.append({"cloth": cloth, "name": name})
                # Sort by cloth number if available
                try:
                    runners.sort(key=lambda x: (x.get("cloth") is None, int(x.get("cloth") or 1)))
                except Exception:
                    pass # Fallback to unsorted if cloth number is not int
        except (json.JSONDecodeError, TypeError):
            runners = [] # Ensure runners is an empty list on error

    # Final fallback: derive runners from WIN product selections if available
    if not runners:
        try:
            dfw = sql_df(
                """
                SELECT CAST(s.number AS INT64) AS cloth, COALESCE(s.competitor, s.selection_id) AS name
                FROM tote_product_selections s
                JOIN tote_products p ON p.product_id = s.product_id
                WHERE p.event_id = ? AND UPPER(p.bet_type) = 'WIN' AND s.leg_index=1
                ORDER BY CAST(s.number AS INT64)
                """,
                params=(event_id,)
            )
            runners = ([] if dfw.empty else dfw.to_dict("records"))
        except Exception:
            pass

    # Fetch products (include pool components)
    products_df = sql_df(
        """
        SELECT product_id, bet_type, status, start_iso, event_id, event_name, venue,
               COALESCE(SAFE_CAST(total_gross AS FLOAT64), 0.0) AS total_gross,
               COALESCE(SAFE_CAST(total_net AS FLOAT64), 0.0) AS total_net,
               COALESCE(SAFE_CAST(rollover AS FLOAT64), 0.0) AS rollover,
               COALESCE(SAFE_CAST(deduction_rate AS FLOAT64), 0.0) AS deduction_rate -- Use deduction_rate directly
        FROM vw_products_latest_totals
        WHERE event_id=?
        ORDER BY bet_type
        """,
        params=(event_id,),
        cache_ttl=0,
    )
    products = products_df.to_dict("records") if not products_df.empty else []
    # Format deduction_rate for display and handle nulls
    for p in products:
        if p.get("deduction_rate") and p["deduction_rate"] > 1.0:
            p["deduction_rate"] /= 100.0
        p["deduction_rate_display"] = f"{p['deduction_rate'] * 100:.1f}%" if pd.notnull(p["deduction_rate"]) else "N/A"

    # Runners with latest probable odds
    runners_prob: list = []
    try: # noqa
        # Simplified query: vw_tote_probable_odds already joins with tote_product_selections
        # to get cloth_number and competitor name.
        rprob = sql_df(
            """
            -- Simplified and more robust query for probable odds
            SELECT
                o.selection_id,
                o.cloth_number,
                COALESCE(s.competitor, o.selection_id) AS horse, -- Fallback to ID if name is missing
                o.decimal_odds,
                FORMAT_TIMESTAMP('%FT%T%Ez', TIMESTAMP_MILLIS(o.ts_ms)) AS odds_iso
            FROM `autobet-470818.autobet.vw_tote_probable_odds` o
            JOIN `autobet-470818.autobet.tote_products` p ON o.product_id = p.product_id
            LEFT JOIN `autobet-470818.autobet.tote_product_selections` s ON o.selection_id = s.selection_id AND o.product_id = p.product_id
            WHERE p.event_id = @event_id AND UPPER(p.bet_type) = 'WIN' -- Use WIN market for probable odds
            ORDER BY o.cloth_number ASC
            """,
            params={'event_id': event_id},
            cache_ttl=0,
        )
        runners_prob = rprob.to_dict("records") if not rprob.empty else []
    except Exception as e:
        print(f"Error fetching probable odds for event {event_id}: {e}")
        traceback.print_exc()
        runners_prob = []

    # Fetch features
    features_df = sql_df("SELECT * FROM vw_runner_features WHERE event_id=?", params=(event_id,), cache_ttl=0)
    features = features_df.to_dict("records") if not features_df.empty else []

    return render_template(
        "event_detail.html",
        event=event,
        conditions=conditions,
        runners=runners,
        products=products,
        features=features,
        runner_rows=runner_rows,
        runners_prob=runners_prob
    )

@app.post("/api/tote/refresh_event_pools/<event_id>")
def api_refresh_event_pools(event_id: str):
    """Triggers Cloud Run jobs to refresh pool info for all products of an event."""
    try:
        if not event_id:
            return app.response_class(json.dumps({"error": "missing event_id"}), mimetype="application/json", status=400)

        # Get project and topic from config/env
        project_id = os.getenv("GCP_PROJECT") or cfg.bq_project
        topic_id = os.getenv("PUBSUB_TOPIC_ID", "ingest-jobs")
        if not project_id or not topic_id:
            return app.response_class(json.dumps({"error": "GCP project or Pub/Sub topic not configured"}), mimetype="application/json", status=500)

        # List all products for the event to trigger a refresh for each.
        df = sql_df("SELECT product_id FROM tote_products WHERE event_id=?", params=(event_id,), cache_ttl=0)
        product_ids = [] if df.empty else [r["product_id"] for _, r in df.iterrows()]

        if not product_ids:
            return app.response_class(json.dumps({"triggered": 0, "product_ids": []}), mimetype="application/json")

        # Publish one job per product
        message_ids = []
        for pid in product_ids:
            try:
                # This uses the same task as the manual trigger page
                mid = publish_pubsub_message(project_id, topic_id, {"task": "ingest_single_product", "product_id": pid})
                message_ids.append(mid)
            except Exception as e:
                # Log and continue
                print(f"Failed to publish job for product {pid}: {e}")
        
        return app.response_class(json.dumps({"triggered": len(message_ids), "product_ids": product_ids}), mimetype="application/json")

    except Exception as e:
        traceback.print_exc()
        return app.response_class(json.dumps({"error": str(e)}), mimetype="application/json", status=500)

@app.post("/api/tote/refresh_odds/<event_id>")
def api_refresh_odds(event_id: str):
    """
    Refreshes probable odds by re-ingesting the WIN product for the event.
    This uses the main GraphQL ingestion path for consistency and robustness.
    """
    try:
        # Find an OPEN or SELLING WIN product for this event to re-ingest.
        pdf = sql_df(
            "SELECT product_id FROM `autobet-470818.autobet.tote_products` WHERE event_id=? AND UPPER(bet_type)='WIN' AND UPPER(status) IN ('OPEN', 'SELLING') ORDER BY status DESC LIMIT 1",
            params=(event_id,)
        )
        if pdf.empty:
            # Fallback to any WIN product if none are open
            pdf = sql_df(
                "SELECT product_id FROM `autobet-470818.autobet.tote_products` WHERE event_id=? AND UPPER(bet_type)='WIN' ORDER BY start_iso DESC LIMIT 1",
                params=(event_id,)
            )

        if pdf.empty:
            return app.response_class(json.dumps({"error": "no WIN product found for event"}), mimetype="application/json", status=404)

        win_product_id = pdf.iloc[0]["product_id"]

        from .bq import get_bq_sink
        from .providers.tote_api import ToteClient
        from .ingest.tote_products import ingest_products

        sink = get_bq_sink()
        client = ToteClient()

        # Re-ingest this specific product. The ingest_products function will fetch
        # all data, including the latest probable odds, and store it.
        updated_count = ingest_products(
            db=sink,
            client=client,
            date_iso=None,
            status=None,
            first=1,
            bet_types=None,
            product_ids=[win_product_id]
        )

        if updated_count > 0:
            return app.response_class(json.dumps({"ok": True, "product_id": win_product_id, "lines": "refreshed"}), mimetype="application/json")
        else:
            return app.response_class(json.dumps({"error": "ingest_products returned 0 updates", "product_id": win_product_id}), mimetype="application/json", status=500)

    except Exception as e:
        traceback.print_exc()
        return app.response_class(json.dumps({"error": str(e)}), mimetype="application/json", status=500)

@app.route("/api/tote/product_selections/<product_id>")
def api_tote_product_selections(product_id: str):
    """Return selections for a given product (id, number, competitor)."""
    if not product_id:
        return app.response_class(json.dumps({"error": "missing product_id"}), mimetype="application/json", status=400)
    try:
        df = sql_df(
            "SELECT selection_id, number, competitor FROM `autobet-470818.autobet.tote_product_selections` WHERE product_id=? ORDER BY CAST(number AS INT64) NULLS LAST",
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
    horse_df = sql_df("SELECT * FROM `autobet-470818.autobet.hr_horses` WHERE horse_id=?", params=(horse_id,))
    if horse_df.empty:
        flash("Horse not found", "error")
        return redirect(url_for("index"))
    horse = horse_df.to_dict("records")[0]

    # Fetch last 10 runs with conditions
    runs_df = sql_df(
        """
        SELECT r.*, e.name as event_name, e.venue, c.going, c.weather_desc
        FROM `autobet-470818.autobet.hr_horse_runs` r
        LEFT JOIN `autobet-470818.autobet.tote_events` e ON e.event_id = r.event_id
        LEFT JOIN `autobet-470818.autobet.race_conditions` c ON c.event_id = r.event_id
        WHERE r.horse_id = ?
        ORDER BY e.start_iso DESC
        LIMIT 10
        """, params=(horse_id,)
    )
    runs = [] if runs_df.empty else runs_df.to_dict("records")
    return render_template("horse_detail.html", horse=horse, runs=runs)

@app.route("/tote-superfecta/<product_id>")
def tote_superfecta_detail(product_id: str):
    pdf = sql_df("SELECT * FROM `autobet-470818.autobet.vw_products_latest_totals` WHERE product_id=?", params=(product_id,))
    if pdf.empty:
        flash("Unknown product id", "error")
        return redirect(url_for("tote_superfecta_page"))
    p = pdf.iloc[0].to_dict()
    runners_df = sql_df("SELECT DISTINCT number, competitor FROM `autobet-470818.autobet.tote_product_selections` WHERE product_id=? ORDER BY number", params=(product_id,))
    runners = runners_df.to_dict("records") if not runners_df.empty else []
    # Fallback: if no runners recorded yet, infer cloth numbers from probable odds view
    if not runners:
        try:
            odf = sql_df("SELECT DISTINCT CAST(cloth_number AS INT64) AS number FROM `autobet-470818.autobet.vw_tote_probable_odds` WHERE product_id=? ORDER BY number", params=(product_id,))
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
        FROM `autobet-470818.autobet.tote_product_dividends`
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
        FROM `autobet-4f70818.autobet.hr_horse_runs` WHERE event_id=? AND finish_pos IS NOT NULL
        ORDER BY finish_pos ASC
        """,
        params=(p.get('event_id'),)
    )
    # Load conditions (going + weather)
    cond = sql_df(
        "SELECT going, weather_temp_c, weather_wind_kph, weather_precip_mm FROM `autobet-470818.autobet.race_conditions` WHERE event_id=?",
        params=(p.get('event_id'),)
    )
    # Runners with latest probable odds (if available)
    runners2: list = []
    try: # noqa
        r2 = sql_df(
            """
            SELECT
                o.selection_id,
                o.cloth_number,
                COALESCE(s.competitor, o.selection_id) AS horse,
                o.decimal_odds,
                TIMESTAMP_MILLIS(o.ts_ms) AS odds_iso
            FROM vw_tote_probable_odds o
            JOIN tote_products p ON o.product_id = p.product_id
            LEFT JOIN tote_product_selections s ON o.selection_id = s.selection_id AND o.product_id = p.product_id
            WHERE p.event_id = @event_id AND UPPER(p.bet_type) = 'WIN'
            ORDER BY o.cloth_number ASC
        """,
        params={'event_id': p.get('event_id')}
        )
        runners2 = r2.to_dict("records") if not r2.empty else []
    except Exception as e:
        print(f"Error fetching probable odds for superfecta detail {product_id}: {e}")
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

    if request.method == "POST":
        # This flag is used to prevent auto-adjustment on subsequent POSTs if the user
        # wants to stick with their manually entered (but potentially -EV) parameters.
        manual_override_active = request.form.get("manual_override_active") == "1"

        try:
            calc_params["num_runners"] = int(request.form.get("num_runners", calc_params["num_runners"]))
            calc_params["bet_type"] = request.form.get("bet_type", calc_params["bet_type"]).upper()
            calc_params["bankroll"] = float(request.form.get("bankroll", calc_params["bankroll"]))
            # New weighting multipliers
            calc_params["key_horse_mult"] = float(request.form.get("key_horse_mult", calc_params["key_horse_mult"]))
            calc_params["poor_horse_mult"] = float(request.form.get("poor_horse_mult", calc_params["poor_horse_mult"]))
            # Accept 0..1 or 0..100 inputs and normalize to 0..1 internally
            conc_raw = float(request.form.get("concentration", calc_params["concentration"]))
            mi_raw = float(request.form.get("market_inefficiency", calc_params["market_inefficiency"]))
            calc_params["concentration"] = conc_raw / 100.0 if conc_raw > 1.0 else conc_raw
            calc_params["market_inefficiency"] = mi_raw / 100.0 if mi_raw > 1.0 else mi_raw
            calc_params["desired_profit_pct"] = float(request.form.get("desired_profit_pct", calc_params["desired_profit_pct"]))
            # Takeout rate: accept percent (0..100) or fraction (0..1)
            tr_raw = float(request.form.get("take_rate", calc_params["take_rate"]))
            calc_params["take_rate"] = tr_raw / 100.0 if tr_raw > 1.0 else tr_raw
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
            if not (0 <= calc_params["take_rate"] <= 1):
                errors.append("Takeout rate must be between 0% and 100%.")

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
            # Call the refactored calculation function
            results = calculate_pl_strategy(
                runners=calc_params["runners"],
                bet_type=calc_params["bet_type"],
                bankroll=calc_params["bankroll"],
                key_horse_mult=calc_params["key_horse_mult"],
                poor_horse_mult=calc_params["poor_horse_mult"],
                concentration=calc_params["concentration"],
                market_inefficiency=calc_params["market_inefficiency"],
                desired_profit_pct=calc_params["desired_profit_pct"],
                take_rate=calc_params["take_rate"],
                net_rollover=calc_params["net_rollover"],
                inc_self=calc_params["inc_self"],
                div_mult=calc_params["div_mult"],
                f_fix=calc_params["f_fix"],
                pool_gross_other=calc_params["pool_gross_other"],
            )
            errors.extend(results.get("errors", []))

            # Handle auto-adjustment feedback
            if results.get("auto_adjusted_params") and not manual_override_active:
                adj = results["auto_adjusted_params"]
                flash(f"Original settings were -EV. Parameters auto-adjusted to find a profitable strategy: Bet Concentration set to {adj['concentration']*100:.0f}% and Market Inefficiency to {adj['market_inefficiency']*100:.1f}%. You can modify these and recalculate.", "info")
                calc_params['concentration'] = adj['concentration']
                calc_params['market_inefficiency'] = adj['market_inefficiency']
                manual_override_active = True

    return render_template(
        "manual_calculator.html",
        calc_params=calc_params,
        results=results,
        errors=errors,
        bet_types=["WIN", "EXACTA", "TRIFECTA", "SUPERFECTA"],
        manual_override_active=manual_override_active,
    )

@app.route("/tote/calculator/<product_id>", methods=["GET", "POST"])
def tote_product_calculator_page(product_id: str):
    """Pre-populated manual calculator for a specific product.

    - GET: Prefills runners from WIN probable odds for the product's event and product context
      (take rate, rollover, pool size). Runs an initial calculation.
    - POST: Re-runs the calculation with adjusted parameters from the form.
    """
    # Load product context (prefer view, fallback to base table)
    psql = (
        "SELECT p.product_id, p.event_id, p.event_name, COALESCE(e.venue, p.venue) AS venue, "
        "COALESCE(e.country, p.currency) AS country, p.start_iso, COALESCE(p.status,'') AS status, p.currency, "
        "p.total_gross, p.total_net, p.rollover, p.deduction_rate, UPPER(p.bet_type) AS bet_type "
        "FROM vw_products_latest_totals p LEFT JOIN tote_events e USING(event_id) WHERE p.product_id=?"
    )
    try:
        pdf = sql_df(psql, params=(product_id,), cache_ttl=0)
    except Exception:
        psql2 = psql.replace("vw_products_latest_totals p", "tote_products p")
        pdf = sql_df(psql2, params=(product_id,), cache_ttl=0)
    if pdf.empty:
        flash("Unknown product id", "error")
        return redirect(url_for("tote_live_model_page"))
    prod = pdf.iloc[0].to_dict()

    # Defaults (tote_params) for t, f, stake_per_line, R
    try:
        params_df = sql_df("SELECT * FROM tote_params ORDER BY updated_ts DESC, ts_ms DESC LIMIT 1", cache_ttl=300)
        model_params = params_df.iloc[0].to_dict() if not params_df.empty else {}
    except Exception:
        model_params = {}

    # Build initial calc params or parse from POST
    calc_params = {
        "num_runners": 8,
        "bet_type": "SUPERFECTA",
        "runners": [],
        "bankroll": float(model_params.get("stake_per_line", 0.1) * 100.0),
        "key_horse_mult": 2.0,
        "poor_horse_mult": 0.5,
        "concentration": 0.0,
        "market_inefficiency": 0.10,
        "desired_profit_pct": 5.0,
        "take_rate": float(prod.get("deduction_rate") or model_params.get("t", 0.30) or 0.30),
        "net_rollover": float(prod.get("rollover") or model_params.get("R", 0.0) or 0.0),
        "inc_self": True,
        "div_mult": 1.0,
        # Default: do not force f-share; user can override via form
        "f_fix": None,
        "pool_gross_other": float(prod.get("total_gross") or 0.0),
    }

    errors: list[str] = []
    results: dict = {}

    if request.method == "POST":
        manual_override_active = request.form.get("manual_override_active") == "1"
        try:
            calc_params["num_runners"] = int(request.form.get("num_runners", calc_params["num_runners"]))
            calc_params["bet_type"] = request.form.get("bet_type", calc_params["bet_type"]).upper()
            calc_params["bankroll"] = float(request.form.get("bankroll", calc_params["bankroll"]))
            calc_params["key_horse_mult"] = float(request.form.get("key_horse_mult", calc_params["key_horse_mult"]))
            calc_params["poor_horse_mult"] = float(request.form.get("poor_horse_mult", calc_params["poor_horse_mult"]))
            conc_raw = float(request.form.get("concentration", calc_params["concentration"]))
            mi_raw = float(request.form.get("market_inefficiency", calc_params["market_inefficiency"]))
            calc_params["concentration"] = conc_raw / 100.0 if conc_raw > 1.0 else conc_raw
            calc_params["market_inefficiency"] = mi_raw / 100.0 if mi_raw > 1.0 else mi_raw
            calc_params["desired_profit_pct"] = float(request.form.get("desired_profit_pct", calc_params["desired_profit_pct"]))
            tr_raw = float(request.form.get("take_rate", calc_params["take_rate"]))
            calc_params["take_rate"] = tr_raw / 100.0 if tr_raw > 1.0 else tr_raw
            calc_params["net_rollover"] = float(request.form.get("net_rollover", calc_params["net_rollover"]))
            calc_params["inc_self"] = request.form.get("inc_self") == "1"
            calc_params["div_mult"] = float(request.form.get("div_mult", calc_params["div_mult"]))
            calc_params["f_fix"] = float(request.form.get("f_fix")) if request.form.get("f_fix") else None
            calc_params["pool_gross_other"] = float(request.form.get("pool_gross_other", calc_params["pool_gross_other"]))

            # Parse runners grid from form
            runners_from_form = []
            for i in range(calc_params["num_runners"]):
                odds = float(request.form.get(f"runner_odds_{i}", "10.0"))
                if odds <= 1.0:
                    errors.append(f"Runner {i+1} odds must be greater than 1.0.")
                runners_from_form.append({
                    "name": request.form.get(f"runner_name_{i}", f"Runner {i+1}"),
                    "odds": odds,
                    "is_key": request.form.get(f"runner_key_{i}") == "on",
                    "is_poor": request.form.get(f"runner_poor_{i}") == "on",
                })
            calc_params["runners"] = runners_from_form
        except Exception as e:
            errors.append(f"Invalid input: {e}")

        # Minimal validation
        k_map = {"WIN": 1, "EXACTA": 2, "TRIFECTA": 3, "SUPERFECTA": 4}
        k_perm = k_map.get(calc_params["bet_type"], 4)
        if calc_params["num_runners"] < k_perm:
            errors.append(f"Not enough runners ({calc_params['num_runners']}) for {calc_params['bet_type']}")

        if not errors:
            results = calculate_pl_strategy(
                runners=calc_params["runners"],
                bet_type=calc_params["bet_type"],
                bankroll=calc_params["bankroll"],
                key_horse_mult=calc_params["key_horse_mult"],
                poor_horse_mult=calc_params["poor_horse_mult"],
                concentration=calc_params["concentration"],
                market_inefficiency=calc_params["market_inefficiency"],
                desired_profit_pct=calc_params["desired_profit_pct"],
                take_rate=calc_params["take_rate"],
                net_rollover=calc_params["net_rollover"],
                inc_self=calc_params["inc_self"],
                div_mult=calc_params["div_mult"],
                f_fix=calc_params["f_fix"],
                pool_gross_other=calc_params["pool_gross_other"],
            )
    else:
        # GET: Prefill runners from WIN probable odds (latest) for the event
        try:
            # Find an open WIN product for the same event
            wdf = sql_df(
                """
                SELECT product_id
                FROM (
                  SELECT w.product_id, ROW_NUMBER() OVER() rn
                  FROM vw_products_latest_totals w
                  WHERE w.event_id=@eid AND UPPER(w.bet_type)='WIN' AND UPPER(COALESCE(w.status,'')) IN ('OPEN','SELLING')
                ) WHERE rn=1
                """,
                params={"eid": prod.get("event_id")},
            )
        except Exception:
            wdf = sql_df(
                "SELECT product_id FROM tote_products WHERE event_id=? AND UPPER(bet_type)='WIN' ORDER BY status DESC LIMIT 1",
                params=(prod.get("event_id"),),
            )
        win_pid = wdf.iloc[0]["product_id"] if not wdf.empty else None
        runners = []
        if win_pid:
            try:
                odf = sql_df(
                    """
                    SELECT
                      COALESCE(CAST(s.number AS INT64), CAST(o.cloth_number AS INT64)) AS cloth,
                      COALESCE(s.competitor, CONCAT('#', CAST(COALESCE(s.number, o.cloth_number) AS STRING))) AS name,
                      CAST(o.decimal_odds AS FLOAT64) AS odds
                    FROM vw_tote_probable_odds o
                    LEFT JOIN tote_product_selections s
                      ON s.selection_id = o.selection_id AND s.product_id = o.product_id
                    WHERE o.product_id = ?
                    ORDER BY COALESCE(CAST(s.number AS INT64), CAST(o.cloth_number AS INT64))
                    """,
                    params=(win_pid,),
                )
                # If no odds found, try an inline refresh from the partner endpoint then re-query
                if odf.empty or odf['odds'].isnull().all():
                    try:
                        base = cfg.tote_graphql_url or ""
                        host_root = ""
                        try:
                            if "/partner/" in base:
                                host_root = base.split("/partner/")[0].rstrip("/")
                            else:
                                from urllib.parse import urlparse
                                u = urlparse(base)
                                if u.scheme and u.netloc:
                                    host_root = f"{u.scheme}://{u.netloc}"
                        except Exception:
                            host_root = ""
                        if not host_root:
                            host_root = "https://hub.production.racing.tote.co.uk"
                        candidates = [
                            f"{host_root}/partner/gateway/probable-odds/v1/products/{win_pid}",
                            f"{host_root}/partner/gateway/v1/products/{win_pid}/probable-odds",
                            f"{host_root}/v1/products/{win_pid}/probable-odds",
                        ]
                        headers = {"Authorization": f"Api-Key {cfg.tote_api_key}", "Accept": "application/json"}
                        data = None
                        for url in candidates:
                            try:
                                r = requests.get(url, headers=headers, timeout=10)
                                if r.status_code == 200:
                                    data = r.json(); break
                            except Exception:
                                continue
                        if data:
                            def _extract_lines(obj):
                                lines = []
                                if not isinstance(obj, dict):
                                    return lines
                                try:
                                    if isinstance(obj.get("lines"), dict) and isinstance(obj["lines"].get("nodes"), list):
                                        for ln in obj["lines"]["nodes"]:
                                            lines.append(ln)
                                    elif isinstance(obj.get("lines"), list):
                                        lines.extend(obj["lines"])
                                except Exception:
                                    pass
                                for k, v in list(obj.items()):
                                    if isinstance(v, dict):
                                        lines.extend(_extract_lines(v))
                                    elif isinstance(v, list):
                                        for it in v:
                                            if isinstance(it, dict):
                                                lines.extend(_extract_lines(it))
                                return lines
                            lines = _extract_lines(data)
                            norm_lines = []
                            for ln in lines:
                                try:
                                    odds = ((ln.get("odds") or {}).get("decimal"))
                                    legs = ln.get("legs") or []
                                    sel_id = None
                                    if legs:
                                        try:
                                            sels = (legs[0].get("lineSelections") or [])
                                            if sels:
                                                sel_id = sels[0].get("selectionId")
                                        except Exception:
                                            pass
                                    if sel_id and odds is not None:
                                        norm_lines.append({
                                            "legs": [{"lineSelections": [{"selectionId": sel_id}]}],
                                            "odds": {"decimal": float(odds)},
                                        })
                                except Exception:
                                    continue
                            if norm_lines:
                                from .bq import get_bq_sink
                                sink = get_bq_sink()
                                import time as _t
                                ts_ms = int(_t.time() * 1000)
                                payload = {"products": {"nodes": [{"id": win_pid, "lines": {"nodes": norm_lines}}]}}
                                sink.upsert_raw_tote_probable_odds([
                                    {"raw_id": f"probable:{win_pid}:{ts_ms}", "fetched_ts": ts_ms, "payload": json.dumps(payload), "product_id": win_pid}
                                ])
                                # Re-query odds
                                odf = sql_df(
                                    """
                                    SELECT
                                      COALESCE(CAST(s.number AS INT64), CAST(o.cloth_number AS INT64)) AS cloth,
                                      COALESCE(s.competitor, CONCAT('#', CAST(COALESCE(s.number, o.cloth_number) AS STRING))) AS name,
                                      CAST(o.decimal_odds AS FLOAT64) AS odds
                                    FROM vw_tote_probable_odds o
                                    LEFT JOIN tote_product_selections s
                                      ON s.selection_id = o.selection_id AND s.product_id = o.product_id
                                    WHERE o.product_id = ?
                                    ORDER BY COALESCE(CAST(s.number AS INT64), CAST(o.cloth_number AS INT64))
                                    """,
                                    params=(win_pid,),
                                )
                    except Exception:
                        pass
                for _, r in odf.iterrows():
                    try:
                        name = str(r.get("name") or f"#{r.get('cloth')}")
                        odds = float(r.get("odds") or 0.0)
                        if odds and odds > 1.0:
                            runners.append({"name": name, "odds": odds, "is_key": False, "is_poor": False})
                    except Exception:
                        continue
            except Exception:
                pass
        if not runners:
            # Fallback: load runner list without odds
            try:
                sdf = sql_df(
                    "SELECT number AS cloth, COALESCE(competitor, CAST(number AS STRING)) AS name FROM tote_product_selections WHERE product_id=? ORDER BY CAST(number AS INT64)",
                    params=(product_id,),
                )
                for _, r in sdf.iterrows():
                    runners.append({"name": str(r.get("name") or f"#{r.get('cloth')}") , "odds": 10.0, "is_key": False, "is_poor": False})
            except Exception:
                pass
        if runners:
            calc_params["runners"] = runners
            calc_params["num_runners"] = len(runners)
        # Run an initial calculation with defaults
        if runners:
            results = calculate_pl_strategy(
                runners=calc_params["runners"],
                bet_type=calc_params["bet_type"],
                bankroll=calc_params["bankroll"],
                key_horse_mult=calc_params["key_horse_mult"],
                poor_horse_mult=calc_params["poor_horse_mult"],
                concentration=calc_params["concentration"],
                market_inefficiency=calc_params["market_inefficiency"],
                desired_profit_pct=calc_params["desired_profit_pct"],
                take_rate=calc_params["take_rate"],
                net_rollover=calc_params["net_rollover"],
                inc_self=calc_params["inc_self"],
                div_mult=calc_params["div_mult"],
                f_fix=calc_params["f_fix"],
                pool_gross_other=calc_params["pool_gross_other"],
            )

    # Build placeable lines list from results (ignore £0.00 lines)
    place_lines: list[str] = []
    place_items: list[dict] = []
    if results and isinstance(results, dict):
        pl = results.get("pl_model") or {}
        plan = pl.get("staking_plan") or []
        for s in plan:
            try:
                amt = float(s.get("stake") or 0.0)
                if round(amt + 1e-9, 2) > 0.0:
                    ln = s.get("line")
                    if ln:
                        place_lines.append(ln)
                        place_items.append({"line": ln, "stake": round(amt, 2)})
            except Exception:
                pass

    return render_template(
        "manual_calculator.html",
        calc_params=calc_params,
        results=results,
        errors=errors,
        bet_types=["WIN", "EXACTA", "TRIFECTA", "SUPERFECTA"],
        manual_override_active=True,
        product=prod,
        place_lines_text="\n".join(place_lines),
        place_lines_count=len(place_lines),
        place_lines=place_items,
    )

@app.route("/tote/live-model", methods=["GET", "POST"])
def tote_live_model_page():
    """
    Shows upcoming events, runs the PL model on them, and allows placing bets.
    """
    if request.method == "POST":
        # Handle bet placement
        product_id = request.form.get("product_id")
        stake = float(request.form.get("stake", "0"))
        selections_text = request.form.get("selections_text", "")
        mode = request.form.get("mode", "audit")

        if not all([product_id, stake > 0, selections_text]):
            flash("Missing product, stake, or selections for bet placement.", "error")
            return redirect(url_for("tote_live_model_page"))

        selections = [s.strip() for s in selections_text.splitlines() if s.strip()]
        # Optional: per-line stake amounts (JSON [{line, stake}])
        line_amounts = None
        try:
            lines_json = request.form.get("lines_json")
            if lines_json:
                arr = json.loads(lines_json)
                if isinstance(arr, list):
                    m = {}
                    for it in arr:
                        try:
                            ln = (it.get("line") if isinstance(it, dict) else None)
                            amt = float(it.get("stake")) if isinstance(it, dict) and it.get("stake") is not None else None
                            if ln and amt is not None and amt > 0:
                                m[str(ln).strip()] = round(amt, 2)
                        except Exception:
                            continue
                    if m:
                        line_amounts = m
        except Exception:
            line_amounts = None
        db = get_db()
        from .providers.tote_api import ToteClient
        client = ToteClient()
        if mode == 'live':
            if cfg.tote_graphql_url:
                client.base_url = cfg.tote_graphql_url.strip()
        elif mode == 'audit':
            if cfg.tote_audit_graphql_url:
                client.base_url = cfg.tote_audit_graphql_url.strip()

        res = place_audit_superfecta(
            db,
            mode=mode,
            product_id=product_id,
            selections=selections,
            stake=stake,
            post=True,
            client=client,
            line_amounts=line_amounts,
        )
        
        err_msg = res.get("error")
        if err_msg:
            flash(f"Bet placement error: {err_msg}", "error")
        else:
            st = res.get("placement_status")
            msg = f"Bet placement for {product_id} status: {st}"
            flash(msg, "success" if str(st).upper() in ("PLACED", "ACCEPTED") else "warning")
        return redirect(url_for("tote_live_model_page"))

    # GET: Display model results for upcoming events

    # --- Filters ---
    country = (request.args.get("country") or os.getenv("DEFAULT_COUNTRY", "GB")).strip().upper()
    date_filter = (request.args.get("date") or _today_iso()).strip()
    venue_filter = (request.args.get("venue") or "").strip()
    calculate_product_id = request.args.get("calculate_product_id")

    # Fetch default model parameters from BigQuery
    try:
        params_df = sql_df("SELECT * FROM `autobet-470818.autobet.tote_params` ORDER BY ts_ms DESC LIMIT 1", cache_ttl=300)
        model_params = params_df.iloc[0].to_dict() if not params_df.empty else {}
    except Exception:
        model_params = {}
    
    # --- Filter Options ---
    try:
        countries_df = sql_df("SELECT DISTINCT country FROM `autobet-470818.autobet.tote_events` WHERE country IS NOT NULL AND country<>'' ORDER BY country")
        country_options = countries_df['country'].tolist() if not countries_df.empty else []
    except Exception:
        country_options = []
    # Venue options based on filters (country/date) for convenience
    try:
        v_sql = (
            "SELECT DISTINCT COALESCE(e.venue, p.venue) AS venue " +
            "FROM `autobet-470818.autobet.vw_products_latest_totals` p LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id) "
            "WHERE UPPER(p.bet_type)='SUPERFECTA'"
        )
        v_params: list[object] = []
        if country:
            if country == "GB":
                v_sql += " AND (UPPER(e.country)=? OR UPPER(p.currency)=?)"; v_params.extend([country, "GBP"])
            elif country in ("IE", "IRL", "IRELAND"):
                v_sql += " AND (UPPER(e.country)=? OR UPPER(p.currency)=?)"; v_params.extend([country, "EUR"])
            else:
                v_sql += " AND UPPER(e.country)=?"; v_params.append(country)
        if date_filter:
            v_sql += " AND SUBSTR(p.start_iso,1,10)=?"; v_params.append(date_filter)
        v_sql += " ORDER BY venue"
        venues_df = sql_df(v_sql, params=tuple(v_params))
        venue_options = venues_df['venue'].dropna().tolist() if not venues_df.empty else []
    except Exception:
        venue_options = []

    # --- Fetch upcoming events based on filters ---
    sql = """
        SELECT
          p.product_id,
          p.event_id,
          p.event_name,
          p.venue,
          e.country,
          p.start_iso,
          p.status,
          p.currency,
          p.total_gross,
          p.total_net,
          p.rollover,
          p.deduction_rate,
          w.product_id AS win_product_id
        FROM
          `autobet-470818.autobet.vw_products_latest_totals` p
        LEFT JOIN `autobet-470818.autobet.tote_events` e ON p.event_id = e.event_id
        LEFT JOIN (
          SELECT product_id, event_id
          FROM (
            SELECT
              w.product_id,
              w.event_id,
              ROW_NUMBER() OVER(PARTITION BY w.event_id ORDER BY w.product_id) AS rn
            FROM `autobet-470818.autobet.vw_products_latest_totals` w
            WHERE UPPER(w.bet_type) = 'WIN' AND UPPER(COALESCE(w.status,'')) IN ('OPEN','SELLING')
          )
          WHERE rn = 1
        ) w ON w.event_id = p.event_id
    """
    
    where = [
        "UPPER(p.bet_type) = 'SUPERFECTA'",
    ]
    # On the live model page, show all of today's events regardless of status.
    # For other dates, keep to OPEN/SELLING/UNKNOWN to avoid clutter.
    if date_filter != _today_iso():
        where.append("UPPER(COALESCE(p.status,'')) IN ('OPEN','SELLING','UNKNOWN')")
    params = {}

    if country:
        # Prefer event country; also accept common currency proxies (GB->GBP, IE->EUR)
        params["country"] = country
        if country == "GB":
            where.append("(UPPER(e.country) = @country OR UPPER(p.currency) = @currency_proxy)")
            params["currency_proxy"] = "GBP"
        elif country in ("IE", "IRL", "IRELAND"):
            where.append("(UPPER(e.country) = @country OR UPPER(p.currency) = @currency_proxy)")
            params["currency_proxy"] = "EUR"
        else:
            where.append("UPPER(e.country) = @country")
    
    if date_filter:
        where.append("SUBSTR(p.start_iso, 1, 10) = @date")
        params["date"] = date_filter
    if venue_filter:
        where.append("UPPER(COALESCE(e.venue, p.venue)) LIKE @venue")
        params["venue"] = f"%{venue_filter.upper()}%"

    if where:
        sql += " WHERE " + " AND ".join(where)
    
    sql += " ORDER BY p.start_iso ASC"

    try:
        upcoming_df = sql_df(sql, params=params, cache_ttl=60)
    except Exception as e:
        # Fallback to base table if the view is missing
        try:
            sql_fb = sql.replace("`autobet-470818.autobet.vw_products_latest_totals`", "`autobet-470818.autobet.tote_products`")
            upcoming_df = sql_df(sql_fb, params=params, cache_ttl=30)
        except Exception as e2:
            flash(f"Query for upcoming events failed: {e2}", "error")
            upcoming_df = pd.DataFrame()

    # Secondary fallback: for GB today, use the 'next 60 minutes' helper view if empty
    try:
        if (upcoming_df is None or upcoming_df.empty) and country == "GB" and date_filter == _today_iso():
            gb_df = sql_df(
                "SELECT product_id, event_id, event_name, venue, country, start_iso, status, currency, total_gross, total_net, rollover \n"
                "FROM `autobet-470818.autobet.vw_gb_open_superfecta_next60` ORDER BY start_iso",
                cache_ttl=30,
            )
            if gb_df is not None and not gb_df.empty:
                upcoming_df = gb_df
    except Exception:
        pass

    # --- Calculate display pool values ---
    if upcoming_df is not None and not upcoming_df.empty:
        # Ensure columns exist and are numeric, fill NaNs with 0
        for col in ['total_gross', 'total_net', 'rollover']:
            if col not in upcoming_df.columns:
                upcoming_df[col] = 0.0
        upcoming_df['total_gross'] = pd.to_numeric(upcoming_df['total_gross'], errors='coerce').fillna(0.0)
        upcoming_df['total_net'] = pd.to_numeric(upcoming_df['total_net'], errors='coerce').fillna(0.0)
        upcoming_df['rollover'] = pd.to_numeric(upcoming_df['rollover'], errors='coerce').fillna(0.0)

        # Calculate total pool and net pool as requested
        upcoming_df['total_pool'] = upcoming_df['total_gross'] + upcoming_df['rollover']
        upcoming_df['net_pool'] = upcoming_df['total_net'] + upcoming_df['rollover']

    calculation_result = None
    if calculate_product_id and not upcoming_df.empty:
        # Find the specific product to calculate
        product_to_calc_series = upcoming_df[upcoming_df['product_id'] == calculate_product_id]
        if not product_to_calc_series.empty:
            p = product_to_calc_series.iloc[0]
            
            if pd.isna(p.get("win_product_id")):
                 flash(f"Cannot calculate model for {p['event_name']}: No open WIN market found to fetch probable odds.", "warning")
            else:
                # Fetch runners and probable odds
                odds_df = sql_df(
                    """
                    SELECT
                      COALESCE(CAST(s.number AS INT64), CAST(o.cloth_number AS INT64)) AS number,
                      COALESCE(s.competitor, CONCAT('#', CAST(COALESCE(s.number, o.cloth_number) AS STRING))) AS name,
                      CAST(o.decimal_odds AS FLOAT64) AS odds
                    FROM vw_tote_probable_odds o
                    LEFT JOIN tote_product_selections s
                      ON s.selection_id = o.selection_id AND s.product_id = o.product_id
                    WHERE o.product_id = ?
                    ORDER BY COALESCE(CAST(s.number AS INT64), CAST(o.cloth_number AS INT64))
                    """,
                    params=(p["win_product_id"],)
                )
                if odds_df.empty or odds_df['odds'].isnull().all():
                    flash(f"Could not fetch probable odds for {p['event_name']}. Cannot run model.", "warning")
                else:
                    # Prefer BigQuery permutations for performance and accuracy
                    calc_result = None
                    try:
                        if _use_bq():
                            top_n = int(max(1, len(odds_df)))
                            perms_df = sql_df(
                                f"SELECT * FROM `{cfg.bq_project}.{cfg.bq_dataset}.tf_superfecta_perms_any`(@pid, @top_n)",
                                params={"pid": p["product_id"], "top_n": top_n},
                                cache_ttl=0,
                            )
                        else:
                            perms_df = None
                    except Exception:
                        perms_df = None

                    if perms_df is not None and not perms_df.empty:
                        # Build runner odds map from WIN market
                        odds_map = {}
                        try:
                            for _, r in odds_df.iterrows():
                                n = str(int(r["number"])) if not pd.isna(r["number"]) else None
                                if n:
                                    odds_map[n] = float(r["odds"]) if not pd.isna(r["odds"]) else None
                        except Exception:
                            odds_map = {}

                        # Prepare perms for EV helper
                        rows = []
                        for _, r in perms_df.sort_values("p", ascending=False).iterrows():
                            ids = [str(r.get("h1")), str(r.get("h2")), str(r.get("h3")), str(r.get("h4"))]
                            rows.append({"ids": ids, "probability": float(r.get("p") or 0.0)})

                        # Compute EV grid in BigQuery for optimal coverage
                        bankroll = float(model_params.get("stake_per_line", 10.0) * 100)
                        take_rate_val = float(p.get("deduction_rate") or model_params.get("t", 0.3))
                        rollover_val = float(p.get("rollover") or model_params.get("R", 0.0))
                        gross_other = float(p.get("total_gross") or 0.0)
                        try:
                            grid_df = sql_df(
                                f"SELECT * FROM `{cfg.bq_project}.{cfg.bq_dataset}.tf_perm_ev_grid`(@pid,@top_n,@O,@S,@t,@R,@inc,@mult,@f,@conc,@mi)",
                                params={
                                    "pid": p["product_id"],
                                    "top_n": top_n,
                                    "O": gross_other,
                                    "S": bankroll,
                                    "t": take_rate_val,
                                    "R": rollover_val,
                                    "inc": True,
                                    "mult": 1.0,
                                    "f": None,
                                    "conc": 0.0,
                                    "mi": 0.10,
                                },
                                cache_ttl=0,
                            )
                        except Exception as e:
                            grid_df = None

                        if grid_df is not None and not grid_df.empty:
                            # Pick the m with max expected_profit
                            best_row = grid_df.sort_values("expected_profit", ascending=False).iloc[0]
                            m = int(best_row["lines_covered"]) if not pd.isna(best_row["lines_covered"]) else 1
                            # Build staking plan using probability-weighted stakes for top m lines
                            gamma = 1.0 + 2.0 * 0.0
                            perms_sorted = perms_df.sort_values("p", ascending=False).head(m)
                            probs = perms_sorted["p"].astype(float).clip(lower=0.0)
                            weights = probs.pow(gamma)
                            sum_w = float(weights.sum()) or 1.0
                            stakes = (bankroll * (weights / sum_w)).tolist()
                            staking_plan = []
                            for i, (_, r) in enumerate(perms_sorted.iterrows()):
                                line_ids = [str(r.get("h1")), str(r.get("h2")), str(r.get("h3")), str(r.get("h4"))]
                                staking_plan.append({
                                    "line": " - ".join(line_ids),
                                    "probability": float(r.get("p") or 0.0),
                                    "stake": float(stakes[i]),
                                })

                            scenario = {
                                "lines_covered": m,
                                "hit_rate": float(best_row.get("hit_rate") or 0.0),
                                "expected_return": float(best_row.get("expected_return") or 0.0),
                                "expected_profit": float(best_row.get("expected_profit") or 0.0),
                                "f_share_used": float(best_row.get("f_share_used") or 0.0),
                                "net_pool_if_bet": float((1.0) * (((1.0 - take_rate_val) * (gross_other + bankroll)) + rollover_val)),
                                "total_stake": bankroll,
                            }
                            pl_model = {"best_scenario": scenario, "staking_plan": staking_plan, "ev_grid": grid_df.to_dict("records"), "total_possible_lines": len(perms_df)}
                        else:
                            # Fallback to local EV helper if grid not available
                            ev_res = calculate_pl_from_perms(
                                perms=rows,
                                bankroll=bankroll,
                                runner_odds=odds_map,
                                concentration=0.0,
                                market_inefficiency=0.1,
                                desired_profit_pct=5.0,
                                take_rate=take_rate_val,
                                net_rollover=rollover_val,
                                inc_self=True,
                                div_mult=1.0,
                                f_fix=None,
                                pool_gross_other=gross_other,
                            )
                            pl_model = ev_res.get("pl_model")
                    else:
                        # Fallback: run local Python model
                        calc_result = calculate_pl_strategy(
                            runners=odds_df.to_dict("records"),
                            bet_type="SUPERFECTA",
                            bankroll=float(model_params.get("stake_per_line", 10.0) * 100), # Example bankroll
                            key_horse_mult=2.0, # Default
                            poor_horse_mult=0.5, # Default
                            concentration=0.0, # Default
                            market_inefficiency=0.1, # Default
                            desired_profit_pct=5.0, # Default
                            take_rate=float(p.get("deduction_rate") or model_params.get("t", 0.3)),
                            net_rollover=float(p.get("rollover") or model_params.get("R", 0.0)),
                            inc_self=True,
                            div_mult=1.0,
                            # Let the model compute f-share automatically unless user overrides
                            f_fix=None,
                            pool_gross_other=float(p.get("total_gross") or 0.0),
                        )
                        pl_model = calc_result.get("pl_model")
                    if pl_model and pl_model.get("best_scenario"):
                        scenario = pl_model["best_scenario"]
                        staking_plan = pl_model.get("staking_plan") or []
                        # Filter out zero-stake lines (rounded to 2dp) for placement
                        filtered_lines = []
                        weighted_lines = []
                        for s in staking_plan:
                            try:
                                amt = float(s.get("stake") or 0.0)
                                if round(amt + 1e-9, 2) > 0.0:
                                    ln = s.get("line")
                                    filtered_lines.append(ln)
                                    weighted_lines.append({"line": ln, "stake": round(amt, 2)})
                            except Exception:
                                pass
                        calculation_result = {
                            "product": p.to_dict(),
                            "scenario": scenario,
                            "staking_plan_text": "\n".join(filtered_lines),
                            "total_stake": scenario.get("total_stake", 0.0),
                            "weighted_lines": weighted_lines,
                        }
                    else:
                        flash(f"Model did not find a profitable strategy for {p['event_name']}.", "info")
        else:
            flash(f"Product ID {calculate_product_id} not found in upcoming events.", "warning")

    return render_template(
        "tote_live_model.html",
        events=upcoming_df.to_dict("records") if not upcoming_df.empty else [],
        calculation_result=calculation_result,
        filters={
            "country": country,
            "date": date_filter,
            "venue": venue_filter,
        },
        country_options=country_options,
        venue_options=venue_options,
    )


@app.route("/api/tote/bet_status")
def api_tote_bet_status():
    # Not available in the current BigQuery-only mode.
    return app.response_class(
        json.dumps({"error": "Bet status API not available"}),
        mimetype="application/json",
        status=400,
    )

@app.route("/api/tote/placement_id")
def api_tote_placement_id():
    # Not available in the current BigQuery-only mode.
    return app.response_class(
        json.dumps({"error": "Placement ID API not available"}),
        mimetype="application/json",
        status=400,
    )

@app.route("/models")
def models_page():
    """Responsive Models page backed by BigQuery backtest TFs."""
    if not _use_bq():
        flash("BigQuery is not configured. Set BQ_PROJECT and BQ_DATASET.", "error")
        return redirect(url_for('index'))

    start = (request.args.get("start") or "2024-01-01").strip()
    end = (request.args.get("end") or _today_iso()).strip()
    try:
        top_n = int(request.args.get("top_n") or 10)
    except Exception:
        top_n = 10
    try:
        coverage = float(request.args.get("coverage") or 0.60)
    except Exception:
        coverage = 0.60
    try:
        limit = max(1, int(request.args.get("limit") or 200))
    except Exception:
        limit = 200

    tf = f"`{cfg.bq_project}.{cfg.bq_dataset}.tf_sf_backtest_horse_any`"
    try:
        rows_df = sql_df(
            f"SELECT * FROM {tf}(@start,@end,@top_n,@cov) ORDER BY start_iso LIMIT @lim",
            params={"start": start, "end": end, "top_n": top_n, "cov": coverage, "lim": limit},
            cache_ttl=0,
        )
    except Exception as e:
        rows_df = pd.DataFrame()
        flash(f"Backtest error: {e}", "error")

    try:
        sum_df = sql_df(
            f"""
            WITH r AS (
              SELECT * FROM {tf}(@start,@end,@top_n,@cov)
            )
            SELECT
              COUNT(*) AS races,
              COUNTIF(winner_rank IS NOT NULL) AS with_winner_rank,
              COUNTIF(hit_at_coverage) AS hits_at_cov,
              SAFE_DIVIDE(COUNTIF(hit_at_coverage), NULLIF(COUNT(*),0)) AS hit_rate
            FROM r
            """,
            params={"start": start, "end": end, "top_n": top_n, "cov": coverage},
            cache_ttl=0,
        )
        summary = ({} if sum_df.empty else sum_df.iloc[0].to_dict())
    except Exception as e:
        summary = {}
        flash(f"Summary error: {e}", "error")

    # Annotate weight source (PRED vs ODDS) for displayed rows
    try:
        if rows_df is not None and not rows_df.empty:
            pids = list(dict.fromkeys(rows_df["product_id"].astype(str).tolist()))
            if pids:
                csv_ids = ",".join(pids)
                pred_df = sql_df(
                    """
                    WITH ids AS (SELECT id FROM UNNEST(SPLIT(@ids, ',')) AS id)
                    SELECT DISTINCT s.product_id " +
                    "FROM `autobet-470818.autobet.vw_superfecta_runner_strength` s
                    JOIN ids ON ids.id = s.product_id
                    """,
                    params={"ids": csv_ids},
                    cache_ttl=0,
                )
                odds_df = sql_df(
                    """
                    WITH ids AS (SELECT id FROM UNNEST(SPLIT(@ids, ',')) AS id)
                    SELECT DISTINCT s.product_id " +
                    "FROM `autobet-470818.autobet.vw_sf_strengths_from_win_horse` s
                    JOIN ids ON ids.id = s.product_id
                    """,
                    params={"ids": csv_ids},
                    cache_ttl=0,
                )
                pred_set = set([] if pred_df is None or pred_df.empty else pred_df["product_id"].astype(str).tolist())
                odds_set = set([] if odds_df is None or odds_df.empty else odds_df["product_id"].astype(str).tolist())
                rows_df["weights_source"] = rows_df["product_id"].astype(str).apply(
                    lambda x: ("PRED" if x in pred_set else ("ODDS" if x in odds_set else "UNKNOWN"))
                )
    except Exception:
        pass

    return render_template(
        "models.html",
        params={"start": start, "end": end, "top_n": top_n, "coverage": coverage, "limit": limit},
        summary=summary,
        rows=([] if rows_df is None or rows_df.empty else rows_df.to_dict("records")),
    )

@app.route("/api/models/ev_curve")
def api_models_ev_curve():
    if not _use_bq():
        return app.response_class(json.dumps({"error": "BigQuery not configured"}), mimetype="application/json", status=400)
    pid = (request.args.get("pid") or "").strip()
    try:
        top_n = int(request.args.get("top_n") or 10)
    except Exception:
        top_n = 10
    if not pid:
        return app.response_class(json.dumps({"error": "missing pid"}), mimetype="application/json", status=400)
    try:
        df = sql_df(
            f"SELECT line_index, lines_frac, cum_prob, stake_total, o_min_break_even FROM `{cfg.bq_project}.{cfg.bq_dataset}.tf_sf_breakeven_latest`(@pid, @top_n) ORDER BY line_index",
            params={"pid": pid, "top_n": top_n},
            cache_ttl=0,
        )
        rows = ([] if df is None or df.empty else df.to_dict("records"))
        return app.response_class(json.dumps({"rows": rows}), mimetype="application/json")
    except Exception as e:
        return app.response_class(json.dumps({"error": str(e)}), mimetype="application/json", status=500)

@app.route("/models/<model_id>/eval")
def model_eval_page(model_id: str):
    # This page relied on legacy local tables and is disabled.
    flash("Model evaluation is temporarily disabled.", "warning")
    return redirect(url_for('index'))

@app.route("/models/<model_id>/superfecta")
def model_superfecta_eval_page(model_id: str):
    # This page relied on legacy local tables and is disabled.
    flash("Model pages are temporarily disabled.", "warning")
    return redirect(url_for('index'))

@app.route("/models/<model_id>/eval/event/<event_id>")
def model_event_eval_page(model_id: str, event_id: str):
    # This page relied on legacy local tables and is disabled.
    flash("Model pages are temporarily disabled.", "warning")
    return redirect(url_for('index'))
@app.route("/imports")
def imports_page():
    """Show latest data imports and basic counts from BigQuery."""
    try:
        prod_today = sql_df("SELECT COUNT(1) AS c, MAX(start_iso) AS max_start FROM `autobet-470818.autobet.tote_products` WHERE DATE(SUBSTR(start_iso,1,10))=CURRENT_DATE()")
        ev_today = sql_df("SELECT COUNT(1) AS c, MAX(start_iso) AS max_start FROM `autobet-470818.autobet.tote_events` WHERE DATE(SUBSTR(start_iso,1,10))=CURRENT_DATE()")
        raw_latest = sql_df("SELECT endpoint, fetched_ts FROM `autobet-470818.autobet.raw_tote` ORDER BY fetched_ts DESC LIMIT 20")
        prob_latest = sql_df("SELECT fetched_ts FROM `autobet-470818.autobet.raw_tote_probable_odds` ORDER BY fetched_ts DESC LIMIT 20")
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
    """A unified page to place bets. Supports simple (e.g., WIN) and complex (e.g., SUPERFECTA) bets."""
    if request.method == "POST":
        product_id = request.form.get("product_id")
        stake_str = request.form.get("stake")
        currency = request.form.get("currency", "GBP")
        mode = request.form.get("mode", "audit")
        if mode != "live":
            mode = "audit"
        if mode == "live" and (os.getenv("TOTE_LIVE_ENABLED", "0").lower() not in ("1","true","yes","on")):
            flash("Live betting disabled (TOTE_LIVE_ENABLED not set). Using audit mode.", "warning")
            mode = "audit"

        # Add logging
        try:
            print(f"[BetPlacement] Attempting bet product_id={product_id}, stake={stake_str}, mode='{mode}'")
        except Exception:
            pass

        errors = []
        if not product_id:
            errors.append("Product must be selected.")
        if not stake_str:
            errors.append("Stake is required.")

        stake = 0.0
        try:
            stake = float(stake_str)
            if stake <= 0:
                errors.append("Stake must be a positive number.")
        except Exception:
            errors.append("Stake must be a valid number.")

        if errors:
            for e in errors:
                flash(e, "error")
            return redirect(request.referrer or url_for("tote_bet_page"))

        # Determine bet type
        prod_df = sql_df("SELECT bet_type FROM `autobet-470818.autobet.tote_products` WHERE product_id=?", params=(product_id,))
        if prod_df.empty:
            flash(f"Product with ID '{product_id}' not found.", "error")
            return redirect(request.referrer or url_for("tote_bet_page"))
        bet_type = (prod_df.iloc[0]['bet_type'] or "").upper()
        db = get_db()
        res = {}

        if bet_type in ["WIN", "PLACE"]:
            from .providers.tote_bets import place_audit_simple_bet
            from .providers.tote_api import ToteClient
            selection_id = request.form.get("selection_id")
            if not selection_id:
                flash("A runner/selection must be chosen for a WIN/PLACE bet.", "error")
                return redirect(request.referrer or url_for("tote_bet_page"))
            client_for_placement = ToteClient()
            if mode == 'live':
                if cfg.tote_graphql_url:
                    client_for_placement.base_url = cfg.tote_graphql_url.strip()
            elif mode == 'audit':
                if cfg.tote_audit_graphql_url:
                    client_for_placement.base_url = cfg.tote_audit_graphql_url.strip()
            res = place_audit_simple_bet(db, mode=mode, product_id=product_id, selection_id=selection_id, stake=stake, currency=currency, post=True, client=client_for_placement)
        elif bet_type in ["SUPERFECTA", "TRIFECTA", "EXACTA"]:
            from .providers.tote_bets import place_audit_superfecta
            from .providers.tote_api import ToteClient
            selections_text = (request.form.get("selections_text") or "").strip()
            if not selections_text:
                flash("Selections must be provided for a combination bet (e.g., '1-2-3-4').", "error")
                return redirect(request.referrer or url_for("tote_bet_page"))
            selections = [p for p in (selections_text.replace("\r","\n").replace(",","\n").split("\n")) if p.strip()]
            client_for_placement = ToteClient()
            if mode == 'live':
                if cfg.tote_graphql_url:
                    client_for_placement.base_url = cfg.tote_graphql_url.strip()
            elif mode == 'audit':
                if cfg.tote_audit_graphql_url:
                    client_for_placement.base_url = cfg.tote_audit_graphql_url.strip()
            res = place_audit_superfecta(db, mode=mode, product_id=product_id, selections=selections, stake=stake, currency=currency, post=True, stake_type="total", client=client_for_placement)
        else:
            flash(f"Bet type '{bet_type}' is not currently supported on this page.", "error")
            return redirect(request.referrer or url_for("tote_bet_page"))

        st = res.get("placement_status")
        err_msg = res.get("error")
        if err_msg:
            if "Failed to fund ticket" in str(err_msg):
                flash("Bet placement failed: Insufficient funds in the Tote account for a live bet.", "error")
            else:
                flash(f"Bet placement error: {err_msg}", "error")
        elif st:
            ok = str(st).upper() in ("PLACED", "ACCEPTED", "OK", "SUCCESS")
            msg = f"Bet placement status: {st}"
            fr = res.get("failure_reason")
            if fr:
                msg += f" (reason: {fr})"
            flash(msg, "success" if ok else "error")
        else:
            flash("Bet recorded (no placement status returned).", "success")

        return redirect(request.referrer or url_for("tote_bet_page"))

    # GET request: Handle filters and fetch upcoming events
    country = (request.args.get("country") or "").strip().upper()
    venue = (request.args.get("venue") or "").strip()

    where = []
    params: dict[str, Any] = {}
    if country:
        where.append("UPPER(country) = @country")
        params["country"] = country
    if venue:
        where.append("UPPER(venue) LIKE UPPER(@venue)")
        params["venue"] = f"%{venue}%"

    # Default to showing events in the next 24 hours.
    from datetime import timezone
    now = datetime.now(timezone.utc)
    start_iso = now.isoformat(timespec='seconds').replace('+00:00', 'Z')
    end_iso = (now + timedelta(hours=24)).isoformat(timespec='seconds').replace('+00:00', 'Z')
    where.append("start_iso BETWEEN @start AND @end")
    params["start"] = start_iso
    params["end"] = end_iso

    # Filter for events that are not yet finished.
    where.append("status IN ('SCHEDULED', 'OPEN')")

    ev_sql = (
        "SELECT event_id, name, venue, country, start_iso "
        "FROM `autobet-470818.autobet.tote_events`"
    )
    if where:
        ev_sql += " WHERE " + " AND ".join(where)

    ev_sql += " ORDER BY start_iso ASC LIMIT 200"

    events_df = sql_df(ev_sql, params=params)
    countries_df = sql_df("SELECT DISTINCT country FROM `autobet-470818.autobet.tote_events` WHERE country IS NOT NULL AND country<>'' ORDER BY country")
    # Optional preselect by product_id
    preselect = None
    try:
        pid = (request.args.get("product_id") or "").strip()
        if pid:
            df = sql_df("SELECT event_id FROM `autobet-470818.autobet.tote_products` WHERE product_id=?", params=(pid,))
            if not df.empty:
                preselect = {"product_id": pid, "event_id": df.iloc[0]['event_id']}
    except Exception:
        preselect = None

    return render_template(
        "tote_bet.html",
        events=events_df.to_dict("records") if not events_df.empty else [],
        filters={
            "country": country,
            "venue": venue,
        },
        country_options=(countries_df['country'].tolist() if not countries_df.empty else []),
        preselect=preselect,
    )
