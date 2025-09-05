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
from typing import Optional, Sequence
from google.cloud import bigquery
from .providers.tote_bets import place_audit_superfecta
from .providers.tote_bets import refresh_bet_status, audit_list_bets, sync_bets_from_api

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


def sql_df(sql: str, params: Optional[Sequence] = None) -> pd.DataFrame:
    """Runs a query against BigQuery and returns a pandas DataFrame."""
    db = get_db()
    # Convert positional '?' to named parameters @p0, @p1, ...
    q = sql
    qp = []
    if params:
        parts = q.split("?")
        q = ""
        for i, part in enumerate(parts[:-1]):
            q += part + f"@p{i}"
        q += parts[-1]
        for i, v in enumerate(params):
            if isinstance(v, (int, float)):
                typ = bigquery.ScalarQueryParameter(f"p{i}", "FLOAT64" if isinstance(v, float) else "INT64", v)
            else:
                typ = bigquery.ScalarQueryParameter(f"p{i}", "STRING", str(v))
            qp.append(typ)
    job_config = bigquery.QueryJobConfig(
        default_dataset=f"{cfg.bq_project}.{cfg.bq_dataset}",
        query_parameters=qp or [],
    )
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

    # Bankroll is now a placeholder as it was stored in SQLite
    bankroll = cfg.paper_starting_bankroll

    # Now/Next races (horse_racing) in next 60 minutes
    now = datetime.now(timezone.utc)
    start_iso = now.isoformat(timespec='seconds').replace('+00:00', 'Z')
    end_iso = (now + timedelta(minutes=60)).isoformat(timespec='seconds').replace('+00:00', 'Z')

    # The sql_df function now directly uses the BigQuery connection
    ev_sql = (
        "SELECT event_id, name, venue, country, start_iso, sport, status "
        "FROM tote_events WHERE sport='horse_racing' AND start_iso BETWEEN @start AND @end "
        "ORDER BY start_iso ASC LIMIT 20"
    )
    evs = sql_df(ev_sql, params={"start": start_iso, "end": end_iso})

    # Optional: superfecta products upcoming
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
    df = sql_df(base_sql, params=tuple(params_paged))
    total = int(sql_df(count_sql, params=tuple(params)).iloc[0]["c"]) if where else int(sql_df("SELECT COUNT(1) AS c FROM tote_events").iloc[0]["c"])
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
    country = (request.args.get("country") or os.getenv("DEFAULT_COUNTRY", "GB")).strip().upper()
    status = (request.args.get("status") or "OPEN").strip().upper()
    date_from = request.args.get("from") or _today_iso()
    date_to = request.args.get("to") or _today_iso()
    limit = int(request.args.get("limit", "500") or 500)
    upcoming_flag = (request.args.get("upcoming") or "").lower() in ("1","true","yes","on")
    where = ["UPPER(p.bet_type)='SUPERFECTA'"]
    params: list[object] = []
    if country:
        where.append("(UPPER(e.country)=? OR UPPER(p.currency)=?)"); params.extend([country, country])
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
        "COALESCE(p.status,'') AS status, p.currency, p.total_gross, p.total_net, "
        "(SELECT COUNT(1) FROM tote_product_selections s WHERE s.product_id = p.product_id) AS n_runners "
        "FROM tote_products p LEFT JOIN tote_events e USING(event_id) "
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY p.start_iso DESC LIMIT ?"; params.append(limit)
    df = sql_df(sql, params=tuple(params))
    # Options for filters
    cdf = sql_df("SELECT DISTINCT country FROM tote_events WHERE country IS NOT NULL AND country<>'' ORDER BY country")
    sdf = sql_df("SELECT DISTINCT COALESCE(status,'') AS status FROM tote_products WHERE bet_type='SUPERFECTA' ORDER BY 1")
    # Optional upcoming 60m (GB) from BigQuery
    upcoming = []
    try:
        udf = sql_df("SELECT product_id, event_id, event_name, venue, country, start_iso, status, currency, n_competitors, combos, S, O_min, roi_current, viable_now FROM vw_gb_open_superfecta_next60_be ORDER BY start_iso")
        if not udf.empty:
            upcoming = udf.to_dict("records")
    except Exception:
        upcoming = []
    products = df.to_dict("records") if not df.empty else []
    return render_template(
        "tote_superfecta.html",
        products=products,
        filters={
            "country": country,
            "status": status,
            "from": date_from,
            "to": date_to,
            "limit": limit,
            "upcoming": upcoming_flag,
        },
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
    bet_type = request.args.get("bet_type")
    country = request.args.get("country")
    status = request.args.get("status")
    venue = request.args.get("venue")
    date_from = request.args.get("from")
    date_to = request.args.get("to")
    limit = int(request.args.get("limit", "500") or 500)
    page = max(1, int(request.args.get("page", "1") or 1))
    offset = (page - 1) * limit

    where = []
    params = []
    if bet_type:
        where.append("bet_type = ?"); params.append(bet_type)
    if country:
        where.append("UPPER(currency) = UPPER(?)"); params.append(country)
    if status:
        where.append("status = ?"); params.append(status)
    if venue:
        where.append("UPPER(venue) LIKE UPPER(?)"); params.append(f"%{venue}%")
    if date_from:
        where.append("start_iso >= ?"); params.append(date_from)
    if date_to:
        where.append("start_iso <= ?"); params.append(date_to)

    sql = "SELECT * FROM tote_products"
    if where:
        sql += " WHERE " + " AND ".join(where)
    # Total count
    count_sql = sql.replace("SELECT *", "SELECT COUNT(1) AS c")
    total = sql_df(count_sql, params=tuple(params)).iloc[0]["c"] if where else sql_df("SELECT COUNT(1) AS c FROM tote_products").iloc[0]["c"]
    # Page slice
    sql += " ORDER BY start_iso LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    df = sql_df(sql, params=tuple(params))
    # Options for filters
    types_df = sql_df("SELECT DISTINCT bet_type FROM tote_products WHERE bet_type IS NOT NULL AND bet_type <> '' ORDER BY bet_type")
    curr_df = sql_df("SELECT DISTINCT currency FROM tote_products WHERE currency IS NOT NULL AND currency <> '' ORDER BY currency")
    # Group totals per bet type
    gt_sql = "SELECT bet_type, COUNT(1) AS n, SUM(total_net) AS sum_net FROM tote_products"
    if where:
        gt_sql += " WHERE " + " AND ".join(where)
    gt_sql += " GROUP BY bet_type ORDER BY bet_type"
    gt = sql_df(gt_sql, params=tuple(params[:-2])) if where else sql_df(gt_sql)
    # Dividends counts per product for quick visibility
    div_counts_df = sql_df("SELECT product_id, COUNT(1) AS c FROM tote_product_dividends GROUP BY product_id")
    # Conditions map (going + simple weather summary)
    cond_df = sql_df(
        "SELECT event_id, going, weather_temp_c, weather_wind_kph, weather_precip_mm FROM race_conditions",
    )
    div_counts = {r[0]: r[1] for r in div_counts_df.itertuples(index=False)} if not div_counts_df.empty else {}
    prods = df.to_dict("records") if not df.empty else []
    for p in prods:
        p["dividend_count"] = int(div_counts.get(p.get("product_id"), 0))
        # Attach conditions badge
        ev = p.get("event_id")
        if ev and not cond_df.empty:
            row = cond_df[cond_df["event_id"] == ev]
            if not row.empty:
                r = row.iloc[0]
                p["going"] = r.get("going")
                t = r.get("weather_temp_c"); w = r.get("weather_wind_kph"); pr = r.get("weather_precip_mm")
                if pd.notnull(t) or pd.notnull(w) or pd.notnull(pr):
                    p["weather_badge"] = f"{'' if pd.isnull(t) else int(round(t))}°C {'' if pd.isnull(w) else int(round(w))}kph {'' if pd.isnull(pr) else pr:.1f}mm"
                else:
                    p["weather_badge"] = None
    # Aggregate totals
    total_net = sum((p.get('total_net') or 0) for p in prods)
    group_totals = gt.to_dict("records") if not gt.empty else []
    return render_template("tote_pools.html", products=prods, totals={"total_net": total_net, "count": len(prods), "total": int(total), "page": page, "limit": limit}, group_totals=group_totals, filters={
        "bet_type": bet_type or "",
        "country": country or "",
        "status": status or "",
        "venue": venue or "",
        "date_from": date_from or "",
        "date_to": date_to or "",
        "limit": limit,
        "page": page,
    }, bet_type_options=(types_df['bet_type'].tolist() if not types_df.empty else []), country_options=(curr_df['currency'].tolist() if not curr_df.empty else []))

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
    """Single-page calculators using Tote probable odds as fixed odds.

    - Refreshes selection units from Tote API on demand
    - Computes per-runner probable odds (units share)
    - Builds top-N superfecta permutations using PL and shows coverage
    - Breakeven calculator based on takeout, stake/line, and your share
    """
    conn = _get_db_conn(); init_schema(conn)
    # Optional refresh of products (OPEN) to update units
    if request.method == "POST" and (request.form.get("refresh") == "1"):
        try:
            from .providers.tote_api import ToteClient
            from .ingest.tote_products import ingest_products
            ds = _today_iso()
            client = ToteClient()
            # Refresh the currently selected bet type (default SUPERFECTA)
            sel_bt = (request.values.get("bet_type") or "SUPERFECTA").strip().upper()
            ingest_products(conn, client, date_iso=ds, status="OPEN", first=400, bet_types=[sel_bt])  # captures totalUnits
            flash("Refreshed products and selection units from Tote API.", "success")
        except Exception as e:
            flash(f"Refresh failed: {e}", "error")

    # Parameters
    bet_type = (request.values.get("bet_type") or "SUPERFECTA").strip().upper()
    product_id = (request.values.get("product_id") or "").strip()
    # Clamp top_n to at least the finish positions needed by bet type
    try:
        req_top_n = int(request.values.get("top_n", "7") or 7)
    except Exception:
        req_top_n = 7
    k_needed = 4 if bet_type == "SUPERFECTA" else (3 if bet_type == "TRIFECTA" else 2)
    top_n = max(k_needed, min(10, req_top_n))
    coverage = max(0.0, min(1.0, float(request.values.get("coverage", "0.6") or 0.6)))
    stake_per_line = float(request.values.get("stake_per_line", "0.10") or 0.10)
    t_takeout = request.values.get("takeout")
    f_share = float(request.values.get("f_share", "1.0") or 1.0)
    rollover = request.values.get("rollover")

    # Filters: country and course (venue)
    country = (request.values.get("country") or "").strip().upper()
    course = (request.values.get("course") or "").strip()
    date_filter = (request.values.get("date") or _today_iso()).strip()

    # Options for filters
    countries_df = sql_df(conn, (
        "SELECT DISTINCT UPPER(COALESCE(e.country,p.currency)) AS country "
        "FROM tote_products p LEFT JOIN tote_events e USING(event_id) "
        "WHERE UPPER(p.bet_type)=? AND COALESCE(p.status,'') <> '' AND COALESCE(e.country,p.currency) IS NOT NULL AND COALESCE(e.country,p.currency)<>'' "
        "ORDER BY country"
    ), params=(bet_type,))
    venues_df = sql_df(conn, (
        "SELECT DISTINCT COALESCE(e.venue,p.venue) AS venue "
        "FROM tote_products p LEFT JOIN tote_events e USING(event_id) "
        "WHERE UPPER(p.bet_type)=? "
        + (" AND UPPER(COALESCE(e.country,p.currency))=?" if country else "") +
        " AND COALESCE(p.status,'') <> '' AND COALESCE(e.venue,p.venue) IS NOT NULL AND COALESCE(e.venue,p.venue)<>'' "
        "ORDER BY venue"
    ), params=((bet_type, country) if country else (bet_type,)))
    # Product options for selection (recent/open by bet type, filtered)
    base_sql = (
        "SELECT p.product_id, p.event_id, p.event_name, COALESCE(e.venue, p.venue) AS venue, UPPER(COALESCE(e.country,p.currency)) AS country, p.start_iso, p.status, p.currency, p.total_gross, p.total_net, p.rollover, p.deduction_rate "
        "FROM tote_products p LEFT JOIN tote_events e USING(event_id) "
        "WHERE UPPER(p.bet_type)=? AND COALESCE(p.status,'') <> ''"
    )
    params = [bet_type]
    if country:
        base_sql += " AND UPPER(COALESCE(e.country,p.currency))=?"
        params.append(country)
    if course:
        base_sql += " AND UPPER(COALESCE(e.venue,p.venue)) LIKE UPPER(?)"
        params.append(course)
    if date_filter:
        base_sql += " AND substr(p.start_iso,1,10) = ?"
        params.append(date_filter)

    base_sql += " ORDER BY p.start_iso DESC LIMIT 400"
    opts = sql_df(conn, base_sql, params=tuple(params))

    # Fetch runners and units for selected product
    runners = []
    o_min = None
    top_lines = []
    total_lines = 0
    cum_p = 0.0
    efficiency = None
    if product_id:
        prod_df = sql_df(conn, "SELECT * FROM tote_products WHERE product_id=?", params=(product_id,))
        prod = (prod_df.to_dict("records")[0] if not prod_df.empty else None)
        # Deduct defaults
        try:
            t_default = float(prod.get("deduction_rate") or 0.30)
        except Exception:
            t_default = 0.30
        t_val = float(t_takeout) if t_takeout not in (None, "") else t_default
        try:
            R_default = float(prod.get("rollover") or 0.0)
        except Exception:
            R_default = 0.0
        R_val = float(rollover) if rollover not in (None, "") else R_default

        df_sel = sql_df(conn, (
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
                bq_probs = _bq_df(
                    "SELECT CAST(cloth_number AS INT64) AS number, CAST(decimal_odds AS FLOAT64) AS decimal_odds FROM vw_tote_probable_odds WHERE product_id = ?",
                    params=(product_id,)
                )
                # If empty, map via event_id to the WIN product for the same event
                if (bq_probs.empty) and prod and prod.get('event_id'):
                    bq_probs = _bq_df(
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
        # Local raw JSON fallback if still no runners (parse latest raw_tote_probable_odds)
        if (not runners) or len(runners) == 0:
            try:
                row = conn.execute("SELECT payload FROM raw_tote_probable_odds ORDER BY fetched_ts DESC LIMIT 1").fetchone()
                if row and row[0]:
                    pdata = json.loads(row[0])
                    nodes = (((pdata or {}).get("products") or {}).get("nodes")) or []
                    # Map selectionId -> cloth number from local selections (limit to this product's numbers)
                    sel_map = {}
                    for _, rr in df_sel.iterrows():
                        try:
                            sel_map[str(rr["selection_id"])]= int(rr["number"]) if rr["number"] is not None else None
                        except Exception:
                            pass
                    out = {}
                    for n in nodes:
                        try:
                            for line in (((n.get("lines") or {}).get("nodes")) or []):
                                legs = (line.get("legs") or [])
                                if not legs: continue
                                lsel = ((legs[0] or {}).get("lineSelections") or [])
                                if not lsel: continue
                                sid = str(lsel[0].get("selectionId"))
                                dec = line.get("odds", [{}])[0].get("decimal") if isinstance(line.get("odds"), list) else None
                                if sid in sel_map and dec:
                                    num = sel_map[sid]
                                    if num is not None:
                                        out[num] = float(dec)
                        except Exception:
                            continue
                    if out:
                        # Build runners list
                        rlist = []
                        for num, dec in out.items():
                            rlist.append({"number": int(num), "prob_odds": float(dec)})
                        # Compute strengths -> pct_units
                        import pandas as _pd
                        dfp = _pd.DataFrame(rlist)
                        dfp = dfp[dfp["prob_odds"].notnull() & (dfp["prob_odds"] > 0)]
                        if not dfp.empty:
                            dfp["strength"] = 1.0 / dfp["prob_odds"].astype(float)
                            tot = float(dfp["strength"].sum()) or 0.0
                            dfp["pct_units"] = dfp["strength"] / tot if tot > 0 else 0.0
                            # Join competitor names if available
                            if not df_sel.empty:
                                runners_df = df_sel[["number","competitor"]].copy()
                                dfp = dfp.merge(runners_df, on="number", how="left")
                            runners = dfp.fillna(0).to_dict("records")
            except Exception:
                pass

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
                conn.close()
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
                conn.close()
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
            except Exception:
                o_min_val = None
            o_min = o_min_val
        else:
            flash("No selection units found for this product yet. Click Refresh to pull latest from Tote.", "warning")

    conn.close()
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

    Uses: O_min = S/(f*(1-t)) - S
    """
    conn = _get_db_conn(); init_schema(conn)
    # Product options: recent/open SUPERFECTA for selection
    # Country filter (default GB)
    country = (request.values.get("country") or os.getenv("DEFAULT_COUNTRY", "GB")).strip().upper()
    opts = sql_df(
        conn,
        """
        SELECT p.product_id, p.event_id, p.event_name, COALESCE(e.venue, p.venue) AS venue, p.start_iso, p.currency, p.total_gross, p.total_net, COALESCE(p.status,'') AS status
        FROM tote_products p
        LEFT JOIN tote_events e USING(event_id)
        WHERE UPPER(p.bet_type)='SUPERFECTA' AND (UPPER(e.country)=? OR UPPER(p.currency)=?)
        ORDER BY p.start_iso DESC
        LIMIT 400
        """,
        params=(country, country)
    )
    product_id = (request.values.get("product_id") or "").strip()
    # Load product
    prod = None
    if product_id:
        pdf = sql_df(conn, "SELECT * FROM tote_products WHERE product_id=?", params=(product_id,))
        if not pdf.empty:
            prod = pdf.iloc[0].to_dict()
    # Runners count for permutations estimate (allow manual override)
    n_runners = None
    try:
        n_runners = int(request.values.get("n")) if request.values.get("n") else None
    except Exception:
        n_runners = None
    if prod and n_runners is None:
        try:
            ndf = sql_df(conn, "SELECT COUNT(1) AS n FROM tote_product_selections WHERE product_id=?", params=(product_id,))
            n_runners = int(ndf.iloc[0]["n"]) if not ndf.empty else None
        except Exception:
            n_runners = None
    # Infer takeout from totals if available (default 30%)
    t_default = 0.30
    t_infer = None
    if prod and prod.get("total_gross") and prod.get("total_net") and prod.get("total_gross") > 0:
        try:
            t_infer = 1.0 - float(prod["total_net"]) / float(prod["total_gross"])  # type: ignore[index]
        except Exception:
            t_infer = None
    try:
        takeout = float(request.values.get("takeout", t_infer if t_infer is not None else t_default))
    except Exception:
        takeout = t_infer if t_infer is not None else t_default
    # Strategy inputs
    mode = request.values.get("mode", "all")  # 'all' or 'custom'
    try:
        stake_per_combo = float(request.values.get("s", request.values.get("stake_per_combo", "1") or 1))
    except Exception:
        stake_per_combo = 1.0
    c_param = request.values.get("c", request.values.get("combos") or "")
    try:
        custom_combos = int(c_param) if c_param else None
    except Exception:
        custom_combos = None
    your_units_winner = request.values.get("yuw") or ""
    others_units_winner = request.values.get("ouw") or ""
    # Allow direct f override if provided
    f_override = request.values.get("f")
    # Rollover/seed (net)
    try:
        rollover = float(request.values.get("R", request.values.get("rollover", "0") or 0))
    except Exception:
        rollover = 0.0
    # Compute permutations if covering all
    C_all = None
    if n_runners and n_runners >= 4:
        try:
            C_all = n_runners * (n_runners - 1) * (n_runners - 2) * (n_runners - 3)
        except Exception:
            C_all = None
    C = (C_all if mode == "all" and C_all is not None else (custom_combos or 0))
    S = max(0.0, float(C) * float(stake_per_combo))
    # f share
    try:
        yuw = float(your_units_winner) if your_units_winner != "" else (stake_per_combo if C > 0 else 0.0)
    except Exception:
        yuw = stake_per_combo if C > 0 else 0.0
    try:
        ouw = float(others_units_winner) if others_units_winner != "" else 0.0
    except Exception:
        ouw = 0.0
    f = None
    try:
        if f_override not in (None, ""):
            f = float(f_override)
    except Exception:
        f = None
    if f is None:
        f = (yuw / (yuw + ouw)) if (yuw + ouw) > 0 else 0.0
    # Others' current gross in pool (assumes you haven't bet yet)
    O_current = None
    if prod and prod.get("total_gross") is not None:
        try:
            O_current = float(prod["total_gross"])  # type: ignore[index]
        except Exception:
            O_current = None
    # Compute thresholds
    O_min = None
    ROI_current = None
    viable_now = None
    return_current = None
    profit_current = None
    if S > 0 and 0 < takeout < 1 and f > 0:
        try:
            # O_min = S/(f*(1-t)) - S - R/(1-t)
            O_min = (S / (f * (1.0 - takeout))) - S - (rollover / (1.0 - takeout))
        except Exception:
            O_min = None
        if O_current is not None:
            try:
                # Return = f * ((1-t)*(S+O) + R)
                ret = f * ((1.0 - takeout) * (S + O_current) + rollover)
                ROI_current = (ret - S) / S
                viable_now = ret > S
                return_current = ret
                profit_current = ret - S
            except Exception:
                ROI_current = None
                viable_now = None
                return_current = None
                profit_current = None
    # Optional: BigQuery-calculated metrics for this product
    bq_calc = None
    upcoming = []
    if product_id and _use_bq():
        try:
            bqv = sql_df(conn, "SELECT product_id, combos, S, O_min, roi_current, viable_now FROM vw_today_gb_superfecta_be WHERE product_id=?", params=(product_id,))
            if not bqv.empty:
                row = bqv.iloc[0]
                bq_calc = {
                    "combos": row.get("combos"),
                    "S": row.get("S"),
                    "O_min": row.get("O_min"),
                    "roi_current": row.get("roi_current"),
                    "viable_now": row.get("viable_now"),
                }
        except Exception:
            bq_calc = None
    # Upcoming (next 60 min, GB) panel from BigQuery
    if _use_bq():
        try:
            uq = (
                "SELECT product_id, event_id, event_name, venue, country, start_iso, status, currency, "
                "n_competitors, combos, S, O_min, roi_current, viable_now "
                "FROM vw_gb_open_superfecta_next60_be ORDER BY start_iso"
            )
            udf = sql_df(conn, uq)
            if not udf.empty:
                upcoming = udf.to_dict("records")
        except Exception:
            upcoming = []
    result = {
        "product": prod,
        "n_runners": n_runners,
        "C_all": C_all,
        "mode": mode,
        "stake_per_combo": stake_per_combo,
        "C": C,
        "S": S,
        "takeout": takeout,
        "yuw": yuw,
        "ouw": ouw,
        "f": f,
        "O_current": O_current,
        "O_min": O_min,
        "ROI_current": ROI_current,
        "viable_now": viable_now,
        "t_infer": t_infer,
        "R": rollover,
        "t": takeout,
        "bq": bq_calc,
        "return_current": return_current,
        "profit_current": profit_current,
    }
    # Country options
    cdf = sql_df(conn, "SELECT DISTINCT country FROM tote_events WHERE country IS NOT NULL AND country<>'' ORDER BY country")
    conn.close()
    return render_template(
        "tote_viability.html",
        products=(opts.to_dict("records") if not opts.empty else []),
        product_id=product_id,
        result=result,
        country=country,
        country_options=(cdf['country'].tolist() if not cdf.empty else ['GB']),
        upcoming=(upcoming or []),
    )

@app.route("/api/tote/viability")
def api_tote_viability():
    """JSON endpoint to recompute viability with latest pool totals for auto-refresh."""
    conn = _get_db_conn(); init_schema(conn)
    product_id = (request.args.get("product_id") or "").strip()
    # Load product
    prod = None
    if product_id:
        pdf = sql_df(conn, "SELECT * FROM tote_products WHERE product_id=?", params=(product_id,))
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
                ndf = sql_df(conn, "SELECT COUNT(1) AS n FROM tote_product_selections WHERE product_id=?", params=(product_id,))
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
    conn.close()
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
    conn = _get_db_conn(); init_schema(conn)
    rows = sql_df(conn, "SELECT DISTINCT number, competitor FROM tote_product_selections WHERE product_id=? ORDER BY number", params=(pid,))
    conn.close()
    return app.response_class(json.dumps(rows.to_dict("records") if not rows.empty else []), mimetype="application/json")

@app.route("/api/tote/pool_snapshot/<product_id>")
def api_tote_pool_snapshot(product_id: str):
    """Return latest pool snapshot; if absent, fall back to tote_products totals.

    Avoids noisy 404s by providing best-effort data for UI auto-refresh.
    """
    conn = _get_db_conn(); init_schema(conn)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM tote_pool_snapshots WHERE product_id=? ORDER BY ts_ms DESC LIMIT 1",
            (product_id,),
        )
        row = cursor.fetchone()
        if row:
            cols = [d[0] for d in cursor.description]
            row_dict = {cols[i]: row[i] for i in range(len(cols))}
            return app.response_class(json.dumps(row_dict), mimetype="application/json")
        # Fallback: read from products
        pdf = sql_df(conn, "SELECT product_id, event_id, currency, status, start_iso, total_gross, total_net, rollover, deduction_rate FROM tote_products WHERE product_id=?", params=(product_id,))
        if not pdf.empty:
            p = pdf.iloc[0].to_dict()
            out = {
                "product_id": p.get("product_id"),
                "event_id": p.get("event_id"),
                "currency": p.get("currency"),
                "status": p.get("status"),
                "start_iso": p.get("start_iso"),
                "ts_ms": None,
                "total_gross": p.get("total_gross"),
                "total_net": p.get("total_net"),
                "rollover": p.get("rollover"),
                "deduction_rate": p.get("deduction_rate"),
                "source": "products_fallback",
            }
            return app.response_class(json.dumps(out), mimetype="application/json")
        return app.response_class(json.dumps({"error": "not found"}), mimetype="application/json", status=404)
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.route("/tote/audit/superfecta", methods=["POST"])
def tote_audit_superfecta_post():
    from .providers.tote_bets import place_audit_superfecta
    pid = (request.form.get("product_id") or "").strip()
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
    conn = _get_db_conn(); init_schema(conn)
    selections = None
    if sels_raw:
        # Split by newline or comma
        parts = [p.strip() for p in (sels_raw.replace("\r","\n").replace(",","\n").split("\n"))]
        selections = [p for p in parts if p]
    # Preflight checks: product status OPEN and selection IDs valid/active
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
            conn.close()
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
                conn.close(); return redirect(request.referrer or url_for('tote_superfecta_page'))
            missing = [str(n) for n in nums[:4] if n not in selmap]
            if missing:
                flash(f"Preflight: unknown numbers {{{','.join(missing)}}}.", "error")
                conn.close(); return redirect(request.referrer or url_for('tote_superfecta_page'))
            inactive = [str(n) for n in nums[:4] if selmap.get(n,(None,None))[1] not in ("ACTIVE","OPEN","AVAILABLE","VALID")]
            if inactive:
                flash(f"Preflight: selection(s) not active: {{{','.join(inactive)}}}.", "error")
                conn.close(); return redirect(request.referrer or url_for('tote_superfecta_page'))
    except Exception as e:
        # Non-fatal; continue to attempt placement but surface info
        flash(f"Preflight warning: {e}", "warning")
    # Resolve placement-visible product id (match by event/date), with alias fallback
    placement_pid = pid
    try:
        from .providers.tote_api import ToteClient
        client = ToteClient()
        row = conn.execute("SELECT event_id, substr(start_iso,1,10) FROM tote_products WHERE product_id=?", (pid,)).fetchone()
        event_id = row[0] if row else None
        day = row[1] if row else None
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
    res = place_audit_superfecta(conn, product_id=pid, selection=(sel or None), selections=selections, stake=sk, currency=currency, post=post_flag, stake_type=stake_type, placement_product_id=placement_pid)
    conn.commit(); conn.close()
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
    """List recent audit bets from local DB with links to detail."""
    conn = _get_db_conn(); init_schema(conn)
    rows = conn.execute(
        "SELECT bet_id, ts, mode, product_id, selection, stake, currency, status, outcome FROM tote_bets ORDER BY ts DESC LIMIT 50"
    ).fetchall()
    conn.close()
    cols = ["bet_id","ts","mode","product_id","selection","stake","currency","status","outcome"]
    bets = [{cols[i]: r[i] for i in range(len(cols))} for r in rows]
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

@app.route("/event/<event_id>")
def event_detail(event_id: str):
    """Display details for a single event."""
    conn = _get_db_conn()
    
    # Fetch event details
    event_df = sql_df(conn, "SELECT * FROM tote_events WHERE event_id=?", params=(event_id,))
    if event_df.empty:
        flash("Event not found", "error")
        return redirect(url_for("tote_events_page"))
    event = event_df.to_dict("records")[0]

    # Fetch conditions
    conditions_df = sql_df(conn, "SELECT * FROM race_conditions WHERE event_id=?", params=(event_id,))
    conditions = None if conditions_df.empty else conditions_df.to_dict("records")[0]

    # Fetch runners/competitors
    runners = []
    competitors = []
    if event.get("competitors_json"):
        try:
            competitors = json.loads(event["competitors_json"])
            if event.get("sport") == "horse_racing":
                runners = competitors
        except json.JSONDecodeError:
            pass

    # Fetch products
    products_df = sql_df(conn, "SELECT * FROM tote_products WHERE event_id=? ORDER BY bet_type", params=(event_id,))
    products = products_df.to_dict("records") if not products_df.empty else []

    # Fetch features
    features_df = sql_df(conn, "SELECT * FROM vw_runner_features WHERE event_id=?", params=(event_id,))
    features = features_df.to_dict("records") if not features_df.empty else []

    # Fetch runner rows (placeholder)
    runner_rows = []

    conn.close()

    return render_template(
        "event_detail.html",
        event=event,
        conditions=conditions,
        runners=runners,
        products=products,
        features=features,
        runner_rows=runner_rows,
        competitors=competitors,
    )

@app.route("/tote-superfecta/<product_id>")
def tote_superfecta_detail(product_id: str):
    conn = _get_db_conn(); init_schema(conn)
    pdf = sql_df(conn, "SELECT * FROM tote_products WHERE product_id=?", params=(product_id,))
    if pdf.empty:
        conn.close()
        flash("Unknown product id", "error")
        return redirect(url_for("tote_superfecta_page"))
    p = pdf.iloc[0].to_dict()
    runners_df = sql_df(conn, "SELECT DISTINCT number, competitor FROM tote_product_selections WHERE product_id=? ORDER BY number", params=(product_id,))
    runners = runners_df.to_dict("records") if not runners_df.empty else []
    items = []
    for r in runners:
        items.append({
            'id': r.get('competitor'),
            'name': r.get('competitor'),
            'cloth': r.get('number')
        })
    items = [x for x in items if x.get('cloth') is not None]
    items.sort(key=lambda x: x.get('cloth'))
    # Load any reported dividends for this product (latest per selection)
    divs = sql_df(
        conn,
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
        conn,
        """
        SELECT horse_id, finish_pos, status, cloth_number
        FROM hr_horse_runs WHERE event_id=? AND finish_pos IS NOT NULL
        ORDER BY finish_pos ASC
        """,
        params=(p.get('event_id'),)
    )
    # Load conditions (going + weather)
    cond = sql_df(
        conn,
        "SELECT going, weather_temp_c, weather_wind_kph, weather_precip_mm FROM race_conditions WHERE event_id=?",
        params=(p.get('event_id'),)
    )
    # Recent audit bets for this product
    recent = sql_df(conn, "SELECT bet_id, ts, mode, selection, stake, currency, status, error, outcome FROM tote_bets WHERE product_id=? ORDER BY ts DESC LIMIT 20", params=(product_id,))
    conn.close()
    dividends = divs.to_dict("records") if not divs.empty else []
    finishing = fr.to_dict("records") if not fr.empty else []
    conditions = cond.iloc[0].to_dict() if not cond.empty else {}
    return render_template("tote_superfecta_detail.html", product=p, runners=items, dividends=dividends, finishing=finishing, conditions=conditions, recent_bets=(recent.to_dict("records") if not recent.empty else []))

@app.route("/api/tote/bet_status")
def api_tote_bet_status():
    from .providers.tote_bets import refresh_bet_status
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
