import streamlit as st
import pandas as pd
from datetime import date
from typing import Any
import traceback
import os
import requests
import google.auth
from google.auth.transport.requests import AuthorizedSession

# It's better to use the existing db and query functions
from autobet.sports.webapp import sql_df
from autobet.sports.config import cfg
from autobet.sports.providers.tote_api import ToteClient, ToteError

st.set_page_config(layout="wide", page_title="Autobet Performance Dashboard")

st.title("Autobet Performance Dashboard")
st.markdown("A dashboard to analyze betting history, P&L, and model performance using data from BigQuery.")

# --- Data Loading ---
@st.cache_data(ttl=60) # Cache for 1 minute
def get_tote_balance():
    """Fetches the current account balance from the Tote API."""
    if not (cfg.tote_api_key and cfg.tote_graphql_url):
        return None, "Tote API credentials not configured for live account."

    try:
        # Use live client
        client = ToteClient(base_url=cfg.tote_graphql_url, api_key=cfg.tote_api_key)
        
        query = """
            query GetCustomerWallet {
              customer {
                wallet {
                  balances {
                    currency { code }
                    decimalAmount
                  }
                }
              }
            }
        """
        data = client.graphql(query, keep_auth=True)
        balances = data.get("customer", {}).get("wallet", {}).get("balances", [])
        if balances:
            # Assuming we take the first balance found.
            balance_data = balances[0]
            amount = float(balance_data.get("decimalAmount", 0.0))
            currency = balance_data.get("currency", {}).get("code", "GBP")
            return f"{currency} {amount:,.2f}", None
        return None, f"Could not parse balance from API response. Data: {data}"
    except ToteError as e:
        return None, f"Tote API Error: {e}"
    except Exception as e:
        return None, f"An unexpected error occurred: {e}"

@st.cache_data(ttl=300)
def load_bets():
    """
    Loads and processes bet data from BigQuery.
    - Fetches all audit bets and joins with product/event info.
    - Fetches dividend data and identifies winning selections.
    - Explodes multi-line bets into individual lines.
    - Calculates stake per line.
    - Joins with dividends to determine wins and calculate P&L.
    """
    # 1. Updated query to join events table for venue info
    bets_query = """
        SELECT
          b.bet_id,
          b.ts_ms,
          b.mode,
          b.status AS placement_status,
          b.selection,
          b.stake,
          b.currency,
          b.product_id,
          p.event_id,
          p.event_name,
          p.start_iso,
          p.bet_type,
          p.status as product_status,
          e.venue
        FROM `tote_audit_bets` b
        LEFT JOIN `tote_products` p ON b.product_id = p.product_id
        LEFT JOIN `tote_events` e ON p.event_id = e.event_id
    """
    bets_df = sql_df(bets_query, cache_ttl=300)

    dividends_query = "SELECT product_id, selection, dividend FROM `tote_product_dividends`"
    dividends_df = sql_df(dividends_query, cache_ttl=300)

    if bets_df.empty:
        return pd.DataFrame()

    # 2. Create a summary of winning selections per product
    winning_selections_df = dividends_df[dividends_df['dividend'] > 0].groupby('product_id')['selection'].apply(lambda x: ', '.join(x)).reset_index()
    winning_selections_df.rename(columns={'selection': 'winning_selections'}, inplace=True)

    # 3. Add human-readable datetime
    bets_df['bet_time'] = pd.to_datetime(bets_df['ts_ms'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(None)

    # 4. Explode multi-line bets
    bets_df['lines'] = bets_df['selection'].str.split(',')
    bets_df = bets_df.explode('lines')
    bets_df.rename(columns={'lines': 'line_selection'}, inplace=True)
    bets_df['line_selection'] = bets_df['line_selection'].str.strip()

    # 5. Calculate stake per line
    bets_df['num_lines'] = bets_df.groupby('bet_id')['bet_id'].transform('count')
    bets_df['stake_per_line'] = bets_df['stake'] / bets_df['num_lines']

    # 6. Merge with winning selections summary
    merged_df = pd.merge(
        bets_df,
        winning_selections_df,
        on='product_id',
        how='left'
    )

    # 7. Merge with individual dividends to find winning lines
    merged_df = pd.merge(
        merged_df,
        dividends_df,
        how='left',
        left_on=['product_id', 'line_selection'],
        right_on=['product_id', 'selection']
    )

    # 8. Calculate P&L and handle dtypes to fix FutureWarning
    merged_df['is_win'] = (merged_df['dividend'].notna() & (merged_df['dividend'] > 0)).astype('boolean')
    merged_df['winnings'] = 0.0
    # Tote dividends are typically for a £1 stake
    merged_df.loc[merged_df['is_win'].fillna(False), 'winnings'] = merged_df['stake_per_line'] * merged_df['dividend']
    merged_df['pnl'] = merged_df['winnings'] - merged_df['stake_per_line']
    
    # Only count P&L for settled/resulted products
    resulted_mask = merged_df['product_status'] == 'RESULTED'
    merged_df.loc[~resulted_mask, 'pnl'] = 0
    merged_df.loc[~resulted_mask, 'winnings'] = 0
    merged_df.loc[~resulted_mask, 'is_win'] = pd.NA
    merged_df['winning_selections'] = merged_df['winning_selections'].fillna('N/A')

    return merged_df

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
        "total_lines": C, "lines_covered": M_adj, "coverage_frac": alpha, "stake_total": S, "net_pool_if_bet": NetPool_now, "f_share_used": f_used, "expected_return": ExpReturn, "expected_profit": ExpProfit, "is_positive_ev": (ExpProfit > 0), "cover_all_stake": cover_all_stake, "cover_all_expected_profit": ExpProfit_all, "cover_all_is_positive": cover_all_pos, "cover_all_o_min_break_even": O_min, "coverage_frac_min_positive": alpha_min,
    }


# --- Status Page Functions ---

def _gcp_project_region():
    proj = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or cfg.bq_project
    region = os.getenv("GCP_REGION") or os.getenv("CLOUD_RUN_REGION") or os.getenv("REGION") or "europe-west2"
    return proj, region

def _gcp_auth_session():
    try:
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        return AuthorizedSession(creds)
    except Exception:
        return None

@st.cache_data(ttl=60)
def get_data_freshness():
    """Fetches data freshness stats from BigQuery."""
    try:
        ev_today = sql_df("SELECT COUNT(1) AS c FROM tote_events WHERE SUBSTR(start_iso,1,10)=FORMAT_DATE('%F', CURRENT_DATE())")
        ev_last = sql_df("SELECT TIMESTAMP_MILLIS(MAX(ended_ts)) AS ts FROM ingest_job_runs WHERE task IN ('ingest_events_for_day','ingest_events_range') AND status='OK'")
        pr_today = sql_df("SELECT COUNT(1) AS c FROM tote_products WHERE SUBSTR(start_iso,1,10)=FORMAT_DATE('%F', CURRENT_DATE())")
        pr_last = sql_df("SELECT TIMESTAMP_MILLIS(MAX(ended_ts)) AS ts FROM ingest_job_runs WHERE task IN ('ingest_products_for_day','ingest_single_product') AND status='OK'")
        po_today = sql_df("SELECT COUNT(1) AS c FROM raw_tote_probable_odds WHERE DATE(TIMESTAMP_MILLIS(fetched_ts))=CURRENT_DATE()")
        po_last = sql_df("SELECT TIMESTAMP_MILLIS(MAX(fetched_ts)) AS ts FROM raw_tote_probable_odds")
        ps_today = sql_df("SELECT COUNT(1) AS c FROM tote_pool_snapshots WHERE DATE(TIMESTAMP_MILLIS(ts_ms))=CURRENT_DATE()")
        ps_last = sql_df("SELECT TIMESTAMP_MILLIS(MAX(ts_ms)) AS ts FROM tote_pool_snapshots")

        def _format_ts(df_col):
            if df_col.empty or pd.isna(df_col.iloc[0]):
                return "N/A"
            # Convert pandas Timestamp (which is UTC from BQ) to local time string
            return df_col.iloc[0].tz_convert(None).strftime('%Y-%m-%d %H:%M:%S')

        return {
            "events": {"today": int(ev_today.iloc[0]["c"]) if not ev_today.empty else 0, "last": _format_ts(ev_last['ts'])},
            "products": {"today": int(pr_today.iloc[0]["c"]) if not pr_today.empty else 0, "last": _format_ts(pr_last['ts'])},
            "probable_odds": {
                "today": int(po_today.iloc[0]["c"]) if not po_today.empty else 0,
                "last": _format_ts(po_last['ts']),
            },
            "pool_snapshots": {
                "today": int(ps_today.iloc[0]["c"]) if not ps_today.empty else 0,
                "last": _format_ts(ps_last['ts']),
            },
        }
    except Exception as e:
        return {"error": str(e)}

@st.cache_data(ttl=60)
def get_qc_stats():
    """Fetches data quality stats from BigQuery."""
    try:
        out = {}
        df = sql_df("SELECT COUNT(DISTINCT product_id) AS c FROM vw_qc_today_missing_runner_numbers")
        out["missing_runner_numbers"] = int(df.iloc[0]["c"]) if not df.empty else 0
        df = sql_df("SELECT COUNT(1) AS c FROM vw_qc_missing_bet_rules")
        out["missing_bet_rules"] = int(df.iloc[0]["c"]) if not df.empty else 0
        df = sql_df("SELECT AVG(coverage) AS avg_cov FROM vw_qc_probable_odds_coverage")
        out["probable_odds_avg_cov"] = float(df.iloc[0]["avg_cov"]) if not df.empty and pd.notna(df.iloc[0]["avg_cov"]) else None
        df = sql_df("SELECT COUNT(1) AS c FROM vw_qc_today_gb_sf_missing_snapshots")
        out["gb_sf_missing_snapshots"] = int(df.iloc[0]["c"]) if not df.empty else 0
        return out
    except Exception as e:
        return {"error": str(e)}

@st.cache_data(ttl=60)
def get_gcp_status():
    """Fetches status of GCP resources like Cloud Run and Scheduler."""
    project, region = _gcp_project_region()
    if not project:
        return {"error": "GCP project not configured"}

    sess = _gcp_auth_session()
    if not sess:
        return {"error": "GCP auth unavailable"}

    out = {"project": project, "region": region, "cloud_run": {}, "scheduler": {}, "pubsub": {}}

    # Cloud Run
    try:
        services: list[dict] = []
        url = f"https://run.googleapis.com/v2/projects/{project}/locations/{region}/services"
        r = sess.get(url, timeout=10)
        if r.status_code == 200:
            for j in (r.json().get("services") or []):
                conds = {c.get("type"): c.get("state") or c.get("status") for c in j.get("conditions", [])}
                ok_vals = {"CONDITION_SUCCEEDED", "True", True}
                ready_from_sub = (conds.get("RoutesReady") in ok_vals) and (conds.get("ConfigurationsReady") in ok_vals)
                ready = bool((conds.get("Ready") in ok_vals) or ready_from_sub or bool(j.get("latestReadyRevision")))
                services.append({
                    "name": (j.get("name") or "").split("/")[-1], "uri": j.get("uri"), "ready": ready,
                })
        else:
            out["cloud_run"]["error"] = f"API returned {r.status_code}"
        out["cloud_run"]["services"] = services
    except Exception as e:
        out["cloud_run"]["error"] = str(e)

    # Cloud Scheduler
    try:
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{project}/locations/{region}/jobs"
        r = sess.get(url, timeout=10)
        jobs = []
        if r.status_code == 200:
            for j in r.json().get("jobs", []):
                jobs.append({
                    "name": (j.get("name") or "").split("/")[-1], "schedule": j.get("schedule"), "state": j.get("state"),
                })
        else:
            out["scheduler"]["error"] = f"API returned {r.status_code}"
        out["scheduler"]["jobs"] = jobs
    except Exception as e:
        out["scheduler"]["error"] = str(e)

    # Pub/Sub
    try:
        topic_id = os.getenv("PUBSUB_TOPIC_ID", "ingest-jobs")
        topic = f"projects/{project}/topics/{topic_id}"
        sub = f"projects/{project}/subscriptions/ingest-fetcher-sub"
        t = sess.get(f"https://pubsub.googleapis.com/v1/{topic}", timeout=10)
        s = sess.get(f"https://pubsub.googleapis.com/v1/{sub}", timeout=10)
        out["pubsub"] = {
            "topic_exists": t.status_code == 200,
            "subscription_exists": s.status_code == 200,
        }
    except Exception as e:
        out["pubsub"]["error"] = str(e)

    return out

@st.cache_data(ttl=30)
def get_job_log():
    """Fetches the last 50 job logs from BigQuery."""
    try:
        df = sql_df(
            "SELECT job_id, component, task, status, started_ts, ended_ts, duration_ms, error "
            "FROM ingest_job_runs ORDER BY started_ts DESC LIMIT 50"
        )
        df['started_ts'] = pd.to_datetime(df['started_ts'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(None)
        return df
    except Exception as e:
        st.error(f"Failed to fetch job log: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_upcoming_races_status():
    """Fetches upcoming races for the status page."""
    # This view `vw_gb_open_superfecta_next240_be` is not defined in bq.py.
    # Let's try the `next60` version and then a generic fallback.
    try:
        # First, try the 60-minute view which is known to exist.
        df = sql_df("SELECT product_id, event_name, venue, start_iso, roi_current, viable_now FROM vw_gb_open_superfecta_next60_be ORDER BY start_iso")
    except Exception:
        # This might fail if the view doesn't exist, so we'll fall back.
        df = pd.DataFrame()

    if not df.empty:
        df['start_iso'] = pd.to_datetime(df['start_iso']).dt.tz_convert(None)
        return df

    try:
        # Fallback to a generic query for any open Superfecta in the next 4 hours.
        df = sql_df("SELECT p.product_id, p.event_name, p.venue, p.start_iso FROM vw_products_latest_totals p WHERE UPPER(p.bet_type) = 'SUPERFECTA' AND p.status = 'OPEN' AND TIMESTAMP(p.start_iso) BETWEEN CURRENT_TIMESTAMP() AND TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR) ORDER BY p.start_iso")
        if not df.empty:
            df['start_iso'] = pd.to_datetime(df['start_iso']).dt.tz_convert(None)
        return df
    except Exception:
        # If all else fails, return an empty DataFrame.
        return pd.DataFrame()

@st.cache_data(ttl=120)
def fetch_live_bets_from_api(since, until):
    """Fetches bets directly from the Tote API for a given date range."""
    if not (cfg.tote_api_key and cfg.tote_graphql_url):
        st.error("Tote API credentials not configured for live account.")
        return pd.DataFrame()

    try:
        client = ToteClient(base_url=cfg.tote_graphql_url, api_key=cfg.tote_api_key)
        
        query = """
            query GetBets($since: DateTime, $until: DateTime, $first: Int, $after: String) {
              bets(since: $since, until: $until, first: $first, after: $after) {
                pageInfo { hasNextPage endCursor }
                nodes {
                  id
                  toteId
                  betType { code }
                  placement {
                    status
                    stake { decimalAmount currency { code } }
                    payout { decimalAmount }
                    settledTimeUTC
                    legs {
                      nodes {
                        event {
                          id
                          name
                          scheduledStartDateTime { iso8601 }
                        }
                        selections {
                          nodes {
                            name
                            finishingPosition
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
        """
        
        all_bets = []
        variables = {"since": since, "until": until, "first": 100}
        
        while True:
            data = client.graphql(query, variables, keep_auth=True)
            bets_data = data.get("bets", {})
            nodes = bets_data.get("nodes", [])
            all_bets.extend(nodes)
            
            page_info = bets_data.get("pageInfo", {})
            if page_info.get("hasNextPage") and page_info.get("endCursor"):
                variables["after"] = page_info["endCursor"]
            else:
                break
        
        if not all_bets:
            return pd.DataFrame()

        # Flatten the complex JSON into a pandas DataFrame
        flat_bets = []
        for bet in all_bets:
            placement = bet.get("placement", {})
            stake = placement.get("stake", {})
            legs = (placement.get("legs", {}).get("nodes", []) or [{}])[0]
            event = legs.get("event", {})
            selections = (legs.get("selections", {}).get("nodes", []) or [])
            
            flat_bet = {
                "bet_id": bet.get("id"), "tote_id": bet.get("toteId"), "status": placement.get("status"),
                "bet_type": (bet.get("betType") or {}).get("code"), "stake": stake.get("decimalAmount"),
                "currency": (stake.get("currency") or {}).get("code"), "payout": (placement.get("payout") or {}).get("decimalAmount"),
                "settled_time": placement.get("settledTimeUTC"), "event_name": event.get("name"),
                "event_start": (event.get("scheduledStartDateTime") or {}).get("iso8601"),
                "selections": ", ".join([s.get("name", "") for s in selections]),
            }
            flat_bets.append(flat_bet)

        df = pd.DataFrame(flat_bets)
        if not df.empty:
            df['settled_time'] = pd.to_datetime(df['settled_time']).dt.tz_convert(None)
            df['event_start'] = pd.to_datetime(df['event_start']).dt.tz_convert(None)
        return df

    except ToteError as e:
        st.error(f"Tote API Error: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return pd.DataFrame()

# --- Sidebar ---
st.sidebar.header("Navigation")
page = st.sidebar.radio("Go to", [
    "Overview", 
    "Bet Performance", 
    "Live Account", 
    "Status",
    "Tote Events",
    "Tote Superfecta",
    "Viability Calculator",
    "Model Review", 
    "Model Deep Dive"
])


# --- Page Content ---
if page == "Overview":
    st.header("Dashboard Overview")
    
    st.subheader("Live Account Balance")
    balance, error = get_tote_balance()
    if error:
        st.warning(error)
    elif balance:
        st.metric("Current Balance", balance)
    else:
        st.info("Balance not available.")

    with st.spinner("Loading bet performance data..."):
        bets_df = load_bets()

    if bets_df.empty:
        st.warning("No bet data found. Place some audit bets to see performance metrics.")
    else:
        # Calculate summary metrics for resulted bets
        resulted_bets_df = bets_df[bets_df['product_status'] == 'RESULTED'].copy()
        total_stake = resulted_bets_df['stake_per_line'].sum()
        total_winnings = resulted_bets_df['winnings'].sum()
        total_pnl = resulted_bets_df['pnl'].sum()
        roi = (total_pnl / total_stake) * 100 if total_stake > 0 else 0
        num_lines = len(resulted_bets_df)
        win_rate = (resulted_bets_df['is_win'].sum() / num_lines) * 100 if num_lines > 0 else 0

        # Display KPIs
        st.subheader("Performance KPIs (Resulted Bets)")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total P&L", f"£{total_pnl:,.2f}", delta=f"{total_pnl:,.2f}")
        col2.metric("Total Staked", f"£{total_stake:,.2f}")
        col3.metric("ROI", f"{roi:.2f}%")
        col4.metric("Win Rate (Lines)", f"{win_rate:.2f}%")

        # Bankroll chart
        st.subheader("Bankroll Over Time")
        resulted_bets_df['bet_time'] = pd.to_datetime(resulted_bets_df['ts_ms'], unit='ms')
        daily_pnl = resulted_bets_df.set_index('bet_time').resample('D')['pnl'].sum().cumsum()
        bankroll_over_time = cfg.paper_starting_bankroll + daily_pnl
        st.line_chart(bankroll_over_time)

elif page == "Bet Performance":
    st.header("Bet Performance Details")
    with st.spinner("Loading bet performance data..."):
        bets_df = load_bets()

    if bets_df.empty:
        st.warning("No bet data found.")
    else:
        # --- Filters ---
        st.subheader("Filters")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            all_modes = ['all'] + bets_df['mode'].unique().tolist()
            selected_mode = st.selectbox("Filter by Mode", options=all_modes, index=0)
        
        with col2:
            all_bet_types = ['all'] + bets_df['bet_type'].dropna().unique().tolist()
            selected_bet_type = st.selectbox("Filter by Bet Type", options=all_bet_types, index=0)

        with col3:
            min_date = bets_df['bet_time'].min().date()
            max_date = bets_df['bet_time'].max().date()
            date_range = st.date_input(
                "Filter by Bet Date",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
            )

        # Apply filters
        filtered_df = bets_df.copy()
        if selected_mode != 'all':
            filtered_df = filtered_df[filtered_df['mode'] == selected_mode]
        if selected_bet_type != 'all':
            filtered_df = filtered_df[filtered_df['bet_type'] == selected_bet_type]
        if len(date_range) == 2:
            start_date, end_date = date_range
            filtered_df = filtered_df[
                (filtered_df['bet_time'].dt.date >= start_date) & 
                (filtered_df['bet_time'].dt.date <= end_date)
            ]

        # --- KPIs for filtered data ---
        st.subheader("Filtered Performance KPIs")
        resulted_filtered_df = filtered_df[filtered_df['product_status'] == 'RESULTED'].copy()
        
        total_stake = resulted_filtered_df['stake_per_line'].sum()
        total_pnl = resulted_filtered_df['pnl'].sum()
        roi = (total_pnl / total_stake) * 100 if total_stake > 0 else 0
        num_lines = len(resulted_filtered_df)
        win_rate = (resulted_filtered_df['is_win'].sum() / num_lines) * 100 if num_lines > 0 else 0

        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("Total P&L", f"£{total_pnl:,.2f}", delta=f"{total_pnl:,.2f}")
        kpi2.metric("Total Staked", f"£{total_stake:,.2f}")
        kpi3.metric("ROI", f"{roi:.2f}%")
        kpi4.metric("Win Rate (Lines)", f"{win_rate:.2f}%" if pd.notna(win_rate) else "N/A")

        # --- Display Data ---
        st.subheader("Bet Details")
        display_df = filtered_df[[
            'bet_time', 'mode', 'bet_type', 'event_name', 'venue',
            'line_selection', 'stake_per_line', 'is_win', 'pnl', 'winning_selections',
            'product_status', 'placement_status'
        ]].sort_values('bet_time', ascending=False)

        st.dataframe(
            display_df,
            hide_index=True,
            use_container_width=True
        )

elif page == "Status":
    st.header("System Status")
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    # --- Data Freshness ---
    st.subheader("Data Freshness")
    freshness = get_data_freshness()
    if freshness.get("error"):
        st.error(f"Could not load data freshness: {freshness['error']}")
    else:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Events Today", freshness.get("events", {}).get("today", 0))
            st.caption(f"Last Ingest: {freshness.get('events', {}).get('last', 'N/A')}")
        with c2:
            st.metric("Products Today", freshness.get("products", {}).get("today", 0))
            st.caption(f"Last Ingest: {freshness.get('products', {}).get('last', 'N/A')}")
        with c3:
            st.metric("Probable Odds Today", freshness.get("probable_odds", {}).get("today", 0))
            st.caption(f"Last Ingest: {freshness.get('probable_odds', {}).get('last', 'N/A')}")
        with c4:
            st.metric("Pool Snapshots Today", freshness.get("pool_snapshots", {}).get("today", 0))
            st.caption(f"Last Ingest: {freshness.get('pool_snapshots', {}).get('last', 'N/A')}")

    # --- Data Quality ---
    st.subheader("Data Quality Checks")
    qc = get_qc_stats()
    if qc.get("error"):
        st.error(f"Could not load QC stats: {qc['error']}")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Products w/ Missing Runners", qc.get("missing_runner_numbers", "N/A"), delta_color="inverse")
        c2.metric("Products w/ Missing Bet Rules", qc.get("missing_bet_rules", "N/A"), delta_color="inverse")
        c3.metric("GB SF Missing Snapshots", qc.get("gb_sf_missing_snapshots", 0), delta_color="inverse")

    # --- GCP Status ---
    with st.expander("GCP Status", expanded=False):
        gcp = get_gcp_status()
        if gcp.get("error"):
            st.error(f"Could not load GCP status: {gcp['error']}")
        else:
            st.write(f"**Project:** `{gcp.get('project')}` | **Region:** `{gcp.get('region')}`")
            
            st.markdown("##### Cloud Run Services")
            run_services = gcp.get("cloud_run", {}).get("services", [])
            if not run_services:
                st.info("No Cloud Run services found.")
            else:
                for service in run_services:
                    st.text(f"- {service['name']}: {'✅ Ready' if service['ready'] else '❌ Not Ready'}")

            st.markdown("##### Cloud Scheduler Jobs")
            scheduler_jobs = gcp.get("scheduler", {}).get("jobs", [])
            if not scheduler_jobs:
                st.info("No Cloud Scheduler jobs found.")
            else:
                for job in scheduler_jobs:
                    st.text(f"- {job['name']}: {job['state']} ({job['schedule']})")

            st.markdown("##### Pub/Sub")
            pubsub_status = gcp.get("pubsub", {})
            st.text(f"- Ingest Topic Exists: {'✅ Yes' if pubsub_status.get('topic_exists') else '❌ No'}")
            st.text(f"- Ingest Subscription Exists: {'✅ Yes' if pubsub_status.get('subscription_exists') else '❌ No'}")

    # --- Upcoming Races ---
    with st.expander("Upcoming Races (Next 4 Hours)", expanded=False):
        upcoming_df = get_upcoming_races_status()
        if upcoming_df.empty:
            st.info("No upcoming viable races found.")
        else:
            st.dataframe(upcoming_df, use_container_width=True, hide_index=True)

    # --- Job Log ---
    with st.expander("Recent Ingest Jobs", expanded=True):
        job_log_df = get_job_log()
        if job_log_df.empty:
            st.info("No job logs found.")
        else:
            st.dataframe(job_log_df, use_container_width=True, hide_index=True)

elif page == "Tote Events":
    st.header("Tote Events")

    # --- Filters ---
    try:
        # Use the new materialized views for faster filter loading
        countries_df = sql_df("SELECT country FROM `mv_event_filters_country` ORDER BY country")
        sports_df = sql_df("SELECT sport FROM `mv_event_filters_sport` ORDER BY sport")
        venues_df = sql_df("SELECT venue FROM `mv_event_filters_venue` ORDER BY venue")
        countries_list = countries_df['country'].tolist()
        sports_list = sports_df['sport'].tolist()
        venues_list = venues_df['venue'].tolist()
    except Exception:
        # Fallback to old method if MV doesn't exist or fails
        countries_df = sql_df("SELECT DISTINCT country FROM `tote_events` WHERE country IS NOT NULL AND country<>'' ORDER BY country")
        sports_df = sql_df("SELECT DISTINCT sport FROM `tote_events` WHERE sport IS NOT NULL AND sport<>'' ORDER BY sport")
        venues_df = sql_df("SELECT DISTINCT venue FROM `tote_events` WHERE venue IS NOT NULL AND venue<>'' ORDER BY venue")
        countries_list = countries_df['country'].tolist()
        sports_list = sports_df['sport'].tolist()
        venues_list = venues_df['venue'].tolist()

    col1, col2, col3 = st.columns(3)
    with col1:
        country = st.selectbox("Country", [""] + countries_list)
    with col2:
        sport = st.selectbox("Sport", [""] + sports_list)
    with col3:
        venue = st.selectbox("Venue", [""] + venues_list)

    col4, col5 = st.columns(2)
    with col4:
        name = st.text_input("Event Name Filter")
    with col5:
        date_range = st.date_input("Date Range", value=())

    limit = st.number_input("Limit", min_value=1, max_value=1000, value=200)

    # --- Query ---
    where = []
    params: dict[str, Any] = {}
    if country:
        where.append("UPPER(country)=@country"); params["country"] = country.upper()
    if sport:
        where.append("sport=@sport"); params["sport"] = sport
    if venue:
        where.append("UPPER(venue) LIKE UPPER(@venue)"); params["venue"] = f"%{venue}%"
    if name:
        where.append("UPPER(name) LIKE UPPER(@name)"); params["name"] = f"%{name.upper()}%"
    if len(date_range) == 2:
        where.append("SUBSTR(start_iso,1,10) BETWEEN @dfrom AND @dto")
        params["dfrom"] = date_range[0].isoformat()
        params["dto"] = date_range[1].isoformat()

    base_sql = "SELECT event_id, name, venue, country, start_iso, sport, status FROM `tote_events`"
    if where:
        base_sql += " WHERE " + " AND ".join(where)
    
    order_by = " ORDER BY start_iso DESC"
    if len(date_range) == 2:
        order_by = " ORDER BY start_iso ASC"
    
    base_sql += order_by + " LIMIT @limit"
    params["limit"] = limit

    with st.spinner("Fetching events..."):
        df = sql_df(base_sql, params=params, cache_ttl=60)

    st.subheader("Events")
    st.dataframe(df, use_container_width=True, hide_index=True)

elif page == "Tote Superfecta":
    st.header("Tote Superfecta Products")

    # --- Filters ---
    try:
        cdf = sql_df("SELECT country FROM `mv_event_filters_country` ORDER BY country")
        vdf = sql_df("SELECT venue FROM `mv_event_filters_venue` ORDER BY venue")
    except Exception:
        # Fallback to direct queries
        cdf = sql_df("SELECT DISTINCT country FROM `tote_events` WHERE country IS NOT NULL AND country<>'' ORDER BY country")
        vdf = sql_df("SELECT DISTINCT venue FROM `tote_events` WHERE venue IS NOT NULL AND venue<>'' ORDER BY venue")
    sdf = sql_df("SELECT DISTINCT COALESCE(status,'') AS status FROM tote_products WHERE bet_type='SUPERFECTA' ORDER BY 1")

    col1, col2, col3 = st.columns(3)
    with col1:
        country = st.selectbox("Country", [""] + cdf['country'].tolist(), index=1) # Default to GB
    with col2:
        venue = st.selectbox("Venue", [""] + vdf['venue'].tolist())
    with col3:
        status = st.selectbox("Status", [""] + sdf['status'].tolist(), index=1) # Default to OPEN

    date_range = st.date_input("Date Range", value=(date.today(), date.today()))
    limit = st.number_input("Limit", min_value=1, max_value=1000, value=200)

    # --- Query ---
    where = ["UPPER(p.bet_type)='SUPERFECTA'"]
    params: dict[str, Any] = {}
    if country:
        where.append("(UPPER(e.country)=@c OR UPPER(p.currency)=@c)"); params["c"] = country
    if venue:
        where.append("UPPER(COALESCE(e.venue, p.venue)) LIKE UPPER(@v)"); params["v"] = f"%{venue}%"
    if status:
        where.append("UPPER(COALESCE(p.status,'')) = @s"); params["s"] = status
    if len(date_range) == 2:
        where.append("substr(p.start_iso,1,10) BETWEEN @dfrom AND @dto")
        params["dfrom"] = date_range[0].isoformat()
        params["dto"] = date_range[1].isoformat()

    sql = (
        "SELECT p.product_id, p.event_id, p.event_name, COALESCE(e.venue, p.venue) AS venue, e.country, p.start_iso, "
        "COALESCE(p.status,'') AS status, p.currency, p.total_gross, p.total_net, p.rollover, "
        "(SELECT COUNT(1) FROM `tote_product_selections` s WHERE s.product_id = p.product_id) AS n_runners "
        "FROM `vw_products_latest_totals` p LEFT JOIN `tote_events` e USING(event_id) "
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY p.start_iso ASC LIMIT @lim"; params["lim"] = limit

    with st.spinner("Fetching Superfecta products..."):
        df = sql_df(sql, params=params)

    st.subheader("Products")
    st.dataframe(df, use_container_width=True, hide_index=True)

elif page == "Viability Calculator":
    st.header("Viability Calculator")

    # --- Product Selection ---
    st.subheader("1. Select Product")
    col1, col2, col3 = st.columns(3)
    with col1:
        bet_type = st.selectbox("Bet Type", ["SUPERFECTA", "EXACTA", "TRIFECTA", "WIN", "SWINGER"], index=0)
    with col2:
        date_filter = st.date_input("Date", value=date.today())
    with col3:
        country = st.selectbox("Country", ["GB", "IE"], index=0)

    # Fetch product options
    opts_sql = """
        SELECT p.product_id, p.event_name, p.start_iso, COALESCE(e.venue, p.venue) AS venue
        FROM `vw_products_latest_totals` p LEFT JOIN `tote_events` e USING(event_id)
        WHERE UPPER(p.bet_type)=@bt AND SUBSTR(p.start_iso, 1, 10) = @d AND (UPPER(e.country)=@c OR UPPER(p.currency)=@c)
        ORDER BY p.start_iso ASC
    """
    opts_params = {"bt": bet_type, "d": date_filter.isoformat(), "c": country}
    opts = sql_df(opts_sql, params=opts_params)
    
    product_options = {f"{row['event_name']} ({row['start_iso']})": row['product_id'] for _, row in opts.iterrows()}
    selected_product_display = st.selectbox("Select Product", options=list(product_options.keys()))

    if selected_product_display:
        product_id = product_options[selected_product_display]
        st.info(f"Selected Product ID: `{product_id}`")

        # --- Calculation Parameters ---
        st.subheader("2. Set Parameters")
        
        # Fetch product details for defaults
        prod_df = sql_df("SELECT * FROM vw_products_latest_totals WHERE product_id=@pid", params={"pid": product_id})
        prod = prod_df.iloc[0].to_dict() if not prod_df.empty else {}

        params_df = sql_df("""
            SELECT (SELECT COUNT(1) FROM `tote_product_selections` WHERE product_id=@pid AND leg_index=1) AS n,
                   (SELECT total_gross FROM `tote_pool_snapshots` WHERE product_id=@pid ORDER BY ts_ms DESC LIMIT 1) AS latest_gross
        """, params={"pid": product_id})
        
        N = int(params_df.iloc[0]["n"]) if not params_df.empty else 0
        pool_gross = float(params_df.iloc[0]["latest_gross"]) if not params_df.empty and pd.notna(params_df.iloc[0]["latest_gross"]) else float(prod.get("total_gross") or 0)

        col1, col2, col3 = st.columns(3)
        with col1:
            stake_per_line = st.number_input("Stake per Line (£)", value=0.10, format="%.2f")
            coverage_in_pct = st.slider("Coverage %", min_value=0.0, max_value=100.0, value=60.0, step=1.0)
        with col2:
            take_rate = st.number_input("Takeout Rate (%)", value=float(prod.get("deduction_rate", 0.30) * 100), format="%.2f") / 100.0
            net_rollover = st.number_input("Net Rollover (£)", value=float(prod.get("rollover", 0.0)), format="%.2f")
        with col3:
            f_fix = st.number_input("f-share Override (optional)", value=None, placeholder="e.g., 0.5")
            div_mult = st.number_input("Dividend Multiplier", value=1.0)
            inc_self = st.checkbox("Include Self in Pool", value=True)

        # --- Calculation ---
        st.subheader("3. Results")
        k_map = {"WIN": 1, "EXACTA": 2, "TRIFECTA": 3, "SUPERFECTA": 4, "SWINGER": 2}
        k_perm = int(k_map.get(bet_type, 4))
        coverage_in = coverage_in_pct / 100.0
        viab = None
        grid = []

        try:
            if bet_type == "SWINGER":
                C_all = int(N*(N-1)/2) if (N and N>=2) else 0
                M = int(round(max(0.0, min(1.0, coverage_in)) * C_all)) if C_all > 0 else 0
                # Use the correct BQ function for combinations
                viab_df = sql_df(
                    f"SELECT * FROM `{cfg.bq_project}.{cfg.bq_dataset}.tf_combo_viability_simple`(@N,@K,@O,@M,@l,@t,@R,@inc,@mult,@f)",
                    params={
                        "N": N, "K": 2, "O": pool_gross, "M": M, "l": stake_per_line, "t": take_rate,
                        "R": net_rollover, "inc": inc_self, "mult": div_mult, "f": f_fix,
                    },
                )
                if not viab_df.empty:
                    viab = viab_df.iloc[0].to_dict()
                else:
                    viab = None
                st.warning("Viability grid is not yet supported for SWINGER bets in this view.")
            else:
                C_all = 1
                if N and N >= k_perm:
                    for i in range(k_perm): C_all *= (N - i)
                M = int(round(max(0.0, min(1.0, coverage_in)) * C_all)) if C_all > 0 else 0

                # Use BigQuery function for grid
                grid_df = sql_df(
                    f"SELECT * FROM `{cfg.bq_project}.{cfg.bq_dataset}.tf_perm_viability_grid`(@N,@K,@O,@l,@t,@R,@inc,@mult,@f, @steps)",
                    params={"N": N, "K": k_perm, "O": pool_gross, "l": stake_per_line, "t": take_rate, "R": net_rollover, "inc": inc_self, "mult": div_mult, "f": f_fix, "steps": 20},
                )
                if not grid_df.empty:
                    grid = grid_df.to_dict("records")

                # Use local function for single point calculation
                viab = _viability_local_perm(N, k_perm, pool_gross, M, stake_per_line, take_rate, net_rollover, inc_self, div_mult, f_fix)

        except Exception as e:
            st.error(f"Viability calculation failed: {e}")
            traceback.print_exc()

        if viab:
            st.metric("Is Positive EV?", "✅ Yes" if viab.get('is_positive_ev') else "❌ No")
            kpi1, kpi2, kpi3 = st.columns(3)
            kpi1.metric("Expected Profit", f"£{viab.get('expected_profit', 0):,.2f}")
            kpi2.metric("Total Stake", f"£{viab.get('stake_total', 0):,.2f}")
            kpi3.metric("Lines Covered", f"{viab.get('lines_covered', 0):,}")
            
            with st.expander("Show Detailed Calculation Results"):
                st.json(viab)
        
        if grid:
            st.subheader("Viability Grid (by Coverage %)")
            grid_df_display = pd.DataFrame(grid)
            grid_df_display['coverage_frac'] = grid_df_display['coverage_frac'] * 100
            st.line_chart(grid_df_display.rename(columns={'coverage_frac': 'Coverage %', 'expected_profit': 'Expected Profit (£)'}).set_index('Coverage %')['Expected Profit (£)'])
            st.dataframe(grid_df_display, use_container_width=True, hide_index=True)

elif page == "Live Account":

    st.header("Live Account Bet History")
    st.markdown("View your bet history directly from your Tote account.")

    col1, col2 = st.columns(2)
    with col1:
        since_date = st.date_input("From Date", value=date.today() - pd.Timedelta(days=7))
    with col2:
        until_date = st.date_input("To Date", value=date.today())

    if st.button("Fetch Bets"):
        since_iso = pd.Timestamp(since_date, tz='UTC').isoformat()
        until_iso = (pd.Timestamp(until_date, tz='UTC') + pd.Timedelta(days=1)).isoformat()
        
        with st.spinner("Fetching live bet data from Tote API..."):
            live_bets_df = fetch_live_bets_from_api(since=since_iso, until=until_iso)
        
        if live_bets_df.empty:
            st.info("No bets found for the selected period.")
        else:
            st.dataframe(live_bets_df)
elif page == "Model Review":
    st.header("Superfecta Model Backtest")
    st.markdown("Review the performance of the Plackett-Luce model by running a historical backtest.")

    # Filters
    st.subheader("Backtest Parameters")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        start_date = st.date_input("Start Date", value=date(2024, 1, 1))
    with col2:
        end_date = st.date_input("End Date", value=date.today())
    with col3:
        top_n = st.number_input("Top N Runners", min_value=4, max_value=20, value=10)
    with col4:
        coverage = st.slider("Coverage", min_value=0.0, max_value=1.0, value=0.6, step=0.05)

    if st.button("Run Backtest"):
        tf = f"`{cfg.bq_project}.{cfg.bq_dataset}.tf_sf_backtest_horse_any`"
        with st.spinner("Running backtest query... this may take a moment."):
            rows_df = sql_df(
                f"SELECT * FROM {tf}(@start,@end,@top_n,@cov) ORDER BY start_iso",
                params={"start": start_date.isoformat(), "end": end_date.isoformat(), "top_n": top_n, "cov": coverage},
                cache_ttl=0,
            )
            sum_df = sql_df(
                f'''
                WITH r AS (SELECT * FROM {tf}(@start,@end,@top_n,@cov))
                SELECT
                  COUNT(*) AS races,
                  COUNTIF(hit_at_coverage) AS hits_at_cov,
                  SAFE_DIVIDE(COUNTIF(hit_at_coverage), NULLIF(COUNT(*),0)) AS hit_rate
                FROM r
                ''',
                params={"start": start_date.isoformat(), "end": end_date.isoformat(), "top_n": top_n, "cov": coverage},
                cache_ttl=0,
            )

        st.subheader("Summary")
        if not sum_df.empty:
            summary = sum_df.iloc[0]
            kpi1, kpi2, kpi3 = st.columns(3)
            kpi1.metric("Races Analyzed", f"{summary['races']:,}")
            kpi2.metric("Hits at Coverage", f"{summary['hits_at_cov']:,}")
            kpi3.metric("Hit Rate", f"{summary['hit_rate']:.2%}" if summary['hit_rate'] is not None else "N/A")

        st.subheader("Backtest Results")
        st.dataframe(rows_df)

elif page == "Model Deep Dive":
    st.header("Model Deep Dive")
    st.markdown("Analyze a specific race to see how the model's predictions compare to the actual results.")

    product_id = st.text_input("Enter Product ID to analyze (e.g., a Superfecta product)", "")

    if product_id:
        with st.spinner(f"Analyzing product {product_id}..."):
            # 1. Get product details and winning combination
            try:
                # Get winning combination from dividends table
                dividends_df = sql_df(
                    "SELECT selection, dividend FROM `tote_product_dividends` WHERE product_id = @pid AND dividend > 0 ORDER BY dividend DESC",
                    params={"pid": product_id}
                )
                
                # Get product details
                product_df = sql_df(
                    "SELECT event_name, start_iso, status FROM `tote_products` WHERE product_id = @pid",
                    params={"pid": product_id}
                )
                product_info = product_df.iloc[0] if not product_df.empty else None

            except Exception as e:
                st.error(f"Could not fetch data for product {product_id}: {e}")
                st.stop()

            if product_info is None:
                st.warning(f"Product {product_id} not found in the database.")
                st.stop()

            st.subheader(f"Race: {product_info['event_name']} ({product_info['start_iso']})")
            
            winning_selection = None
            if dividends_df.empty or product_info['status'] != 'RESULTED':
                st.info("This race has not been resulted yet, or no winning dividends were found.")
            else:
                st.subheader("Winning Combinations")
                st.dataframe(dividends_df)
                winning_selection = dividends_df.iloc[0]['selection']

            # 2. Get model's permutations for this race
            try:
                perms_df = sql_df(
                    f"SELECT * FROM `{cfg.bq_project}.{cfg.bq_dataset}.tf_superfecta_perms_horse_any`(@pid, @top_n)",
                    params={"pid": product_id, "top_n": 20}, cache_ttl=0,
                )
                perms_df['line'] = perms_df['h1'].astype(str) + '-' + perms_df['h2'].astype(str) + '-' + perms_df['h3'].astype(str) + '-' + perms_df['h4'].astype(str)
                perms_df = perms_df.sort_values("p", ascending=False).reset_index(drop=True)
                perms_df['rank'] = perms_df.index + 1
                st.subheader("Model's Top Predictions")
                st.dataframe(perms_df[['rank', 'line', 'p']].head(20))

                if winning_selection:
                    winning_line_in_model = perms_df[perms_df['line'] == winning_selection]
                    st.subheader("Analysis of Winning Combination")
                    if not winning_line_in_model.empty:
                        win_rank, win_prob = winning_line_in_model.iloc[0][['rank', 'p']]
                        st.success(f"The winning combination **{winning_selection}** was ranked **#{win_rank}** by the model with a probability of **{win_prob:.4%}**.")
                    else:
                        st.warning(f"The winning combination **{winning_selection}** was **not in the model's generated permutations**.")
            except Exception as e:
                st.warning(f"Could not generate model permutations. The model may not have data for this race. Error: {e}")