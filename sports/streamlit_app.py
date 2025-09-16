import streamlit as st
import pandas as pd
from datetime import date

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
            query GetCustomer {
              customer {
                account {
                  balance {
                    currency { code }
                    decimalAmount
                  }
                }
              }
            }
        """
        data = client.graphql(query, keep_auth=True)
        balance_data = data.get("customer", {}).get("account", {}).get("balance", {})
        if balance_data:
            amount = float(balance_data.get("decimalAmount", 0.0))
            currency = balance_data.get("currency", {}).get("code", "GBP")
            return f"{currency} {amount:,.2f}", None
        return None, "Could not parse balance from API response."
    except ToteError as e:
        return None, f"Tote API Error: {e}"
    except Exception as e:
        return None, f"An unexpected error occurred: {e}"

@st.cache_data(ttl=300)
def load_bets():
    """
    Loads and processes bet data from BigQuery.
    - Fetches all audit bets.
    - Fetches dividend data.
    - Explodes multi-line bets into individual lines.
    - Calculates stake per line.
    - Joins with dividends to determine wins and calculate P&L.
    """
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
          p.status as product_status
        FROM `tote_audit_bets` b
        LEFT JOIN `tote_products` p ON b.product_id = p.product_id
    """
    bets_df = sql_df(bets_query, cache_ttl=300)

    dividends_query = "SELECT product_id, selection, dividend FROM `tote_product_dividends`"
    dividends_df = sql_df(dividends_query, cache_ttl=300)

    if bets_df.empty:
        return pd.DataFrame()

    # Explode multi-line bets (where 'selection' is comma-separated)
    bets_df['lines'] = bets_df['selection'].str.split(',')
    bets_df = bets_df.explode('lines')
    bets_df.rename(columns={'lines': 'line_selection'}, inplace=True)
    bets_df['line_selection'] = bets_df['line_selection'].str.strip()

    # Calculate stake per line
    bets_df['num_lines'] = bets_df.groupby('bet_id')['bet_id'].transform('count')
    bets_df['stake_per_line'] = bets_df['stake'] / bets_df['num_lines']

    # Merge with dividends to find winning lines
    merged_df = pd.merge(
        bets_df,
        dividends_df,
        how='left',
        left_on=['product_id', 'line_selection'],
        right_on=['product_id', 'selection']
    )

    # Calculate P&L for resulted bets
    merged_df['is_win'] = merged_df['dividend'].notna() & (merged_df['dividend'] > 0)
    merged_df['winnings'] = 0.0
    # Tote dividends are typically for a £1 stake
    merged_df.loc[merged_df['is_win'], 'winnings'] = merged_df['stake_per_line'] * merged_df['dividend']
    merged_df['pnl'] = merged_df['winnings'] - merged_df['stake_per_line']
    
    # Only count P&L for settled/resulted products
    merged_df.loc[merged_df['product_status'] != 'RESULTED', 'pnl'] = 0
    merged_df.loc[merged_df['product_status'] != 'RESULTED', 'winnings'] = 0
    merged_df.loc[merged_df['product_status'] != 'RESULTED', 'is_win'] = pd.NA

    return merged_df

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
                  status
                  stake { decimalAmount currency { code } }
                  payout { decimalAmount }
                  settledTimeUTC
                  betType { code }
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
            stake = bet.get("stake", {})
            legs = (bet.get("legs", {}).get("nodes", []) or [{}])[0]
            event = legs.get("event", {})
            selections = (legs.get("selections", {}).get("nodes", []) or [])
            
            flat_bet = {
                "bet_id": bet.get("id"), "tote_id": bet.get("toteId"), "status": bet.get("status"),
                "bet_type": (bet.get("betType") or {}).get("code"), "stake": stake.get("decimalAmount"),
                "currency": (stake.get("currency") or {}).get("code"), "payout": (bet.get("payout") or {}).get("decimalAmount"),
                "settled_time": bet.get("settledTimeUTC"), "event_name": event.get("name"),
                "event_start": (event.get("scheduledStartDateTime") or {}).get("iso8601"),
                "selections": ", ".join([s.get("name", "") for s in selections]),
            }
            flat_bets.append(flat_bet)
            
        return pd.DataFrame(flat_bets)

    except ToteError as e:
        st.error(f"Tote API Error: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return pd.DataFrame()

# --- Sidebar ---
st.sidebar.header("Navigation")
page = st.sidebar.radio("Go to", ["Overview", "Bet Performance", "Live Account", "Model Review", "Model Deep Dive"])


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
        resulted_bets_df['ts'] = pd.to_datetime(resulted_bets_df['ts_ms'], unit='ms')
        daily_pnl = resulted_bets_df.set_index('ts').resample('D')['pnl'].sum().cumsum()
        bankroll_over_time = cfg.paper_starting_bankroll + daily_pnl
        st.line_chart(bankroll_over_time)

elif page == "Bet Performance":
    st.header("Bet Performance Details")
    with st.spinner("Loading bet performance data..."):
        bets_df = load_bets()

    if bets_df.empty:
        st.warning("No bet data found.")
    else:
        st.dataframe(bets_df)

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