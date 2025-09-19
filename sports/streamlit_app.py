from __future__ import annotations

import pandas as pd
import streamlit as st
from datetime import date
from typing import Optional

from autobet.sports.config import cfg
from autobet.sports.webapp import sql_df

st.set_page_config(page_title="Superfecta Automation", layout="wide")
st.title("Superfecta Automation Reporting")

DATASET = f"{cfg.bq_project}.{cfg.bq_dataset}"


@st.cache_data(ttl=60)
def load_morning(run_date: str) -> pd.DataFrame:
    return sql_df(
        f"SELECT * FROM `{DATASET}.tote_superfecta_morning` WHERE run_date = @run_date ORDER BY start_iso",
        params={"run_date": run_date},
        cache_ttl=30,
    )


@st.cache_data(ttl=60)
def load_recommendations(run_date: str) -> pd.DataFrame:
    return sql_df(
        f"SELECT r.*, p.event_name, p.start_iso FROM `{DATASET}.tote_superfecta_recommendations` r "
        f"LEFT JOIN `{DATASET}.tote_products` p USING (product_id) "
        "WHERE r.run_date = @run_date ORDER BY p.start_iso",
        params={"run_date": run_date},
        cache_ttl=30,
    )


@st.cache_data(ttl=60)
def load_live_checks(run_date: str, product_id: Optional[str]) -> pd.DataFrame:
    where = "DATE(TIMESTAMP_MILLIS(check_ts)) = @run_date"
    params = {"run_date": run_date}
    if product_id:
        where += " AND product_id = @product_id"
        params["product_id"] = product_id
    return sql_df(
        f"SELECT * FROM `{DATASET}.tote_superfecta_live_checks` WHERE {where} ORDER BY check_ts DESC",
        params=params,
        cache_ttl=30,
    )


@st.cache_data(ttl=60)
def load_bets(run_date: str, product_ids: list[str]) -> pd.DataFrame:
    if not product_ids:
        return pd.DataFrame()
    return sql_df(
        f"SELECT bet_id, ts_ms, status AS placement_status, stake, currency FROM `{DATASET}.tote_audit_bets` "
        "WHERE DATE(TIMESTAMP_MILLIS(ts_ms)) = @run_date AND product_id IN UNNEST(@product_ids) "
        "ORDER BY ts_ms DESC",
        params={"run_date": run_date, "product_ids": product_ids},
        cache_ttl=30,
    )


def main() -> None:
    selected_date = st.sidebar.date_input("Race date", value=date.today())
    date_str = selected_date.isoformat()

    morning_df = load_morning(date_str)
    recs_df = load_recommendations(date_str)

    product_options = ["(All)"]
    if not recs_df.empty:
        product_options.extend(recs_df["product_id"].dropna().astype(str).unique().tolist())
    selected_option = st.sidebar.selectbox("Recommendation", product_options)
    selected_product = None if selected_option == "(All)" else selected_option

    live_df = load_live_checks(date_str, selected_product)
    product_ids = [] if recs_df.empty else recs_df["product_id"].dropna().astype(str).unique().tolist()
    bet_df = load_bets(date_str, product_ids)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Recommendations", int(0 if recs_df.empty else len(recs_df)))
    ready_count = int(0 if recs_df.empty else (recs_df["status"].str.lower() == "ready").sum())
    placed_count = int(0 if recs_df.empty else (recs_df["status"].str.lower() == "placed").sum())
    col2.metric("Ready", ready_count)
    col3.metric("Placed", placed_count)
    col4.metric("Live Checks", int(0 if live_df.empty else len(live_df)))

    st.subheader("Morning Snapshot")
    if morning_df.empty:
        st.info("No morning recommendations recorded for this date.")
    else:
        st.dataframe(morning_df)

    st.subheader("Recommendations")
    if recs_df.empty:
        st.info("No recommendations for this date.")
    else:
        st.dataframe(recs_df)

    st.subheader("Live Checks")
    if live_df.empty:
        st.info("No live evaluation results yet.")
    else:
        st.dataframe(live_df)

    st.subheader("Audit Bets")
    if bet_df.empty:
        st.info("No audit bets recorded for the selected date.")
    else:
        st.dataframe(bet_df)


if __name__ == "__main__":
    main()
