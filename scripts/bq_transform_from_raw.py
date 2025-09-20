from __future__ import annotations

"""
Transform Tote GraphQL raw JSON (raw_tote) into structured BigQuery tables
matching this repo's schema so existing views continue to work.

Creates/merges into:
- tote_events(event_id, name, sport, venue, country, start_iso, status, source)
- tote_products(product_id, bet_type, status, currency, total_gross, total_net,
                event_id, event_name, venue, start_iso, source)
- tote_product_selections(product_id, leg_index, selection_id, competitor, number,
                          leg_event_id, leg_event_name, leg_venue, leg_start_iso)

Usage:
  python scripts/bq_transform_from_raw.py \
    --project autobet-470818 --dataset autobet --date 2025-09-05

Notes:
- Assumes raw_tote.payload contains a JSON like products.edges[].node{...}
- Safe-casts and ignores rows where fields are missing.
"""

import argparse
from google.cloud import bigquery
from pathlib import Path
from sports.config import cfg
import sys

# Ensure repo root (containing 'sports') is on sys.path when run as a script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


DDL_TEMPLATE = """
-- Ensure tables exist with the expected columns
CREATE TABLE IF NOT EXISTS `{ds}.tote_events`(
  event_id STRING,
  name STRING,
  sport STRING,
  venue STRING,
  country STRING,
  start_iso STRING,
  status STRING,
  result_status STRING,
  comp STRING,
  home STRING,
  away STRING,
  competitors_json STRING,
  source STRING
);

CREATE TABLE IF NOT EXISTS `{ds}.tote_products`(
  product_id STRING,
  bet_type STRING,
  status STRING,
  currency STRING,
  total_gross FLOAT64,
  total_net FLOAT64,
  event_id STRING,
  event_name STRING,
  venue STRING,
  start_iso STRING,
  source STRING,
  rollover FLOAT64,
  deduction_rate FLOAT64
);

CREATE TABLE IF NOT EXISTS `{ds}.tote_product_selections`(
  product_id STRING,
  leg_index INT64,
  selection_id STRING,
  product_leg_id STRING,
  competitor STRING,
  number INT64,
  leg_event_id STRING,
  leg_event_name STRING,
  leg_venue STRING,
  leg_start_iso STRING
);
"""


MERGE_EVENTS = """
DECLARE d DATE DEFAULT @date;
MERGE `{ds}.tote_events` T
USING (
  WITH edges AS (
    SELECT edge
    FROM `{ds}.raw_tote` r, UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.edges')) AS edge
    WHERE DATE(r.fetched_ts) = d
  ), leg_nodes AS (
    SELECT edge, leg
    FROM edges, UNNEST(JSON_EXTRACT_ARRAY(edge, '$.node.legs.nodes')) AS leg
  ), items AS (
    SELECT
      JSON_EXTRACT_SCALAR(leg, '$.event.id') AS event_id,
      JSON_EXTRACT_SCALAR(leg, '$.event.name') AS name,
      'horse_racing' AS sport,
      JSON_EXTRACT_SCALAR(leg, '$.event.venue.name') AS venue,
      JSON_EXTRACT_SCALAR(edge, '$.node.country.alpha2Code') AS country,
      JSON_EXTRACT_SCALAR(leg, '$.event.scheduledStartDateTime.iso8601') AS start_iso,
      JSON_EXTRACT_SCALAR(edge, '$.node.selling.status') AS status,
      'tote_graphql' AS source
    FROM leg_nodes
    GROUP BY event_id, name, venue, country, start_iso, status
  )
  SELECT * FROM items WHERE event_id IS NOT NULL
) S
ON T.event_id = S.event_id
WHEN MATCHED THEN UPDATE SET
  T.name=S.name, T.sport=S.sport, T.venue=S.venue, T.country=S.country,
  T.start_iso=S.start_iso, T.status=S.status, T.source=S.source
WHEN NOT MATCHED THEN INSERT (event_id,name,sport,venue,country,start_iso,status,source)
VALUES (S.event_id,S.name,S.sport,S.venue,S.country,S.start_iso,S.status,S.source);
"""


MERGE_PRODUCTS = """
DECLARE d DATE DEFAULT @date;
MERGE `{ds}.tote_products` T
USING (
  WITH edges AS (
    SELECT edge
    FROM `{ds}.raw_tote` r, UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.edges')) AS edge
    WHERE DATE(r.fetched_ts) = d
  ), leg_nodes AS (
    SELECT edge, leg
    FROM edges, UNNEST(JSON_EXTRACT_ARRAY(edge, '$.node.legs.nodes')) AS leg
  ), items AS (
    SELECT
      JSON_EXTRACT_SCALAR(edge, '$.node.id') AS product_id,
      UPPER(JSON_EXTRACT_SCALAR(edge, '$.node.betType.code')) AS bet_type,
      JSON_EXTRACT_SCALAR(edge, '$.node.selling.status') AS status,
      JSON_EXTRACT_SCALAR(edge, '$.node.country.alpha2Code') AS currency,
      COALESCE(
        SAFE_CAST(JSON_EXTRACT_SCALAR(edge, '$.node.pool.total.grossAmount.decimalAmount') AS FLOAT64),
        (SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(f, '$.total.grossAmount.decimalAmount') AS FLOAT64))
         FROM UNNEST(JSON_EXTRACT_ARRAY(edge, '$.node.pool.funds')) AS f)
      ) AS total_gross,
      COALESCE(
        SAFE_CAST(JSON_EXTRACT_SCALAR(edge, '$.node.pool.total.netAmount.decimalAmount') AS FLOAT64),
        (SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(f, '$.total.netAmount.decimalAmount') AS FLOAT64))
         FROM UNNEST(JSON_EXTRACT_ARRAY(edge, '$.node.pool.funds')) AS f)
      ) AS total_net,
      COALESCE(
        SAFE_CAST(JSON_EXTRACT_SCALAR(edge, '$.node.pool.carryIn.grossAmount.decimalAmount') AS FLOAT64),
        (SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(f, '$.carryIn.grossAmount.decimalAmount') AS FLOAT64))
         FROM UNNEST(JSON_EXTRACT_ARRAY(edge, '$.node.pool.funds')) AS f)
      ) AS rollover,
      SAFE_CAST(JSON_EXTRACT_SCALAR(edge, '$.node.pool.takeout.percentage') AS FLOAT64) AS deduction_rate,
      JSON_EXTRACT_SCALAR(leg, '$.event.id') AS event_id,
      JSON_EXTRACT_SCALAR(leg, '$.event.name') AS event_name,
      JSON_EXTRACT_SCALAR(leg, '$.event.venue.name') AS venue,
      JSON_EXTRACT_SCALAR(leg, '$.event.scheduledStartDateTime.iso8601') AS start_iso,
      'tote_graphql' AS source
    FROM leg_nodes
  )
  SELECT * FROM items WHERE product_id IS NOT NULL
) S
ON T.product_id = S.product_id
WHEN MATCHED THEN UPDATE SET
  T.bet_type=S.bet_type, T.status=S.status, T.currency=S.currency, T.total_gross=S.total_gross,
  T.total_net=S.total_net, T.rollover=S.rollover, T.deduction_rate=S.deduction_rate,
  T.event_id=S.event_id, T.event_name=S.event_name, T.venue=S.venue,
  T.start_iso=S.start_iso, T.source=S.source
WHEN NOT MATCHED THEN INSERT (product_id,bet_type,status,currency,total_gross,total_net,rollover,deduction_rate,event_id,event_name,venue,start_iso,source)
VALUES (S.product_id,S.bet_type,S.status,S.currency,S.total_gross,S.total_net,S.rollover,S.deduction_rate,S.event_id,S.event_name,S.venue,S.start_iso,S.source);
"""


MERGE_SELECTIONS = """
DECLARE d DATE DEFAULT @date;
MERGE `{ds}.tote_product_selections` T
USING (
  WITH edges AS (
    SELECT edge
    FROM `{ds}.raw_tote` r, UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.edges')) AS edge
    WHERE DATE(r.fetched_ts) = d
  ), leg_nodes AS (
    SELECT edge, leg
    FROM edges, UNNEST(JSON_EXTRACT_ARRAY(edge, '$.node.legs.nodes')) AS leg
  ), sel_nodes AS (
    SELECT edge, leg, sel
    FROM leg_nodes, UNNEST(JSON_EXTRACT_ARRAY(leg, '$.selections.nodes')) AS sel
  ), items AS (
    SELECT
      JSON_EXTRACT_SCALAR(edge, '$.node.id') AS product_id,
      1 AS leg_index,
      JSON_EXTRACT_SCALAR(sel, '$.id') AS selection_id,
      JSON_EXTRACT_SCALAR(sel, '$.competitor.name') AS competitor,
      COALESCE(
        SAFE_CAST(JSON_EXTRACT_SCALAR(sel, '$.competitor.details.clothNumber') AS INT64),
        SAFE_CAST(JSON_EXTRACT_SCALAR(sel, '$.competitor.details.trapNumber') AS INT64)
      ) AS number,
      JSON_EXTRACT_SCALAR(leg, '$.event.id') AS leg_event_id,
      JSON_EXTRACT_SCALAR(leg, '$.event.name') AS leg_event_name,
      JSON_EXTRACT_SCALAR(leg, '$.event.venue.name') AS leg_venue,
      JSON_EXTRACT_SCALAR(leg, '$.event.scheduledStartDateTime.iso8601') AS leg_start_iso
    FROM sel_nodes
  )
  SELECT * FROM items WHERE product_id IS NOT NULL AND selection_id IS NOT NULL
) S
ON T.product_id=S.product_id AND T.leg_index=S.leg_index AND T.selection_id=S.selection_id
WHEN MATCHED THEN UPDATE SET
  T.competitor=S.competitor, T.number=S.number,
  T.leg_event_id=S.leg_event_id, T.leg_event_name=S.leg_event_name,
  T.leg_venue=S.leg_venue, T.leg_start_iso=S.leg_start_iso
WHEN NOT MATCHED THEN INSERT (product_id,leg_index,selection_id,competitor,number,leg_event_id,leg_event_name,leg_venue,leg_start_iso)
VALUES (S.product_id,S.leg_index,S.selection_id,S.competitor,S.number,S.leg_event_id,S.leg_event_name,S.leg_venue,S.leg_start_iso);
"""


def main() -> None:
    ap = argparse.ArgumentParser(description="Transform raw_tote JSON to structured BigQuery tables")
    ap.add_argument("--project", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (process raw_tote for this date)")
    args = ap.parse_args()

    client = bigquery.Client(project=args.project)
    ds = f"{args.project}.{args.dataset}"

    # DDL (idempotent)
    client.query(DDL_TEMPLATE.format(ds=ds)).result()

    # MERGEs
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("date", "DATE", args.date,
        location=cfg.bq_location
    )])
    for sql in (MERGE_EVENTS, MERGE_PRODUCTS, MERGE_SELECTIONS):
        client.query(sql.format(ds=ds), job_config=job_config).result()

    print("Transform complete for", args.date)


if __name__ == "__main__":
    main()
