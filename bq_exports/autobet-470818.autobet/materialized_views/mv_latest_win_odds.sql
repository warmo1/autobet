CREATE MATERIALIZED VIEW `autobet-470818.autobet.mv_latest_win_odds`
OPTIONS(
  refresh_interval_minutes=720.0,
  allow_non_incremental_definition=true,
  max_staleness=INTERVAL '0-0 0 12:0:0' YEAR TO SECOND
)
AS SELECT
  product_id,
  selection_id,
  (ARRAY_AGG(STRUCT(decimal_odds, ts) ORDER BY ts DESC LIMIT 1))[OFFSET(0)].decimal_odds AS decimal_odds,
  MAX(ts) AS latest_ts,
  DATE(MAX(ts)) AS latest_date
FROM (
  SELECT
    SAFE_CAST(JSON_EXTRACT_SCALAR(prod, '$.id') AS STRING) AS product_id,
    COALESCE(
      JSON_EXTRACT_SCALAR(line, '$.legs.lineSelections[0].selectionId'),
      JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(line, '$.legs')[SAFE_OFFSET(0)], '$.lineSelections[0].selectionId')
    ) AS selection_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(line, '$.odds.decimal') AS FLOAT64) AS decimal_odds,
    TIMESTAMP_MILLIS(r.fetched_ts) AS ts
  FROM `autobet-470818.autobet.raw_tote_probable_odds` r,
  UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod,
  -- tolerate both shapes: lines.nodes[] OR lines[]
  UNNEST(IFNULL(JSON_EXTRACT_ARRAY(prod, '$.lines.nodes'),
                JSON_EXTRACT_ARRAY(prod, '$.lines'))) AS line
  WHERE JSON_EXTRACT_SCALAR(line, '$.odds.decimal') IS NOT NULL
)
WHERE selection_id IS NOT NULL
  AND decimal_odds IS NOT NULL AND decimal_odds > 0
  AND product_id IS NOT NULL
GROUP BY product_id, selection_id;
