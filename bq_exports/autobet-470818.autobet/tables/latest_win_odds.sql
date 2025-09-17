CREATE TABLE `autobet-470818.autobet.latest_win_odds`
(
  product_id STRING,
  selection_id STRING,
  decimal_odds FLOAT64,
  latest_ts TIMESTAMP,
  latest_date DATE
)
PARTITION BY latest_date
CLUSTER BY product_id, selection_id;
