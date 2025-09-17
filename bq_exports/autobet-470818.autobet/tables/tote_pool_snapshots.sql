CREATE TABLE `autobet-470818.autobet.tote_pool_snapshots`
(
  product_id STRING,
  event_id STRING,
  bet_type STRING,
  status STRING,
  currency STRING,
  start_iso STRING,
  ts_ms INT64,
  total_gross FLOAT64,
  total_net FLOAT64,
  rollover FLOAT64,
  deduction_rate FLOAT64
);
