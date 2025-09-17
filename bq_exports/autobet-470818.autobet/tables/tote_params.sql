CREATE TABLE `autobet-470818.autobet.tote_params`
(
  t FLOAT64,
  f FLOAT64,
  stake_per_line FLOAT64,
  R FLOAT64,
  updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  model_id STRING,
  ts_ms INT64,
  default_top_n INT64,
  target_coverage FLOAT64
);
