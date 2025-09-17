CREATE TABLE `autobet-470818.autobet.predictions`
(
  model_id STRING,
  ts_ms INT64,
  event_id STRING,
  horse_id STRING,
  market STRING,
  proba FLOAT64,
  rank INT64
);
