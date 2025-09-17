CREATE TABLE `autobet-470818.autobet.ingest_job_runs`
(
  job_id STRING,
  component STRING,
  task STRING,
  status STRING,
  started_ts INT64,
  ended_ts INT64,
  duration_ms INT64,
  payload_json STRING,
  error STRING,
  metrics_json STRING
);
