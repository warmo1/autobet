CREATE TABLE `autobet-470818.autobet.features_runner_event`
(
  event_id STRING,
  horse_id STRING,
  event_date STRING,
  cloth_number INT64,
  total_net FLOAT64,
  weather_temp_c FLOAT64,
  weather_wind_kph FLOAT64,
  weather_precip_mm FLOAT64,
  going STRING,
  recent_runs INT64,
  avg_finish FLOAT64,
  wins_last5 INT64,
  places_last5 INT64,
  days_since_last_run INT64
);
