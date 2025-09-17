CREATE VIEW `autobet-470818.autobet.vw_runner_features`
AS SELECT
  f.event_id,
  f.horse_id,
  h.name AS horse_name,
  f.event_date,
  f.cloth_number,
  f.total_net,
  f.weather_temp_c,
  f.weather_wind_kph,
  f.weather_precip_mm,
  f.going,
  f.recent_runs,
  f.avg_finish,
  f.wins_last5,
  f.places_last5,
  f.days_since_last_run
FROM `autobet-470818.autobet.features_runner_event` f
LEFT JOIN `autobet-470818.autobet.hr_horses` h
  ON h.horse_id = f.horse_id;
