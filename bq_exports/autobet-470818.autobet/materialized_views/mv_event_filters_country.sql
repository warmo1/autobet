CREATE MATERIALIZED VIEW `autobet-470818.autobet.mv_event_filters_country`
OPTIONS(
  refresh_interval_minutes=60.0
)
AS SELECT country
FROM `autobet-470818.autobet.tote_events`
WHERE country IS NOT NULL AND country <> ''
GROUP BY country;
