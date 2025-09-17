CREATE MATERIALIZED VIEW `autobet-470818.autobet.mv_event_filters_venue`
OPTIONS(
  refresh_interval_minutes=60.0
)
AS SELECT venue
FROM `autobet-470818.autobet.tote_events`
WHERE venue IS NOT NULL AND venue <> ''
GROUP BY venue;
