CREATE MATERIALIZED VIEW `autobet-470818.autobet.mv_event_filters_sport`
OPTIONS(
  refresh_interval_minutes=60.0
)
AS SELECT sport
FROM `autobet-470818.autobet.tote_events`
WHERE sport IS NOT NULL AND sport <> ''
GROUP BY sport;
