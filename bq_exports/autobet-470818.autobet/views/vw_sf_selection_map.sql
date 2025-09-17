CREATE VIEW `autobet-470818.autobet.vw_sf_selection_map`
AS SELECT
  s.product_id,
  s.product_leg_id,
  s.number AS cloth_or_trap,
  s.selection_id,
  s.competitor,
  p.event_id,
  COALESCE(e.venue, p.venue) AS venue,
  p.start_iso,
  s.units_ts_ms,
  s.total_units
FROM `autobet-470818.autobet.tote_product_selections` s
LEFT JOIN `autobet-470818.autobet.tote_products` p USING (product_id)
LEFT JOIN `autobet-470818.autobet.tote_events` e USING (event_id);
