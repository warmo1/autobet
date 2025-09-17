CREATE VIEW `autobet-470818.autobet.vw_sf_products_with_selections`
AS SELECT
  p.product_id,
  p.event_id,
  p.event_name,
  COALESCE(e.venue, p.venue) AS venue,
  p.start_iso,
  COUNT(s.selection_id) AS n_selections
FROM `autobet-470818.autobet.tote_products` p
JOIN `autobet-470818.autobet.tote_product_selections` s USING (product_id)
LEFT JOIN `autobet-470818.autobet.tote_events` e USING (event_id)
WHERE UPPER(p.bet_type)='SUPERFECTA'
GROUP BY p.product_id, p.event_id, p.event_name, venue, p.start_iso;
