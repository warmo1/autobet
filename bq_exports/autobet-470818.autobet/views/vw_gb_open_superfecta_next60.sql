CREATE VIEW `autobet-470818.autobet.vw_gb_open_superfecta_next60`
AS SELECT
  p.product_id,
  p.event_id,
  p.event_name,
  COALESCE(e.venue, p.venue) AS venue,
  e.country,
  p.start_iso,
  p.status,
  p.currency,
  p.total_gross,
  p.total_net,
  p.rollover,
  ARRAY_LENGTH(JSON_EXTRACT_ARRAY(e.competitors_json, '$')) AS n_competitors
FROM `autobet-470818.autobet.tote_products` p
LEFT JOIN `autobet-470818.autobet.tote_events` e ON e.event_id = p.event_id
WHERE UPPER(p.bet_type)='SUPERFECTA'
  AND e.country='GB'
  AND p.status='OPEN'
  AND TIMESTAMP(p.start_iso) BETWEEN CURRENT_TIMESTAMP() AND TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE);
