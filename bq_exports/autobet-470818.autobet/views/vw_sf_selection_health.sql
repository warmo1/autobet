CREATE VIEW `autobet-470818.autobet.vw_sf_selection_health`
AS WITH base AS (
  SELECT
    s.product_id,
    COUNTIF(s.product_leg_id IS NULL) AS n_missing_leg_id,
    COUNTIF(s.number IS NULL) AS n_missing_number
  FROM `autobet-470818.autobet.tote_product_selections` s
  GROUP BY s.product_id
),
dups AS (
  SELECT product_id, cloth_or_trap, COUNT(*) AS n
  FROM `autobet-470818.autobet.vw_sf_selection_map`
  GROUP BY product_id, cloth_or_trap
  HAVING COUNT(*) > 1
),
no_sel AS (
  SELECT p.product_id
  FROM `autobet-470818.autobet.tote_products` p
  LEFT JOIN `autobet-470818.autobet.tote_product_selections` s ON p.product_id = s.product_id
  WHERE UPPER(p.bet_type)='SUPERFECTA'
  GROUP BY p.product_id
  HAVING COUNT(s.selection_id) = 0
)
SELECT
  p.product_id,
  p.event_id,
  p.event_name,
  COALESCE(e.venue, p.venue) AS venue,
  p.start_iso,
  COALESCE(b.n_missing_leg_id,0) AS n_missing_leg_id,
  COALESCE(b.n_missing_number,0) AS n_missing_number,
  COUNT(d.product_id) AS n_dup_numbers,
  CASE WHEN p.product_id IN (SELECT product_id FROM no_sel) THEN 1 ELSE 0 END AS no_selections
FROM `autobet-470818.autobet.tote_products` p
LEFT JOIN base b ON b.product_id = p.product_id
LEFT JOIN dups d ON d.product_id = p.product_id
LEFT JOIN `autobet-470818.autobet.tote_events` e ON e.event_id = p.event_id
WHERE UPPER(p.bet_type)='SUPERFECTA'
GROUP BY p.product_id, p.event_id, p.event_name, venue, p.start_iso, n_missing_leg_id, n_missing_number, no_selections;
