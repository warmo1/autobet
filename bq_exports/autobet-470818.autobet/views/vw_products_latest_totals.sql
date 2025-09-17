CREATE VIEW `autobet-470818.autobet.vw_products_latest_totals`
AS WITH latest AS (
  SELECT
    product_id,
    ARRAY_AGG(total_gross ORDER BY ts_ms DESC LIMIT 1)[OFFSET(0)] AS latest_gross,
    ARRAY_AGG(total_net ORDER BY ts_ms DESC LIMIT 1)[OFFSET(0)] AS latest_net,
    MAX(ts_ms) AS ts_ms
  FROM `autobet-470818.autobet.tote_pool_snapshots`
  GROUP BY product_id
), pstat AS (
  SELECT
    product_id,
    ARRAY_AGG(status ORDER BY ts_ms DESC LIMIT 1)[OFFSET(0)] AS latest_status
  FROM `autobet-470818.autobet.tote_product_status_log`
  GROUP BY product_id
)
SELECT
  p.product_id,
  p.event_id,
  p.event_name,
  p.venue,
  p.start_iso,
  COALESCE(pstat.latest_status, p.status) AS status,
  p.currency,
  COALESCE(l.latest_gross, p.total_gross) AS total_gross,
  COALESCE(l.latest_net, p.total_net) AS total_net,
  p.rollover,
  p.deduction_rate,
  p.bet_type
FROM `autobet-470818.autobet.tote_products` p
LEFT JOIN latest l USING(product_id)
LEFT JOIN pstat USING(product_id);
