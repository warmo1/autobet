-- Products loaded for today (by event start date)
SELECT
  product_id,
  event_id,
  bet_type,
  status,
  currency,
  start_iso,
  total_gross,
  total_net
FROM `${BQ_PROJECT}.${BQ_DATASET}.tote_products`
WHERE DATE(SUBSTR(start_iso,1,10)) = CURRENT_DATE()
ORDER BY start_iso
LIMIT 200;

