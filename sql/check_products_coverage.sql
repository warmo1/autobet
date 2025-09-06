-- Coverage overview per product (selections/events presence)
SELECT
  product_id,
  event_id,
  bet_type,
  status,
  currency,
  start_iso,
  event_name,
  venue,
  n_selections,
  event_present,
  no_selections,
  missing_event,
  missing_start_iso
FROM `${BQ_PROJECT}.${BQ_DATASET}.vw_products_coverage`
ORDER BY start_iso DESC
LIMIT 500;

