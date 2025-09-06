-- Selection counts per product
SELECT
  p.product_id,
  p.event_id,
  p.bet_type,
  COUNT(s.selection_id) AS n_selections
FROM `${BQ_PROJECT}.${BQ_DATASET}.tote_products` p
LEFT JOIN `${BQ_PROJECT}.${BQ_DATASET}.tote_product_selections` s
  ON s.product_id = p.product_id
WHERE DATE(SUBSTR(p.start_iso,1,10)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
GROUP BY 1,2,3
ORDER BY p.start_iso DESC
LIMIT 200;

