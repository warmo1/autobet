CREATE VIEW `autobet-470818.autobet.vw_sf_selection_to_horse`
AS WITH sel AS (
  SELECT
    s.product_id,
    p.event_id,
    s.selection_id,
    SAFE_CAST(s.number AS INT64) AS cloth_number,
    UPPER(REGEXP_REPLACE(COALESCE(s.competitor, ''), r'[^A-Z0-9]', '')) AS competitor_name_norm
  FROM `autobet-470818.autobet.tote_product_selections` s
  JOIN `autobet-470818.autobet.tote_products` p USING(product_id)
  WHERE s.leg_index = 1
),
cloth AS (
  SELECT
    s.product_id, s.event_id, s.selection_id, s.cloth_number,
    m.horse_id, 1 AS match_rank
  FROM sel s
  JOIN `autobet-470818.autobet.vw_event_runner_map` m
    ON m.event_id = s.event_id AND m.cloth_number = s.cloth_number
  WHERE s.cloth_number IS NOT NULL
),
by_name AS (
  SELECT
    s.product_id, s.event_id, s.selection_id, s.cloth_number,
    m.horse_id, 2 AS match_rank
  FROM sel s
  JOIN `autobet-470818.autobet.vw_event_runner_map` m
    ON m.event_id = s.event_id
   AND m.horse_name_norm = s.competitor_name_norm
),
unioned AS (
  SELECT * FROM cloth
  UNION ALL
  SELECT * FROM by_name
)
SELECT
  product_id, event_id, selection_id, cloth_number, horse_id
FROM unioned
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY product_id, selection_id
  ORDER BY match_rank
) = 1;
