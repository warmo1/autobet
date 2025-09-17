CREATE VIEW `autobet-470818.autobet.vw_superfecta_runner_strength_horse`
AS WITH any_s AS (
  SELECT product_id, event_id, runner_id, strength
  FROM `autobet-470818.autobet.vw_superfecta_runner_strength_any`
),
mapped AS (
  SELECT
    s.product_id,
    s.event_id,
    COALESCE(m.horse_id, s.runner_id) AS horse_id,
    s.strength
  FROM any_s s
  LEFT JOIN `autobet-470818.autobet.vw_sf_selection_to_horse` m
    ON m.product_id = s.product_id
   AND m.selection_id = s.runner_id
)
SELECT
  product_id,
  event_id,
  CAST(horse_id AS STRING) AS runner_id,  -- now runner_id = horse_id everywhere
  strength
FROM mapped
WHERE horse_id IS NOT NULL;
