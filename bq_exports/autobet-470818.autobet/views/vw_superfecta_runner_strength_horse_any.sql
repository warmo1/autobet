CREATE VIEW `autobet-470818.autobet.vw_superfecta_runner_strength_horse_any`
AS WITH pred AS (
  SELECT
    tp.product_id,
    p.event_id,
    CAST(p.horse_id AS STRING) AS runner_id,
    GREATEST(p.proba, 1e-9) AS strength
  FROM `autobet-470818.autobet.predictions` p
  JOIN `autobet-470818.autobet.tote_products` tp
    ON tp.event_id = p.event_id
  WHERE UPPER(tp.bet_type) = 'SUPERFECTA'
),
odds AS (
  SELECT product_id, event_id, runner_id, strength
  FROM `autobet-470818.autobet.vw_sf_strengths_from_win_horse`
)
SELECT product_id, event_id, runner_id, strength
FROM pred
UNION ALL
SELECT o.product_id, o.event_id, o.runner_id, o.strength
FROM odds o
LEFT JOIN pred p
  ON p.product_id = o.product_id AND p.runner_id = o.runner_id
WHERE p.product_id IS NULL;
