CREATE VIEW `autobet-470818.autobet.vw_superfecta_runner_strength_any`
AS WITH pred AS (  -- your model’s strengths (leave as-is if already present)
  SELECT product_id, event_id, runner_id, strength
  FROM `autobet-470818.autobet.vw_superfecta_runner_strength`   -- <— your predictive view
),
fallback AS (
  SELECT product_id, event_id, runner_id, strength
  FROM `autobet-470818.autobet.vw_sf_strengths_from_win_horse`
),
pred_products AS (SELECT DISTINCT product_id FROM pred)

SELECT p.product_id, p.event_id, p.runner_id, p.strength
FROM pred p
UNION ALL
SELECT f.product_id, f.event_id, f.runner_id, f.strength
FROM fallback f
LEFT JOIN pred_products pp USING (product_id)
WHERE pp.product_id IS NULL;
