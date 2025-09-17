CREATE VIEW `autobet-470818.autobet.vw_superfecta_runner_strength`
AS WITH prm AS (
  SELECT model_id, ts_ms
  FROM `autobet-470818.autobet.tote_params`
  WHERE model_id IS NOT NULL
  ORDER BY updated_ts DESC
  LIMIT 1
), latest AS (
  SELECT p.model_id,
         IFNULL((SELECT ts_ms FROM prm), MAX(p.ts_ms)) AS ts_ms
  FROM `autobet-470818.autobet.predictions` p
  WHERE p.model_id = (SELECT model_id FROM prm)
  GROUP BY p.model_id
)
SELECT
  tp.product_id,
  p.event_id,
  CAST(p.horse_id AS STRING) AS runner_id,  -- standardized name
  GREATEST(p.proba, 1e-9) AS strength
FROM `autobet-470818.autobet.predictions` p
JOIN latest l  ON l.model_id = p.model_id AND l.ts_ms = p.ts_ms
JOIN `autobet-470818.autobet.tote_products` tp ON tp.event_id = p.event_id
WHERE UPPER(tp.bet_type) = 'SUPERFECTA';
