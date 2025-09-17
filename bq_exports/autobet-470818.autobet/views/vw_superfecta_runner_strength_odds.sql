CREATE VIEW `autobet-470818.autobet.vw_superfecta_runner_strength_odds`
AS WITH base AS (
  SELECT
    p.product_id,
    p.event_id,
    o.selection_id AS runner_id,
    SAFE_CAST(o.decimal_odds AS FLOAT64) AS decimal_odds
  FROM `autobet-470818.autobet.tote_products` p
  JOIN `autobet-470818.autobet.vw_tote_probable_odds` o
    ON o.product_id = p.product_id
  WHERE UPPER(p.bet_type) = 'SUPERFECTA'
),
raw AS (
  SELECT
    product_id,
    event_id,
    runner_id,
    SAFE_DIVIDE(1.0, NULLIF(decimal_odds, 0.0)) AS s
  FROM base
  WHERE decimal_odds IS NOT NULL AND decimal_odds > 0
)
SELECT
  product_id,
  event_id,
  CAST(runner_id AS STRING) AS runner_id,  -- standardized name
  SAFE_DIVIDE(s, SUM(s) OVER (PARTITION BY product_id)) AS strength
FROM raw;
