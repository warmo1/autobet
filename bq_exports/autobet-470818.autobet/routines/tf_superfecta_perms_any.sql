-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_superfecta_perms_any`(
  in_product_id STRING,
  top_n INT64
)
AS
WITH params AS (
    SELECT CASE WHEN top_n IS NULL OR top_n <= 0 THEN NULL ELSE top_n END AS limit_n
  ),
  product_key AS (SELECT in_product_id AS product_id),
  product_base AS (
    SELECT
      pk.product_id AS super_product_id,
      p.event_id
    FROM product_key pk
    JOIN `autobet-470818.autobet.vw_products_latest_totals` p
      ON p.product_id = pk.product_id
    LIMIT 1
  ),
  win_candidates AS (
    SELECT
      event_id,
      ARRAY_AGG(product_id ORDER BY product_id LIMIT 1)[OFFSET(0)] AS win_product_id
    FROM `autobet-470818.autobet.vw_products_latest_totals`
    WHERE UPPER(bet_type) = 'WIN'
      AND UPPER(COALESCE(status,'')) IN ('OPEN','SELLING')
    GROUP BY event_id
  ),
  prod_info AS (
    SELECT
      pb.super_product_id,
      pb.event_id,
      wc.win_product_id
    FROM product_base pb
    LEFT JOIN win_candidates wc USING (event_id)
  ),
  predicted_strength AS (
    SELECT
      product_id,
      CAST(runner_id AS STRING) AS runner_id,
      CAST(strength AS FLOAT64) AS strength,
      ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY strength DESC) AS rnk
    FROM `autobet-470818.autobet.vw_superfecta_runner_strength`
    WHERE product_id = in_product_id
  ),
  fallback_strength AS (
    SELECT
      pi.super_product_id AS product_id,
      CAST(CAST(o.cloth_number AS INT64) AS STRING) AS runner_id,
      SAFE_DIVIDE(1.0, CAST(o.decimal_odds AS FLOAT64)) AS strength,
      ROW_NUMBER() OVER (
        ORDER BY SAFE_DIVIDE(1.0, CAST(o.decimal_odds AS FLOAT64)) DESC
      ) AS rnk
    FROM prod_info pi
    JOIN `autobet-470818.autobet.vw_tote_probable_odds` o
      ON pi.win_product_id IS NOT NULL
     AND o.product_id = pi.win_product_id
    WHERE o.decimal_odds IS NOT NULL AND o.decimal_odds > 0
  ),
  use_pred AS (
    SELECT
      pk.product_id,
      COUNT(ps.runner_id) > 0 AS has_pred
    FROM product_key pk
    LEFT JOIN predicted_strength ps USING (product_id)
    GROUP BY pk.product_id
  ),
  strength_candidates AS (
    SELECT product_id, runner_id, strength, rnk, 'pred' AS source FROM predicted_strength
    UNION ALL
    SELECT product_id, runner_id, strength, rnk, 'fallback' AS source FROM fallback_strength
  ),
  strength_source AS (
    SELECT sc.product_id, sc.runner_id, sc.strength, sc.rnk
    FROM strength_candidates sc
    JOIN use_pred up USING (product_id)
    WHERE (up.has_pred AND sc.source = 'pred')
       OR (NOT up.has_pred AND sc.source = 'fallback')
  ),
  ranked AS (
    SELECT
      product_id,
      runner_id,
      strength,
      ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY strength DESC) AS rnk
    FROM strength_source
  ),
  limited AS (
    SELECT r.*
    FROM ranked r
    CROSS JOIN params p
    WHERE p.limit_n IS NULL OR r.rnk <= p.limit_n
  ),
  totals AS (
    SELECT product_id, SUM(strength) AS sum_strength
    FROM limited
    GROUP BY product_id
  )
  SELECT
    a.product_id,
    a.runner_id AS h1,
    b.runner_id AS h2,
    c.runner_id AS h3,
    d.runner_id AS h4,
    (
      COALESCE(SAFE_DIVIDE(a.strength, t.sum_strength), 0.0)
      * COALESCE(SAFE_DIVIDE(b.strength, t.sum_strength - a.strength), 0.0)
      * COALESCE(SAFE_DIVIDE(c.strength, t.sum_strength - a.strength - b.strength), 0.0)
      * COALESCE(SAFE_DIVIDE(d.strength, t.sum_strength - a.strength - b.strength - c.strength), 0.0)
    ) AS p,
    1 AS line_cost_cents
  FROM limited a
  JOIN limited b
    ON b.product_id = a.product_id AND b.runner_id != a.runner_id
  JOIN limited c
    ON c.product_id = a.product_id AND c.runner_id NOT IN (a.runner_id, b.runner_id)
  JOIN limited d
    ON d.product_id = a.product_id AND d.runner_id NOT IN (a.runner_id, b.runner_id, c.runner_id)
  JOIN totals t ON t.product_id = a.product_id
