-- Verify Superfecta data end-to-end (event name, runners, odds, pool)
-- Project/Dataset: autobet-470818.autobet (change if needed)

-- 1) Superfecta products for today with event name and runner count
SELECT
  p.product_id,
  p.event_id,
  COALESCE(e.name, p.event_name) AS event_name,
  COALESCE(e.venue, p.venue) AS venue,
  UPPER(COALESCE(e.country, p.currency)) AS country,
  p.start_iso,
  COALESCE(p.status,'') AS status,
  p.total_gross,
  p.total_net,
  COUNT(s.selection_id) AS n_runners
FROM `autobet-470818.autobet.tote_products` AS p
LEFT JOIN `autobet-470818.autobet.tote_events` AS e USING(event_id)
LEFT JOIN `autobet-470818.autobet.tote_product_selections` AS s USING(product_id)
WHERE UPPER(p.bet_type)='SUPERFECTA'
  AND SUBSTR(p.start_iso,1,10) = FORMAT_DATE('%F', CURRENT_DATE())
GROUP BY product_id, event_id, event_name, venue, country, start_iso, status, total_gross, total_net
ORDER BY start_iso DESC;

-- 2) Runners with probable odds for a given product
-- Replace the value of pid below and run just this statement
DECLARE pid STRING DEFAULT 'REPLACE_WITH_PRODUCT_ID';

WITH base AS (
  SELECT
    s.product_id,
    s.number,
    s.selection_id,
    s.competitor AS horse_name,
    COALESCE(e.name, p.event_name) AS event_name,
    COALESCE(e.venue, p.venue) AS venue,
    UPPER(COALESCE(e.country, p.currency)) AS country,
    p.start_iso
  FROM `autobet-470818.autobet.tote_product_selections` AS s
  JOIN `autobet-470818.autobet.tote_products` AS p USING(product_id)
  LEFT JOIN `autobet-470818.autobet.tote_events` AS e USING(event_id)
  WHERE s.product_id = pid AND s.leg_index = 1
)
SELECT
  b.*,
  COALESCE(o1.decimal_odds, o2.decimal_odds) AS decimal_odds
FROM base AS b
LEFT JOIN `autobet-470818.autobet.vw_tote_probable_odds` AS o1
  ON o1.selection_id = b.selection_id
LEFT JOIN `autobet-470818.autobet.vw_tote_probable_odds` AS o2
  ON o2.product_id = b.product_id AND SAFE_CAST(o2.cloth_number AS INT64) = b.number
ORDER BY b.number;

-- 3) Latest pool snapshot (gross/net) for product, fallback to products
DECLARE pid2 STRING DEFAULT 'REPLACE_WITH_PRODUCT_ID';

WITH snap AS (
  SELECT product_id, ts_ms, total_gross, total_net, rollover, deduction_rate
  FROM `autobet-470818.autobet.tote_pool_snapshots`
  WHERE product_id = pid2
  ORDER BY ts_ms DESC
  LIMIT 1
), prod AS (
  SELECT product_id, total_gross, total_net, rollover, deduction_rate
  FROM `autobet-470818.autobet.tote_products`
  WHERE product_id = pid2
)
SELECT
  prod.product_id,
  COALESCE(snap.total_gross, prod.total_gross) AS pool_gross,
  COALESCE(snap.total_net, prod.total_net) AS pool_net,
  snap.ts_ms AS snapshot_ts_ms,
  prod.rollover,
  prod.deduction_rate,
  CASE WHEN snap.ts_ms IS NOT NULL THEN 'snapshot' ELSE 'products' END AS pool_source
FROM prod LEFT JOIN snap USING(product_id);

-- 4) Single-scenario viability via table function (requires views ensured)
DECLARE pid3 STRING DEFAULT 'REPLACE_WITH_PRODUCT_ID';
DECLARE alpha FLOAT64 DEFAULT 0.60;   -- coverage 60%
DECLARE l NUMERIC DEFAULT 0.10;       -- stake per line (£)
DECLARE t FLOAT64 DEFAULT 0.30;       -- takeout
DECLARE R NUMERIC DEFAULT 0.0;        -- net rollover (£)
DECLARE inc BOOL DEFAULT TRUE;        -- include self in pool
DECLARE m FLOAT64 DEFAULT 1.0;        -- dividend multiplier
DECLARE f FLOAT64 DEFAULT NULL;       -- f-share override (NULL -> auto)

WITH params AS (
  SELECT
    (SELECT COUNT(1) FROM `autobet-470818.autobet.tote_product_selections` WHERE product_id=pid3 AND leg_index=1) AS N,
    (
      SELECT COALESCE(
        -- Prefer latest snapshot gross
        (SELECT SAFE_CAST(total_gross AS NUMERIC) FROM `autobet-470818.autobet.tote_pool_snapshots` WHERE product_id=pid3 ORDER BY ts_ms DESC LIMIT 1),
        -- Fallback to products gross
        (SELECT SAFE_CAST(total_gross AS NUMERIC) FROM `autobet-470818.autobet.tote_products` WHERE product_id=pid3),
        -- Default to 0 if both missing
        NUMERIC '0'
      )
    ) AS O
), calc AS (
  SELECT
    N, O,
    CAST(ROUND(alpha * GREATEST(N*(N-1)*(N-2)*(N-3), 0)) AS INT64) AS M
  FROM params
)
SELECT *
FROM `autobet-470818.autobet`.tf_superfecta_viability_simple(
  (SELECT N FROM calc),
  (SELECT O FROM calc),
  (SELECT M FROM calc),
  l, t, R, inc, m, f
);

-- 5) Coverage vs EV grid (20 steps)
DECLARE pid4 STRING DEFAULT 'REPLACE_WITH_PRODUCT_ID';
DECLARE l2 NUMERIC DEFAULT 0.10;
DECLARE t2 FLOAT64 DEFAULT 0.30;
DECLARE R2 NUMERIC DEFAULT 0.0;
DECLARE inc2 BOOL DEFAULT TRUE;
DECLARE m2 FLOAT64 DEFAULT 1.0;
DECLARE f2 FLOAT64 DEFAULT NULL;

WITH params AS (
  SELECT
    (SELECT COUNT(1) FROM `autobet-470818.autobet.tote_product_selections` WHERE product_id=pid4 AND leg_index=1) AS N,
    (
      SELECT COALESCE(
        (SELECT SAFE_CAST(total_gross AS NUMERIC) FROM `autobet-470818.autobet.tote_pool_snapshots` WHERE product_id=pid4 ORDER BY ts_ms DESC LIMIT 1),
        (SELECT SAFE_CAST(total_gross AS NUMERIC) FROM `autobet-470818.autobet.tote_products` WHERE product_id=pid4),
        NUMERIC '0'
      )
    ) AS O
)
SELECT *
FROM `autobet-470818.autobet`.tf_superfecta_viability_grid(
  (SELECT N FROM params),
  (SELECT O FROM params),
  l2, t2, R2, inc2, m2, f2,
  20
)
ORDER BY coverage_frac;
