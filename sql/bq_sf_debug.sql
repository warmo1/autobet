-- BigQuery SQL helpers for Superfecta audit debugging
--
-- How to run (from this repo root):
--   bq query --use_legacy_sql=false < sql/bq_sf_debug.sql
--
-- Dataset: update the prefix below if different from your environment.
-- Current: autobet-470818.autobet

-- 1) Selection map for SUPERFECTA products (numbers → selection IDs)
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_sf_selection_map` AS
SELECT
  s.product_id,
  ANY_VALUE(s.product_leg_id) AS product_leg_id,
  s.number AS cloth_or_trap,
  s.selection_id,
  ANY_VALUE(s.competitor) AS competitor,
  ANY_VALUE(p.event_id) AS event_id,
  ANY_VALUE(COALESCE(e.venue, p.venue)) AS venue,
  ANY_VALUE(p.start_iso) AS start_iso,
  MAX(s.units_ts_ms) AS units_ts_ms,
  ANY_VALUE(s.total_units) AS total_units
FROM `autobet-470818.autobet.tote_product_selections` s
LEFT JOIN `autobet-470818.autobet.tote_products` p USING (product_id)
LEFT JOIN `autobet-470818.autobet.tote_events` e USING (event_id)
GROUP BY s.product_id, s.number, s.selection_id;

-- 2) Health checks for selection mapping integrity
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_sf_selection_health` AS
WITH base AS (
  SELECT
    s.product_id,
    COUNTIF(s.product_leg_id IS NULL) AS n_missing_leg_id,
    COUNTIF(s.number IS NULL) AS n_missing_number
  FROM `autobet-470818.autobet.tote_product_selections` s
  GROUP BY s.product_id
),
dups AS (
  SELECT product_id, cloth_or_trap, COUNT(*) AS n
  FROM `autobet-470818.autobet.vw_sf_selection_map`
  GROUP BY product_id, cloth_or_trap
  HAVING COUNT(*) > 1
),
no_sel AS (
  SELECT p.product_id
  FROM `autobet-470818.autobet.tote_products` p
  LEFT JOIN `autobet-470818.autobet.tote_product_selections` s
  ON p.product_id = s.product_id
  WHERE UPPER(p.bet_type)='SUPERFECTA'
  GROUP BY p.product_id
  HAVING COUNT(s.selection_id)=0
)
SELECT
  p.product_id,
  p.event_id,
  p.event_name,
  COALESCE(e.venue, p.venue) AS venue,
  p.start_iso,
  COALESCE(b.n_missing_leg_id,0) AS n_missing_leg_id,
  COALESCE(b.n_missing_number,0) AS n_missing_number,
  COUNT(d.product_id) AS n_dup_numbers,
  CASE WHEN p.product_id IN (SELECT product_id FROM no_sel) THEN 1 ELSE 0 END AS no_selections
FROM `autobet-470818.autobet.tote_products` p
LEFT JOIN base b ON b.product_id = p.product_id
LEFT JOIN dups d ON d.product_id = p.product_id
LEFT JOIN `autobet-470818.autobet.tote_events` e ON e.event_id = p.event_id
WHERE UPPER(p.bet_type)='SUPERFECTA'
GROUP BY p.product_id, p.event_id, p.event_name, venue, p.start_iso, n_missing_leg_id, n_missing_number, no_selections;

-- 3) Products with selections (sanity)
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_sf_products_with_selections` AS
SELECT
  p.product_id,
  p.event_id,
  p.event_name,
  COALESCE(e.venue, p.venue) AS venue,
  p.start_iso,
  COUNT(s.selection_id) AS n_selections
FROM `autobet-470818.autobet.tote_products` p
JOIN `autobet-470818.autobet.tote_product_selections` s USING (product_id)
LEFT JOIN `autobet-470818.autobet.tote_events` e USING (event_id)
WHERE UPPER(p.bet_type)='SUPERFECTA'
GROUP BY p.product_id, p.event_id, p.event_name, venue, p.start_iso;

-- 4) Table function: build a Superfecta line (numbers 1-4 → placement selections JSON)
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_sf_build_line`(
  in_product_id STRING,
  n1 INT64,
  n2 INT64,
  n3 INT64,
  n4 INT64
)
RETURNS TABLE<
  product_id STRING,
  product_leg_id STRING,
  selections ARRAY<STRUCT<productLegSelectionID STRING, position INT64>>,
  missing_numbers ARRAY<INT64>,
  payload_v1 JSON,
  payload_v2 JSON
>
AS (
  WITH map AS (
    SELECT product_id, product_leg_id, cloth_or_trap AS n, selection_id
    FROM `autobet-470818.autobet.vw_sf_selection_map`
    WHERE product_id = in_product_id
  ),
  wanted AS (
    SELECT 1 AS pos, n1 AS n UNION ALL
    SELECT 2, n2 UNION ALL
    SELECT 3, n3 UNION ALL
    SELECT 4, n4
  ),
  joined AS (
    SELECT w.pos, w.n,
           ANY_VALUE(m.selection_id) AS selection_id,
           ANY_VALUE(m.product_leg_id) AS product_leg_id
    FROM wanted w
    LEFT JOIN map m ON m.n = w.n
    GROUP BY w.pos, w.n
  ),
  sel AS (
    SELECT
      ANY_VALUE(j.product_leg_id) AS product_leg_id,
      ARRAY_AGG(STRUCT(j.selection_id AS productLegSelectionID, j.pos AS position)
                ORDER BY j.pos) AS selections,
      ARRAY_AGG(IF(j.selection_id IS NULL, j.n, NULL) IGNORE NULLS) AS missing
    FROM joined j
  )
  SELECT
    in_product_id AS product_id,
    s.product_leg_id,
    s.selections,
    s.missing AS missing_numbers,
    -- v1 audit (ticket) JSON fragment: array of bets, stake added client-side
    TO_JSON(ARRAY<STRUCT<
      betId STRING,
      productId STRING,
      stake STRUCT<currencyCode STRING, totalAmount NUMERIC>,
      legs ARRAY<STRUCT<productLegId STRING, selections ARRAY<STRUCT<productLegSelectionID STRING, position INT64>>>>
    >>[
      (STRUCT(
          CONCAT('bet-superfecta-', SUBSTR(TO_HEX(FARM_FINGERPRINT(CONCAT(in_product_id, CAST(n1 AS STRING), CAST(n2 AS STRING), CAST(n3 AS STRING), CAST(n4 AS STRING)))), 1, 16)) AS betId,
          in_product_id AS productId,
          STRUCT('GBP' AS currencyCode, CAST(NULL AS NUMERIC) AS totalAmount) AS stake,
          [STRUCT(s.product_leg_id AS productLegId, s.selections AS selections)] AS legs
      ))
    ]) AS payload_v1,
    -- v2 (results) JSON fragment
    TO_JSON(ARRAY<STRUCT<
      bet STRUCT<
        productId STRING,
        stake STRUCT<amount STRUCT<decimalAmount NUMERIC>, currency STRING>,
        legs ARRAY<STRUCT<productLegId STRING, selections ARRAY<STRUCT<productLegSelectionID STRING, position INT64>>>>
      >
    >>[
      (STRUCT(
        STRUCT(
          in_product_id AS productId,
          STRUCT(STRUCT(CAST(NULL AS NUMERIC) AS decimalAmount) AS amount, 'GBP' AS currency) AS stake,
          [STRUCT(s.product_leg_id AS productLegId, s.selections AS selections)] AS legs
        ) AS bet
      ))
    ]) AS payload_v2
  FROM sel s
);

--
-- Examples (uncomment and set your product_id to run)
--
-- SELECT *
-- FROM `autobet-470818.autobet.vw_sf_selection_map`
-- WHERE product_id = 'YOUR_PRODUCT_ID'
-- ORDER BY cloth_or_trap;
--
-- SELECT *
-- FROM `autobet-470818.autobet.vw_sf_selection_health`
-- WHERE DATE(SUBSTR(start_iso,1,10)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
-- ORDER BY start_iso DESC;
--
-- SELECT *
-- FROM `autobet-470818.autobet.tf_sf_build_line`('YOUR_PRODUCT_ID', 1, 2, 4, 7);
