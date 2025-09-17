-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_sf_build_line`(
  in_product_id STRING,
  n1 INT64,
  n2 INT64,
  n3 INT64,
  n4 INT64
)
AS
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
    SELECT
      w.pos,
      w.n,
      ANY_VALUE(m.selection_id) AS selection_id,
      ANY_VALUE(m.product_leg_id) AS product_leg_id
    FROM wanted w
    LEFT JOIN map m ON m.n = w.n
    GROUP BY w.pos, w.n
  ),
  sel AS (
    SELECT
      ANY_VALUE(j.product_leg_id) AS product_leg_id,
      ARRAY_AGG(STRUCT(j.selection_id AS productLegSelectionID, j.pos AS position) ORDER BY j.pos) AS selections,
      ARRAY_AGG(IF(j.selection_id IS NULL, j.n, NULL) IGNORE NULLS) AS missing
    FROM joined j
  )
  SELECT
    in_product_id AS product_id,
    s.product_leg_id,
    s.selections,
    s.missing AS missing_numbers,
    TO_JSON(STRUCT(
      'bet-superfecta-debug' AS ticketId,
      [STRUCT(
        'bet-superfecta-debug' AS betId,
        in_product_id AS productId,
        STRUCT(1.0 AS totalAmount, 'GBP' AS currencyCode) AS stake,
        [STRUCT(s.product_leg_id AS productLegId, s.selections AS selections)] AS legs
      )] AS bets
    )) AS payload_v1,
    TO_JSON(STRUCT(
      'bet-superfecta-debug' AS ticketId,
      [STRUCT(
        'bet-superfecta-debug' AS betId,
        in_product_id AS productId,
        STRUCT(1.0 AS totalAmount, 'GBP' AS currencyCode) AS stake,
        [STRUCT(s.product_leg_id AS productLegId, s.selections AS selections)] AS legs
      )] AS bets
    )) AS payload_v2
  FROM sel s
