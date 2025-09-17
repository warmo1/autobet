-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_exacta_pl_from_tote`(
  in_product_id STRING,
  top_n INT64
)
AS
WITH r_ranked AS (
            SELECT product_id, CAST(number AS STRING) AS rid, pct_units AS strength,
                   ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY pct_units DESC) as rn
            FROM `autobet-470818.autobet.vw_tote_probable_win`
            WHERE product_id = in_product_id
          ), r AS (
            SELECT * FROM r_ranked WHERE rn <= top_n
          ), tot AS (
            SELECT product_id, SUM(strength) AS sum_s FROM r GROUP BY product_id
          )
          SELECT r1.product_id,
                 r1.rid AS h1,
                 r2.rid AS h2,
                 (r1.strength/t.sum_s) * (r2.strength/(t.sum_s - r1.strength)) AS p
          FROM r r1
          JOIN r r2 ON r2.rid != r1.rid AND r2.product_id = r1.product_id
          JOIN tot t ON t.product_id = r1.product_id
