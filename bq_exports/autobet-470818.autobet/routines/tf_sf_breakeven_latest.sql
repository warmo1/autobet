-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_sf_breakeven_latest`(
  in_product_id STRING,
  top_n INT64
)
AS
WITH prm AS (
    SELECT t, f, stake_per_line, R
    FROM `autobet-470818.autobet.tote_params`
    ORDER BY updated_ts DESC
    LIMIT 1
  )
  SELECT b.*
  FROM `autobet-470818.autobet.tf_superfecta_breakeven`(
    in_product_id, top_n,
    (SELECT stake_per_line FROM prm),
    (SELECT f FROM prm),
    (SELECT R FROM prm)
  ) AS b
