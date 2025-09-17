-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_superfecta_coverage_any`(
  in_product_id STRING,
  top_n INT64
)
AS
WITH perms AS (
    SELECT * FROM `autobet-470818.autobet.tf_superfecta_perms_any`(in_product_id, top_n)
  ),
  ordered AS (
    SELECT
      product_id, h1, h2, h3, h4, prob,
      ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY prob DESC) AS rn,
      COUNT(*)    OVER (PARTITION BY product_id) AS total_lines
    FROM perms
  )
  SELECT
    product_id,
    total_lines,
    rn AS line_index,
    h1, h2, h3, h4,
    prob,
    SUM(prob) OVER (PARTITION BY product_id ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_prob,
    rn / total_lines AS lines_frac,
    rn / total_lines AS random_cov,
    SAFE_DIVIDE(
      SUM(prob) OVER (PARTITION BY product_id ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      rn / total_lines
    ) AS efficiency
  FROM ordered
