-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_superfecta_breakeven`(
  in_product_id STRING,
  top_n INT64,
  stake_per_line FLOAT64,
  f_share FLOAT64,
  net_rollover FLOAT64
)
AS
WITH cov AS (
    SELECT * FROM `autobet-470818.autobet.tf_superfecta_coverage`(in_product_id, top_n)
  )
  SELECT
    cov.product_id,
    cov.line_index,
    cov.line_index AS lines,
    cov.lines_frac,
    cov.cum_prob,
    cov.line_index * stake_per_line AS stake_total,
    SAFE_DIVIDE(
      (cov.line_index * stake_per_line)
        - f_share * (0.70 * (cov.line_index * stake_per_line) + net_rollover),
      0.70 * f_share
    ) AS o_min_break_even
  FROM cov
