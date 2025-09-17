-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_superfecta_perms_horse_any`(
  in_product_id STRING,
  top_n INT64
)
AS
WITH runners AS (
    SELECT
      product_id,
      runner_id,
      strength,
      ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY strength DESC) AS rnk
    FROM `autobet-470818.autobet.vw_superfecta_runner_strength_horse_any`
    WHERE product_id = in_product_id
  ),
  top_r AS (SELECT * FROM runners WHERE rnk <= top_n),
  tot AS (SELECT product_id, SUM(strength) AS sum_s FROM top_r GROUP BY product_id)
  SELECT
    a.product_id,
    a.runner_id AS h1,
    b.runner_id AS h2,
    c.runner_id AS h3,
    d.runner_id AS h4,
    (a.strength / t.sum_s) *
    (b.strength / (t.sum_s - a.strength)) *
    (c.strength / (t.sum_s - a.strength - b.strength)) *
    (d.strength / (t.sum_s - a.strength - b.strength - c.strength)) AS p,
    1 AS line_cost_cents
  FROM top_r a
  JOIN top_r b ON b.product_id = a.product_id AND b.runner_id != a.runner_id
  JOIN top_r c ON c.product_id = a.product_id AND c.runner_id NOT IN (a.runner_id, b.runner_id)
  JOIN top_r d ON d.product_id = a.product_id AND d.runner_id NOT IN (a.runner_id, b.runner_id, c.runner_id)
  JOIN tot t ON t.product_id = a.product_id
