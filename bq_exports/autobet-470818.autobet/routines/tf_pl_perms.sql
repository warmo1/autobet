-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_pl_perms`(
  in_product_id STRING,
  top_n INT64
)
AS
WITH runners AS (
                SELECT
                    product_id,
                    selection_id,
                    strength,
                    ROW_NUMBER() OVER (
                        PARTITION BY product_id
                        ORDER BY
                            strength DESC
                    ) AS rnk
                FROM
                    `autobet-470818.autobet.vw_pl_strength`
                WHERE
                    product_id = in_product_id
            ),
            top_r AS (
                SELECT
                    *
                FROM
                    runners
                WHERE
                    rnk <= top_n
            ),
            tot AS (
                SELECT
                    product_id,
                    SUM(strength) AS sum_s
                FROM
                    top_r
                GROUP BY
                    product_id
            )
            SELECT
                tr1.product_id,
                tr1.selection_id AS h1,
                tr2.selection_id AS h2,
                tr3.selection_id AS h3,
                tr4.selection_id AS h4,
                (tr1.strength / t.sum_s) * (
                    tr2.strength / (t.sum_s - tr1.strength)
                ) * (
                    tr3.strength / (
                        t.sum_s - tr1.strength - tr2.strength
                    )
                ) * (
                    tr4.strength / (
                        t.sum_s - tr1.strength - tr2.strength - tr3.strength
                    )
                ) AS p,
                1 AS line_cost_cents
            FROM
                top_r tr1
                JOIN top_r tr2 ON tr2.selection_id != tr1.selection_id
                JOIN top_r tr3 ON tr3.selection_id NOT IN (tr1.selection_id, tr2.selection_id)
                JOIN top_r tr4 ON tr4.selection_id NOT IN (
                    tr1.selection_id,
                    tr2.selection_id,
                    tr3.selection_id
                )
                JOIN tot t ON t.product_id = tr1.product_id
