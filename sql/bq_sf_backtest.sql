-- Historical backtesting for a Superfecta weighting model in BigQuery.
--
-- Evaluates the Plackett–Luce-style permutation generator (`tf_superfecta_perms`)
-- against historical race results using winners from `hr_horse_runs`.
--
-- Inputs:
--   backtest_start_date: start date (inclusive)
--   backtest_end_date:   end date   (inclusive)
--   model_top_n:         number of top runners to include in permutations
--   cover_frac:          fraction of lines covered (e.g., 0.60 for 60%)
--
-- Notes:
--   - Assumes predictions are populated (table `predictions`) and `tote_params.model_id`
--     points to the model_id to use. The view `vw_superfecta_runner_strength` relies on this.
--   - Requires `ensure_views()` to have been run to create the table function and views.

DECLARE backtest_start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);
DECLARE backtest_end_date   DATE DEFAULT CURRENT_DATE();
DECLARE model_top_n         INT64 DEFAULT 10;
DECLARE cover_frac          FLOAT64 DEFAULT 0.60;

-- Helper CTE: closed Superfecta products in range with their events
WITH prods AS (
  SELECT
    p.product_id,
    p.event_id,
    p.event_name,
    COALESCE(e.venue, p.venue) AS venue,
    e.country,
    p.start_iso,
    p.currency,
    p.total_net,
    p.total_gross
  FROM `${BQ_PROJECT}.${BQ_DATASET}.tote_products` p
  LEFT JOIN `${BQ_PROJECT}.${BQ_DATASET}.tote_events` e USING(event_id)
  WHERE UPPER(p.bet_type) = 'SUPERFECTA'
    AND DATE(SUBSTR(p.start_iso,1,10)) BETWEEN backtest_start_date AND backtest_end_date
    AND UPPER(COALESCE(p.status,'')) IN ('CLOSED','SETTLED','RESULTED')
),
-- Winners by event (positions 1..4) from historical runs
winners AS (
  SELECT event_id,
         MAX(IF(finish_pos=1, horse_id, NULL)) AS h1,
         MAX(IF(finish_pos=2, horse_id, NULL)) AS h2,
         MAX(IF(finish_pos=3, horse_id, NULL)) AS h3,
         MAX(IF(finish_pos=4, horse_id, NULL)) AS h4
  FROM `${BQ_PROJECT}.${BQ_DATASET}.hr_horse_runs`
  WHERE finish_pos IS NOT NULL AND finish_pos BETWEEN 1 AND 4
  GROUP BY event_id
),
-- Expand permutations for each product using the model’s strengths
perms AS (
  SELECT
    p.product_id,
    t.h1, t.h2, t.h3, t.h4,
    t.p
  FROM prods p,
       `${BQ_PROJECT}.${BQ_DATASET}.tf_superfecta_perms`(p.product_id, model_top_n) AS t
),
-- Rank permutations by probability per product
ranked AS (
  SELECT
    product_id,
    h1, h2, h3, h4,
    p,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY p DESC) AS rn,
    COUNT(*)    OVER (PARTITION BY product_id) AS total_lines
  FROM perms
),
-- Winning tuple per product (requires all 4 finishers present)
answers AS (
  SELECT pr.product_id, pr.event_id, w.h1, w.h2, w.h3, w.h4
  FROM prods pr
  JOIN winners w USING(event_id)
  WHERE w.h1 IS NOT NULL AND w.h2 IS NOT NULL AND w.h3 IS NOT NULL AND w.h4 IS NOT NULL
),
-- Join ranked permutations with winning tuple to find the model rank and p for the true outcome
hits AS (
  SELECT
    a.product_id,
    a.event_id,
    r.total_lines,
    r.rn  AS winner_rank,
    r.p   AS winner_p
  FROM answers a
  JOIN ranked r
    ON r.product_id = a.product_id
   AND r.h1 = a.h1 AND r.h2 = a.h2 AND r.h3 = a.h3 AND r.h4 = a.h4
),
coverage AS (
  SELECT
    r.product_id,
    r.total_lines,
    CAST(ROUND(cover_frac * r.total_lines) AS INT64) AS cover_lines,
    MAX(IF(rn <= CAST(ROUND(cover_frac * r.total_lines) AS INT64), 1, 0)) OVER (PARTITION BY r.product_id) AS cover_flag
  FROM ranked r
)

-- Final per-product results: rank of the true outcome and hit@coverage
SELECT
  pr.product_id,
  pr.event_id,
  pr.event_name,
  pr.venue,
  pr.country,
  pr.start_iso,
  pr.total_net,
  h.total_lines,
  h.winner_rank,
  h.winner_p,
  c.cover_lines,
  (h.winner_rank IS NOT NULL AND h.winner_rank <= c.cover_lines) AS hit_at_coverage
FROM prods pr
LEFT JOIN hits h USING(product_id)
LEFT JOIN coverage c USING(product_id)
ORDER BY pr.start_iso;

-- To aggregate summary metrics, wrap the above SELECT in another query, e.g.:
-- SELECT
--   COUNTIF(hit_at_coverage) AS hits,
--   COUNT(1) AS races,
--   SAFE_DIVIDE(COUNTIF(hit_at_coverage), COUNT(1)) AS hit_rate
-- FROM ( ... the final SELECT above ... );
