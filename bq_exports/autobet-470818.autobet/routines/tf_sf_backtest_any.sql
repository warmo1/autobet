-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_sf_backtest_any`(
  start_date DATE,
  end_date DATE,
  model_top_n INT64,
  cover_frac FLOAT64
)
AS
WITH prods AS (
    SELECT
      p.product_id, p.event_id, p.event_name,
      COALESCE(e.venue, p.venue) AS venue,
      e.country,
      p.start_iso, p.total_net
    FROM `autobet-470818.autobet.tote_products` p
    LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id)
    WHERE UPPER(p.bet_type) = 'SUPERFECTA'
      AND DATE(SUBSTR(p.start_iso,1,10)) BETWEEN start_date AND end_date
      AND UPPER(COALESCE(p.status,'')) IN ('CLOSED','SETTLED','RESULTED')
  ),
  has_strengths AS (
    SELECT product_id
    FROM `autobet-470818.autobet.vw_superfecta_runner_strength_any`
    GROUP BY product_id
    HAVING COUNT(*) >= 4
  ),
  prods_ready AS (
    SELECT p.* FROM prods p JOIN has_strengths USING(product_id)
  ),
  winners AS (
    SELECT
      event_id,
      MAX(IF(finish_pos=1, horse_id, NULL)) AS h1,
      MAX(IF(finish_pos=2, horse_id, NULL)) AS h2,
      MAX(IF(finish_pos=3, horse_id, NULL)) AS h3,
      MAX(IF(finish_pos=4, horse_id, NULL)) AS h4
    FROM `autobet-470818.autobet.hr_horse_runs`
    WHERE finish_pos BETWEEN 1 AND 4
    GROUP BY event_id
    HAVING COUNTIF(finish_pos=1)=1
       AND COUNTIF(finish_pos=2)=1
       AND COUNTIF(finish_pos=3)=1
       AND COUNTIF(finish_pos=4)=1
  ),
  strengths AS (
    SELECT s.product_id, s.runner_id, s.strength
    FROM `autobet-470818.autobet.vw_superfecta_runner_strength_any` s
    JOIN prods_ready USING (product_id)
  ),
  ranked_runners AS (
    SELECT
      product_id, runner_id, strength,
      ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY strength DESC) AS rnk
    FROM strengths
  ),
  top_r AS (
    SELECT * FROM ranked_runners WHERE rnk <= model_top_n
  ),
  tot AS (
    SELECT product_id, SUM(strength) AS sum_s
    FROM top_r
    GROUP BY product_id
  ),
  perms AS (
    SELECT
      a.product_id,
      a.runner_id AS h1,
      b.runner_id AS h2,
      c.runner_id AS h3,
      d.runner_id AS h4,
      (a.strength / t.sum_s) *
      (b.strength / (t.sum_s - a.strength)) *
      (c.strength / (t.sum_s - a.strength - b.strength)) *
      (d.strength / (t.sum_s - a.strength - b.strength - c.strength)) AS prob
    FROM top_r a
    JOIN top_r b ON b.product_id = a.product_id AND b.runner_id != a.runner_id
    JOIN top_r c ON c.product_id = a.product_id AND c.runner_id NOT IN (a.runner_id, b.runner_id)
    JOIN top_r d ON d.product_id = a.product_id AND d.runner_id NOT IN (a.runner_id, b.runner_id, c.runner_id)
    JOIN tot t ON t.product_id = a.product_id
  ),
  ranked AS (
    SELECT
      product_id, h1, h2, h3, h4, prob,
      ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY prob DESC) AS rn,
      COUNT(*)    OVER (PARTITION BY product_id) AS total_lines
    FROM perms
  ),
  answers AS (
    SELECT p.product_id, p.event_id, w.h1, w.h2, w.h3, w.h4
    FROM prods_ready p
    JOIN winners w USING(event_id)
  ),
  hits AS (
    SELECT
      a.product_id,
      a.event_id,
      r.total_lines,
      r.rn   AS winner_rank,
      r.prob AS winner_prob
    FROM answers a
    JOIN ranked r
      ON r.product_id = a.product_id
     AND r.h1 = a.h1 AND r.h2 = a.h2 AND r.h3 = a.h3 AND r.h4 = a.h4
  ),
  coverage AS (
    SELECT
      product_id,
      ANY_VALUE(total_lines) AS total_lines,
      CAST(ROUND(cover_frac * ANY_VALUE(total_lines)) AS INT64) AS cover_lines
    FROM ranked
    GROUP BY product_id
  )
  SELECT
    pr.product_id,
    pr.event_id,
    pr.event_name,
    pr.venue,
    pr.country,
    pr.start_iso,
    pr.total_net,
    ht.total_lines,
    ht.winner_rank,
    ht.winner_prob,
    cv.cover_lines,
    (ht.winner_rank IS NOT NULL AND ht.winner_rank <= cv.cover_lines) AS hit_at_coverage
  FROM prods_ready pr
  LEFT JOIN hits     AS ht USING(product_id)
  LEFT JOIN coverage AS cv USING(product_id)
