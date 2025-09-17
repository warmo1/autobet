-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_sf_backtest_horse_any`(
  start_date DATE,
  end_date DATE,
  top_n INT64,
  cover_frac FLOAT64
)
AS
WITH prods AS (
    SELECT
      p.product_id, p.event_id, p.event_name,
      COALESCE(e.venue, p.venue) AS venue,
      e.country, p.start_iso, p.total_net
    FROM `autobet-470818.autobet.tote_products` p
    LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id)
    WHERE UPPER(p.bet_type)='SUPERFECTA'
      AND DATE(SUBSTR(p.start_iso,1,10)) BETWEEN start_date AND end_date
      AND UPPER(COALESCE(p.status,'')) IN ('CLOSED','SETTLED','RESULTED')
  ),
  strengths AS (
    SELECT s.product_id, s.runner_id AS horse_id, s.strength
    FROM `autobet-470818.autobet.vw_superfecta_runner_strength_horse_any` s
    JOIN prods USING(product_id)
  ),
  strengths_ok AS (
    SELECT product_id FROM strengths GROUP BY product_id HAVING COUNT(*) >= 4
  ),
  prods_ready AS (SELECT * FROM prods WHERE product_id IN (SELECT product_id FROM strengths_ok)),
  winners AS (
    SELECT * FROM `autobet-470818.autobet.vw_event_winners_1_4`
  ),
  rr AS (
    SELECT
      product_id, horse_id, strength,
      ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY strength DESC) AS rnk
    FROM strengths
    WHERE product_id IN (SELECT product_id FROM prods_ready)
  ),
  top_r AS (SELECT * FROM rr WHERE rnk <= top_n),
  tot AS (SELECT product_id, SUM(strength) AS sum_s FROM top_r GROUP BY product_id),
  perms AS (
    SELECT
      a.product_id,
      a.horse_id AS h1,
      b.horse_id AS h2,
      c.horse_id AS h3,
      d.horse_id AS h4,
      (a.strength / t.sum_s) *
      (b.strength / (t.sum_s - a.strength)) *
      (c.strength / (t.sum_s - a.strength - b.strength)) *
      (d.strength / (t.sum_s - a.strength - b.strength - c.strength)) AS prob
    FROM top_r a
    JOIN top_r b ON b.product_id=a.product_id AND b.horse_id!=a.horse_id
    JOIN top_r c ON c.product_id=a.product_id AND c.horse_id NOT IN (a.horse_id,b.horse_id)
    JOIN top_r d ON d.product_id=a.product_id AND d.horse_id NOT IN (a.horse_id,b.horse_id,c.horse_id)
    JOIN tot t    ON t.product_id=a.product_id
  ),
  ranked AS (
    SELECT
      product_id, h1,h2,h3,h4, prob,
      ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY prob DESC) AS rn,
      COUNT(*)    OVER (PARTITION BY product_id) AS total_lines
    FROM perms
  ),
  answers AS (
    SELECT pr.product_id, pr.event_id, w.h1, w.h2, w.h3, w.h4
    FROM prods_ready pr
    JOIN winners w USING(event_id)
  ),
  hits AS (
    SELECT
      a.product_id, a.event_id,
      r.total_lines,
      r.rn   AS winner_rank,
      r.prob AS winner_prob
    FROM answers a
    JOIN ranked r
      ON r.product_id=a.product_id
     AND r.h1=a.h1 AND r.h2=a.h2 AND r.h3=a.h3 AND r.h4=a.h4
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
    pr.product_id, pr.event_id, pr.event_name, pr.venue, pr.country, pr.start_iso, pr.total_net,
    ht.total_lines, ht.winner_rank, ht.winner_prob, cv.cover_lines,
    (ht.winner_rank IS NOT NULL AND ht.winner_rank <= cv.cover_lines) AS hit_at_coverage
  FROM prods_ready pr
  LEFT JOIN hits     AS ht USING(product_id)
  LEFT JOIN coverage AS cv USING(product_id)
