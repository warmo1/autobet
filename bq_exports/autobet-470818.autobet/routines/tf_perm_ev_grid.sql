-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_perm_ev_grid`(
  in_product_id STRING,
  in_top_n INT64,
  O NUMERIC,
  S NUMERIC,
  t FLOAT64,
  R NUMERIC,
  inc BOOL,
  mult FLOAT64,
  f FLOAT64,
  conc FLOAT64,
  mi FLOAT64
)
AS
WITH inputs AS (
    SELECT
      CAST(IFNULL(S, 0) AS FLOAT64) AS stake_total,
      CAST(IFNULL(O, 0) AS FLOAT64) AS other_pool,
      CAST(IFNULL(t, 0) AS FLOAT64) AS take_rate,
      CAST(IFNULL(R, 0) AS FLOAT64) AS rollover,
      CAST(IFNULL(mult, 1) AS FLOAT64) AS multiplier,
      CAST(f AS FLOAT64) AS f_override,
      CAST(IFNULL(conc, 0) AS FLOAT64) AS concentration,
      CAST(IFNULL(mi, 0) AS FLOAT64) AS mi_factor,
      IFNULL(inc, FALSE) AS include_self
  ),
  cfg AS (
    SELECT
      *,
      GREATEST(0.1, 1.0 - 0.6 * mi_factor) AS beta,
      1.0 + 2.0 * concentration AS gamma,
      multiplier * (((1.0 - take_rate) * (other_pool + CASE WHEN include_self THEN stake_total ELSE 0 END)) + rollover) AS net_pool,
      other_pool * (1.0 - mi_factor) AS other_effective
    FROM inputs
  ),
  product_key AS (SELECT in_product_id AS product_id),
  product_base AS (
    SELECT
      pk.product_id AS super_product_id,
      p.event_id
    FROM product_key pk
    JOIN `autobet-470818.autobet.vw_products_latest_totals` p
      ON p.product_id = pk.product_id
    LIMIT 1
  ),
  win_candidates AS (
    SELECT
      event_id,
      ARRAY_AGG(product_id ORDER BY product_id LIMIT 1)[OFFSET(0)] AS win_product_id
    FROM `autobet-470818.autobet.vw_products_latest_totals`
    WHERE UPPER(bet_type) = 'WIN'
      AND UPPER(COALESCE(status,'')) IN ('OPEN','SELLING')
    GROUP BY event_id
  ),
  prod_info AS (
    SELECT pb.super_product_id, pb.event_id, wc.win_product_id
    FROM product_base pb
    LEFT JOIN win_candidates wc USING (event_id)
  ),
  runner_odds AS (
    SELECT
      CAST(o.cloth_number AS STRING) AS runner_id,
      CAST(o.decimal_odds AS FLOAT64) AS odds
    FROM prod_info pi
    JOIN `autobet-470818.autobet.vw_tote_probable_odds` o
      ON pi.win_product_id IS NOT NULL
     AND o.product_id = pi.win_product_id
  ),
  perms AS (
    SELECT * FROM `autobet-470818.autobet.tf_superfecta_perms_any`(in_product_id, in_top_n)
  ),
  ranked AS (
    SELECT
      ROW_NUMBER() OVER (ORDER BY p DESC) AS rn,
      p,
      h1, h2, h3, h4
    FROM perms
  ),
  scored AS (
    SELECT
      r.rn,
      r.p,
      POW(GREATEST(r.p, 1e-12), cfg.gamma) AS w_raw,
      CASE
        WHEN (IFNULL(o1.odds, 0.0) > 0 OR IFNULL(o2.odds, 0.0) > 0 OR IFNULL(o3.odds, 0.0) > 0 OR IFNULL(o4.odds, 0.0) > 0) THEN
          GREATEST(
            IFNULL(POW(1.0 / NULLIF(o1.odds, 0.0), cfg.beta), 1.0)
            * IFNULL(POW(1.0 / NULLIF(o2.odds, 0.0), cfg.beta), 1.0)
            * IFNULL(POW(1.0 / NULLIF(o3.odds, 0.0), cfg.beta), 1.0)
            * IFNULL(POW(1.0 / NULLIF(o4.odds, 0.0), cfg.beta), 1.0),
            1e-12
          )
        ELSE
          GREATEST(POW(GREATEST(r.p, 1e-12), cfg.beta), 1e-12)
      END AS q_raw
    FROM ranked r
    CROSS JOIN cfg
    LEFT JOIN runner_odds o1 ON o1.runner_id = r.h1
    LEFT JOIN runner_odds o2 ON o2.runner_id = r.h2
    LEFT JOIN runner_odds o3 ON o3.runner_id = r.h3
    LEFT JOIN runner_odds o4 ON o4.runner_id = r.h4
  ),
  agg AS (
    SELECT
      rn,
      SUM(p) OVER (ORDER BY rn) AS cum_prob,
      SUM(w_raw) OVER (ORDER BY rn) AS cum_w,
      SUM(q_raw) OVER (ORDER BY rn) AS cum_q
    FROM scored
  ),
  expanded AS (
    SELECT
      a.rn AS m,
      s.rn AS line_rn,
      s.p,
      s.w_raw,
      s.q_raw,
      a.cum_w,
      a.cum_q
    FROM agg a
    JOIN scored s ON s.rn <= a.rn
  ),
  calc_rows AS (
    SELECT
      e.m,
      e.line_rn,
      e.p,
      COALESCE(SAFE_DIVIDE(cfg.stake_total * e.w_raw, NULLIF(e.cum_w, 0.0)), 0.0) AS stake_i,
      COALESCE(SAFE_DIVIDE(cfg.other_effective * e.q_raw, NULLIF(e.cum_q, 0.0)), 0.0) AS others_i,
      cfg.*
    FROM expanded e
    CROSS JOIN cfg
  ),
  calc_with_f AS (
    SELECT
      m,
      line_rn,
      p,
      stake_i,
      others_i,
      CASE
        WHEN f_override IS NOT NULL THEN f_override
        ELSE COALESCE(SAFE_DIVIDE(stake_i, stake_i + others_i), 0.0)
      END AS f_i,
      net_pool
    FROM calc_rows
  ),
  summary AS (
    SELECT
      m,
      SUM(p) AS hit_rate,
      SUM(p * f_i * net_pool) AS exp_return_sum,
      SUM(p * f_i * net_pool - stake_i) AS exp_profit_sum,
      SUM(p * f_i) AS pf_sum
    FROM calc_with_f
    GROUP BY m
  )
  SELECT
    m AS lines_covered,
    hit_rate,
    exp_return_sum AS expected_return,
    exp_profit_sum AS expected_profit,
    SAFE_DIVIDE(pf_sum, NULLIF(hit_rate, 0.0)) AS f_share_used
  FROM summary
  ORDER BY lines_covered
