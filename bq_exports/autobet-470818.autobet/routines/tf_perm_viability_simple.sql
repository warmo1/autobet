-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_perm_viability_simple`(
  num_runners INT64,
  perm_k INT64,
  pool_gross_other NUMERIC,
  lines_covered INT64,
  stake_per_line NUMERIC,
  take_rate FLOAT64,
  net_rollover NUMERIC,
  include_self_in_pool BOOL,
  dividend_multiplier FLOAT64,
  f_share_override FLOAT64
)
AS
WITH base AS (
            SELECT CAST(num_runners AS INT64) AS N,
                   CAST(perm_k AS INT64) AS K,
                   CAST(pool_gross_other AS NUMERIC) AS O,
                   CAST(lines_covered AS INT64) AS M,
                   CAST(stake_per_line AS NUMERIC) AS stake_val,
                   CAST(take_rate AS FLOAT64) AS t,
                   CAST(net_rollover AS NUMERIC) AS R,
                   include_self_in_pool AS inc_self,
                   CAST(dividend_multiplier AS FLOAT64) AS mult,
                   CAST(f_share_override AS FLOAT64) AS f_fix
          ), perms AS (
            SELECT
              N, K, O, M, stake_val, t, R, inc_self, mult, f_fix,
              CASE WHEN N>=K AND K>0 THEN CAST(ROUND(EXP(SUM(LN(CAST(N - i AS FLOAT64))))) AS INT64) ELSE 0 END AS C
            FROM base, UNNEST(GENERATE_ARRAY(0, LEAST(K, GREATEST(N,0)) - 1)) AS i
            GROUP BY N, K, O, M, stake_val, t, R, inc_self, mult, f_fix
          ), clamp AS (
            SELECT *, CASE WHEN C>0 THEN LEAST(GREATEST(M,0), C) ELSE 0 END AS M_adj FROM perms
          ), cur AS (
            SELECT *, SAFE_DIVIDE(M_adj, C) AS alpha,
                   (M_adj * stake_val) AS S,
                   (CASE WHEN inc_self THEN (M_adj * stake_val) ELSE 0 END) AS S_inc
            FROM clamp
          ), fshare AS (
            SELECT *,
              CASE WHEN C=0 OR (C*stake_val + O)=0 THEN 0.0 ELSE CAST( (C*stake_val) / (C*stake_val + O) AS FLOAT64 ) END AS f_auto,
              CAST( mult * ( (1.0 - t) * ( (O + S_inc) ) + R ) AS NUMERIC ) AS NetPool_now
            FROM cur
          ), used AS (
            SELECT *, CAST(COALESCE(f_fix, f_auto) AS FLOAT64) AS f_used FROM fshare
          ), now_metrics AS (
            SELECT *, CAST(alpha * f_used * NetPool_now AS NUMERIC) AS ExpReturn,
              CAST(alpha * f_used * NetPool_now - S AS NUMERIC) AS ExpProfit,
              (alpha * f_used * NetPool_now - S) > 0 AS is_pos
            FROM used
          ), cover_all AS (
            SELECT *, CAST(C * stake_val AS NUMERIC) AS S_all,
              CAST(CASE WHEN inc_self THEN (C * stake_val) ELSE 0 END AS NUMERIC) AS S_all_inc,
              CAST( mult * ( (1.0 - t) * (O + (CASE WHEN inc_self THEN (C*stake_val) ELSE 0 END)) + R ) AS NUMERIC) AS NetPool_all,
              CAST( CAST(COALESCE(f_fix, f_auto) AS FLOAT64) * CAST( mult * ( (1.0 - t) * (O + (CASE WHEN inc_self THEN (C*stake_val) ELSE 0 END)) + R ) AS NUMERIC) - (C * stake_val) AS NUMERIC) AS ExpProfit_all
            FROM now_metrics
          ), cover_all_viab AS (
            SELECT *, (ExpProfit_all > 0) AS cover_all_pos,
              CAST( SAFE_DIVIDE( (C*stake_val) - (CAST(f_used AS FLOAT64) * mult * ( (1.0 - t) * (S_all_inc) + R ) ), (CAST(f_used AS FLOAT64) * mult * (1.0 - t)) ) AS NUMERIC ) AS O_min_cover_all
            FROM cover_all
          ), alpha_min AS (
            SELECT *,
              CASE
                WHEN C=0 OR stake_val=0 OR f_used<=0 OR (1.0 - t)<=0 THEN NULL
                WHEN inc_self THEN LEAST(1.0, GREATEST(0.0,
                  SAFE_DIVIDE(
                    CAST(C*stake_val AS FLOAT64) - (f_used * mult * ( (1.0 - t) * CAST(O AS FLOAT64) + CAST(R AS FLOAT64) )),
                    (f_used * mult * (1.0 - t) * CAST(C*stake_val AS FLOAT64))
                  )
                ))
                WHEN (f_used * mult * ( (1.0 - t) * CAST(O AS FLOAT64) + CAST(R AS FLOAT64) )) > CAST(C*stake_val AS FLOAT64)
                  THEN 0.0
                ELSE NULL
              END AS alpha_min_pos
            FROM cover_all_viab
          )
          SELECT C AS total_lines, M_adj AS lines_covered, alpha AS coverage_frac, S AS stake_total,
                 NetPool_now AS net_pool_if_bet, f_used AS f_share_used, ExpReturn AS expected_return,
                 ExpProfit AS expected_profit, is_pos AS is_positive_ev,
                 (C*stake_val) AS cover_all_stake, ExpProfit_all AS cover_all_expected_profit,
                 cover_all_pos AS cover_all_is_positive, O_min_cover_all AS cover_all_o_min_break_even,
                 alpha_min_pos AS coverage_frac_min_positive
          FROM alpha_min
