-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_multileg_viability_simple`(
  leg_lines ARRAY<INT64>,
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
            SELECT CAST(pool_gross_other AS NUMERIC) AS O,
                   CAST(lines_covered AS INT64) AS M,
                   CAST(stake_per_line AS NUMERIC) AS stake_val,
                   CAST(take_rate AS FLOAT64) AS t,
                   CAST(net_rollover AS NUMERIC) AS R,
                   include_self_in_pool AS inc_self,
                   CAST(dividend_multiplier AS FLOAT64) AS mult,
                   CAST(f_share_override AS FLOAT64) AS f_fix
          ), tot AS (
            SELECT CAST(ROUND(EXP(SUM(LN(CAST(x AS FLOAT64))))) AS INT64) AS C FROM UNNEST(leg_lines) AS x
          ), cur AS (
            SELECT *,
                   (SELECT C FROM tot) AS C,
                   CASE WHEN (SELECT C FROM tot)>0 THEN LEAST(GREATEST(M,0), (SELECT C FROM tot)) ELSE 0 END AS M_adj
            FROM base
          ), k AS (
            SELECT *, SAFE_DIVIDE(M_adj, C) AS alpha,
                   (M_adj * stake_val) AS S,
                   (CASE WHEN inc_self THEN (M_adj * stake_val) ELSE 0 END) AS S_inc
            FROM cur
          ), fshare AS (
            SELECT *,
              CASE WHEN C=0 OR (C*stake_val + O)=0 THEN 0.0 ELSE CAST( (C*stake_val) / (C*stake_val + O) AS FLOAT64 ) END AS f_auto,
              CAST( mult * ( (1.0 - t) * ( (O + S_inc) ) + R ) AS NUMERIC ) AS NetPool_now
            FROM k
          )
          SELECT
            C AS total_lines,
            M_adj AS lines_covered,
            SAFE_DIVIDE(M_adj, C) AS coverage_frac,
            (M_adj * stake_val) AS stake_total,
            NetPool_now AS net_pool_if_bet,
            CAST(COALESCE(f_fix, f_auto) AS FLOAT64) AS f_share_used,
            CAST(SAFE_DIVIDE(M_adj, C) * COALESCE(f_fix, f_auto) * NetPool_now AS NUMERIC) AS expected_return,
            CAST(SAFE_DIVIDE(M_adj, C) * COALESCE(f_fix, f_auto) * NetPool_now - (M_adj * stake_val) AS NUMERIC) AS expected_profit,
            (SAFE_DIVIDE(M_adj, C) * COALESCE(f_fix, f_auto) * NetPool_now - (M_adj * stake_val)) > 0 AS is_positive_ev
          FROM fshare
