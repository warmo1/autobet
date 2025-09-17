-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_superfecta_viability_grid`(
  num_runners INT64,
  pool_gross_other NUMERIC,
  stake_per_line NUMERIC,
  take_rate FLOAT64,
  net_rollover NUMERIC,
  include_self_in_pool BOOL,
  dividend_multiplier FLOAT64,
  f_share_override FLOAT64,
  steps INT64
)
AS
WITH cfg AS (
            SELECT
              CAST(num_runners AS INT64) AS N,
              GREATEST(num_runners * (num_runners-1) * (num_runners-2) * (num_runners-3), 0) AS C,
              CAST(pool_gross_other AS NUMERIC) AS O,
              CAST(stake_per_line AS NUMERIC) AS stake_val,
              CAST(take_rate AS FLOAT64) AS t,
              CAST(net_rollover AS NUMERIC) AS R,
              include_self_in_pool AS inc_self,
              CAST(dividend_multiplier AS FLOAT64) AS mult,
              CAST(f_share_override AS FLOAT64) AS f_fix,
              CAST(GREATEST(steps,1) AS INT64) AS K
          ), grid AS (
            SELECT N,C,O,stake_val,t,R,inc_self,mult,f_fix,K, GENERATE_ARRAY(1, K) AS arr FROM cfg
          ), grid_rows AS (
            SELECT
              N,C,O,stake_val,t,R,inc_self,mult,f_fix,K,
              SAFE_DIVIDE(i, K) AS alpha,
              CAST(ROUND(SAFE_DIVIDE(i, K) * C) AS INT64) AS L
            FROM grid, UNNEST(arr) AS i
          ), calc AS (
            SELECT
              N,C,O,stake_val,t,R,inc_self,mult,f_fix,
              alpha, L,
              (L * stake_val) AS S,
              (CASE WHEN inc_self THEN (L * stake_val) ELSE 0 END) AS S_inc,
              CASE WHEN C=0 OR (C*stake_val + O)=0 THEN 0.0 ELSE CAST( (C*stake_val) / (C*stake_val + O) AS FLOAT64 ) END AS f_auto
            FROM grid_rows
          )
          SELECT
            alpha AS coverage_frac,
            L AS lines_covered,
            S AS stake_total,
            CAST(mult * ( (1.0 - t) * (O + S_inc) + R ) AS NUMERIC) AS net_pool_if_bet,
            CAST(COALESCE(f_fix, f_auto) AS FLOAT64) AS f_share_used,
            CAST(alpha * COALESCE(f_fix, f_auto) * (mult * ( (1.0 - t) * (O + S_inc) + R )) AS NUMERIC) AS expected_return,
            CAST(alpha * COALESCE(f_fix, f_auto) * (mult * ( (1.0 - t) * (O + S_inc) + R )) - S AS NUMERIC) AS expected_profit,
            (alpha * COALESCE(f_fix, f_auto) * (mult * ( (1.0 - t) * (O + S_inc) + R )) - S) > 0 AS is_positive_ev
          FROM calc
