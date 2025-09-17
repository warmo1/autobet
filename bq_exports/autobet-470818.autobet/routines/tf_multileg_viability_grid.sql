-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_multileg_viability_grid`(
  leg_lines ARRAY<INT64>,
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
              CAST(pool_gross_other AS NUMERIC) AS O,
              CAST(stake_per_line AS NUMERIC) AS l,
              CAST(take_rate AS FLOAT64) AS t,
              CAST(net_rollover AS NUMERIC) AS R,
              include_self_in_pool AS inc_self,
              CAST(dividend_multiplier AS FLOAT64) AS m,
              CAST(f_share_override AS FLOAT64) AS f_fix,
              CAST(GREATEST(steps,1) AS INT64) AS S
          ), tot AS (
            SELECT CAST(ROUND(EXP(SUM(LN(CAST(x AS FLOAT64))))) AS INT64) AS C FROM UNNEST(leg_lines) AS x
          ), grid AS (
            SELECT c.O, c.l, c.t, c.R, c.inc_self, c.m, c.f_fix, c.S, (SELECT C FROM tot) AS C,
                   GENERATE_ARRAY(1, c.S) AS arr
            FROM cfg c
          ), p_rows AS (
            SELECT O, l, t, R, inc_self, m, f_fix, S, C,
                   SAFE_DIVIDE(i, S) AS alpha,
                   CAST(ROUND(SAFE_DIVIDE(i, S) * C) AS INT64) AS lines_cov
            FROM grid, UNNEST(arr) AS i
          )
          SELECT
            alpha AS coverage_frac,
            lines_cov AS lines_covered,
            CAST(lines_cov * l AS NUMERIC) AS stake_total,
            CAST(m * ((1.0 - t) * (O + (CASE WHEN inc_self THEN (lines_cov * l) ELSE 0 END)) + R) AS NUMERIC) AS net_pool_if_bet,
            CAST(COALESCE(f_fix, (CASE WHEN C=0 OR (C*l + O)=0 THEN 0.0 ELSE CAST( (C*l) / (C*l + O) AS FLOAT64 ) END)) AS FLOAT64) AS f_share_used,
            CAST(alpha * COALESCE(f_fix, (CASE WHEN C=0 OR (C*l + O)=0 THEN 0.0 ELSE CAST( (C*l) / (C*l + O) AS FLOAT64 ) END)) * (m * ((1.0 - t) * (O + (CASE WHEN inc_self THEN (lines_cov * l) ELSE 0 END)) + R)) AS NUMERIC) AS expected_return,
            CAST(alpha * COALESCE(f_fix, (CASE WHEN C=0 OR (C*l + O)=0 THEN 0.0 ELSE CAST( (C*l) / (C*l + O) AS FLOAT64 ) END)) * (m * ((1.0 - t) * (O + (CASE WHEN inc_self THEN (lines_cov * l) ELSE 0 END)) + R)) - (lines_cov * l) AS NUMERIC) AS expected_profit,
            (alpha * COALESCE(f_fix, (CASE WHEN C=0 OR (C*l + O)=0 THEN 0.0 ELSE CAST( (C*l) / (C*l + O) AS FLOAT64 ) END)) * (m * ((1.0 - t) * (O + (CASE WHEN inc_self THEN (lines_cov * l) ELSE 0 END)) + R)) - (lines_cov * l)) > 0 AS is_positive_ev
          FROM p_rows
