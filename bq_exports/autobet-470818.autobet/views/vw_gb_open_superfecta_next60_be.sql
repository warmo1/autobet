CREATE VIEW `autobet-470818.autobet.vw_gb_open_superfecta_next60_be`
AS WITH params AS (
  SELECT t, f, stake_per_line, R FROM `autobet-470818.autobet.tote_params` ORDER BY updated_ts DESC LIMIT 1
), latest AS (
  SELECT product_id, ANY_VALUE(total_gross) AS latest_gross, ANY_VALUE(total_net) AS latest_net, MAX(ts_ms) AS ts_ms
  FROM `autobet-470818.autobet.tote_pool_snapshots`
  GROUP BY product_id
)
SELECT
  v.*, prm.t, prm.f, prm.stake_per_line, prm.R,
  SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3) AS combos,
  SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line) AS S,
  CASE WHEN v.n_competitors>=4 AND prm.f>0 AND (1-prm.t)>0 THEN (
    SAFE_DIVIDE(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line), prm.f*(1-prm.t))
    - SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line)
    - SAFE_DIVIDE(prm.R, (1-prm.t))
  ) END AS O_min,
  CASE WHEN v.n_competitors>=4 AND prm.f>0 AND (1-prm.t)>0 THEN (
    (prm.f * (((1-prm.t) * ( SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line) + COALESCE(l.latest_gross, v.total_gross))) + prm.R))
    - SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line)
  ) / NULLIF(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line),0) END AS roi_current,
  CASE WHEN v.n_competitors>=4 AND prm.f>0 AND (1-prm.t)>0 THEN (
    (prm.f * (((1-prm.t) * ( SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line) + COALESCE(l.latest_gross, v.total_gross))) + prm.R))
    > SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line)
  ) END AS viable_now
FROM `autobet-470818.autobet.vw_gb_open_superfecta_next60` v
CROSS JOIN params prm
LEFT JOIN latest l USING(product_id);
