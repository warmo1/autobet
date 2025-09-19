CREATE VIEW `autobet-470818.autobet.vw_today_gb_superfecta_be`
AS WITH params AS (
          SELECT t, f, stake_per_line, R FROM `autobet-470818.autobet.tote_params`
          ORDER BY updated_ts DESC LIMIT 1
        )
        SELECT
          v.product_id,
          v.event_id,
          v.event_name,
          v.venue,
          v.country,
          v.start_iso,
          v.status,
          v.currency,
          v.total_gross,
          v.total_net,
          v.rollover,
          v.n_competitors,
          -- permutations C = N*(N-1)*(N-2)*(N-3)
          SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3) AS combos,
          -- S = C * stake_per_line
          SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line) AS S,
          prm.t,
          prm.f,
          prm.R,
          -- O_min = S/(f*(1-t)) - S - R/(1-t)
          CASE
            WHEN v.n_competitors >= 4 AND prm.f > 0 AND (1-prm.t) > 0 THEN (
              (SAFE_DIVIDE(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line), (prm.f * (1-prm.t))))
              - (SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line))
              - (SAFE_DIVIDE(prm.R, (1-prm.t)))
            ) ELSE NULL END AS O_min,
          -- ROI at current pool using latest snapshot gross as O
          CASE
            WHEN v.n_competitors >= 4 AND prm.f > 0 AND (1-prm.t) > 0 AND s.latest_gross IS NOT NULL THEN (
              (prm.f * (((1-prm.t) * ( (SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line)) + s.latest_gross)) + prm.R))
              - (SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line))
            ) / NULLIF(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line), 0)
            ELSE NULL END AS roi_current,
          CASE
            WHEN v.n_competitors >= 4 AND prm.f > 0 AND (1-prm.t) > 0 AND s.latest_gross IS NOT NULL THEN (
              (prm.f * (((1-prm.t) * ( (SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line)) + s.latest_gross)) + prm.R))
              > (SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line))
            ) ELSE NULL END AS viable_now
        FROM `autobet-470818.autobet.vw_today_gb_superfecta` v
        LEFT JOIN (
          SELECT product_id,
                 ANY_VALUE(total_gross) AS latest_gross,
                 ANY_VALUE(total_net) AS latest_net,
                 MAX(ts_ms) AS ts_ms
          FROM `autobet-470818.autobet.tote_pool_snapshots`
          GROUP BY product_id
        ) s ON s.product_id = v.product_id
        CROSS JOIN params prm;
