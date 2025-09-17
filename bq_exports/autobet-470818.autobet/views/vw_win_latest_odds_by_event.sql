CREATE VIEW `autobet-470818.autobet.vw_win_latest_odds_by_event`
AS WITH win_odds AS (
  SELECT
    p_win.event_id,
    s.number AS cloth_number,
    CAST(o.decimal_odds AS FLOAT64) AS decimal_odds,
    o.ts_ms,
    ROW_NUMBER() OVER (
      PARTITION BY p_win.event_id, s.number
      ORDER BY o.ts_ms DESC
    ) AS rn
  FROM `autobet-470818.autobet.tote_products` p_win
  JOIN `autobet-470818.autobet.vw_tote_probable_odds` o
    ON o.product_id = p_win.product_id
  JOIN `autobet-470818.autobet.tote_product_selections` s
    ON s.product_id = p_win.product_id
   AND s.selection_id = o.selection_id
  WHERE UPPER(p_win.bet_type) = 'WIN'
    AND s.leg_index = 1
)
SELECT
  event_id,
  SAFE_CAST(cloth_number AS INT64) AS cloth_number,
  decimal_odds
FROM win_odds
WHERE rn = 1 AND decimal_odds IS NOT NULL AND decimal_odds > 0;
