CREATE VIEW `autobet-470818.autobet.vw_sf_strengths_from_win_horse`
AS WITH
-- SUPERFECTA products and their events
sf AS (
  SELECT product_id AS sf_product_id, event_id
  FROM `autobet-470818.autobet.tote_products`
  WHERE UPPER(bet_type) = 'SUPERFECTA'
),

-- Corresponding WIN products by event
win AS (
  SELECT event_id, product_id AS win_product_id
  FROM `autobet-470818.autobet.tote_products`
  WHERE UPPER(bet_type) = 'WIN'
),

-- Pair SF with its WIN by event
paired AS (
  SELECT sf.sf_product_id, sf.event_id, win.win_product_id
  FROM sf
  JOIN win USING (event_id)
),

-- WIN product selections -> cloth numbers
sel AS (
  SELECT product_id, selection_id, number AS cloth
  FROM `autobet-470818.autobet.tote_product_selections`
),

-- Latest WIN odds already prebuilt (your table/MV)
latest_odds AS (
  SELECT product_id, selection_id, decimal_odds
  FROM `autobet-470818.autobet.latest_win_odds`
),

-- Competitors from SF event (may be empty)
comp_sf AS (
  SELECT
    e.event_id,
    SAFE_CAST(JSON_VALUE(c, '$.details.clothNumber') AS INT64) AS cloth,
    JSON_VALUE(c, '$.id') AS horse_id
  FROM `autobet-470818.autobet.tote_events` e,
       UNNEST(IFNULL(JSON_EXTRACT_ARRAY(e.competitors_json, '$'), [])) AS c
),

-- Competitors from WIN event (may be empty)
comp_win AS (
  SELECT
    e.event_id,
    SAFE_CAST(JSON_VALUE(c, '$.details.clothNumber') AS INT64) AS cloth,
    JSON_VALUE(c, '$.id') AS horse_id
  FROM `autobet-470818.autobet.tote_events` e,
       UNNEST(IFNULL(JSON_EXTRACT_ARRAY(e.competitors_json, '$'), [])) AS c
),

-- For each event, prefer SF competitors over WIN; if SF has none, WIN wins
comp_union AS (
  SELECT 'SF' AS src, event_id, cloth, horse_id FROM comp_sf
  UNION ALL
  SELECT 'WIN' AS src, event_id, cloth, horse_id FROM comp_win
),
comp_picked AS (
  SELECT event_id, cloth, horse_id
  FROM (
    SELECT
      cu.*,
      ROW_NUMBER() OVER (
        PARTITION BY cu.event_id, cu.cloth
        ORDER BY CASE cu.src WHEN 'SF' THEN 0 ELSE 1 END
      ) AS pref
    FROM comp_union cu
  )
  WHERE pref = 1
),

-- Weights based on WIN odds; map cloth→horse via picked competitors;
--       if still missing, fall back to selection_id so we never drop the row.
weights AS (
  SELECT
    p.sf_product_id AS product_id,
    p.event_id,
    COALESCE(cp.horse_id, s.selection_id) AS runner_id,
    SAFE_DIVIDE(1.0, lo.decimal_odds) AS weight
  FROM paired p
  JOIN sel    s  ON s.product_id = p.win_product_id
  JOIN latest_odds lo
       ON lo.product_id = p.win_product_id AND lo.selection_id = s.selection_id
  LEFT JOIN comp_picked cp
       ON cp.event_id = p.event_id AND cp.cloth = s.cloth
),

tot AS (
  SELECT product_id, SUM(weight) AS w_tot
  FROM weights
  GROUP BY product_id
)

SELECT
  w.product_id,
  w.event_id,
  w.runner_id,
  SAFE_DIVIDE(w.weight, NULLIF(t.w_tot, 0)) AS strength
FROM weights w
JOIN tot t USING (product_id)
WHERE SAFE_DIVIDE(w.weight, NULLIF(t.w_tot, 0)) IS NOT NULL;
