CREATE VIEW `autobet-470818.autobet.vw_qc_probable_odds_coverage`
AS WITH sel AS (
SELECT product_id, COUNT(1) AS sel_count
FROM autobet-470818.autobet.tote_product_selections
GROUP BY product_id
), odded AS (
SELECT product_id, COUNT(DISTINCT selection_id) AS with_odds
FROM autobet-470818.autobet.vw_tote_probable_odds
GROUP BY product_id
)
SELECT
p.product_id,
p.event_id,
COALESCE(o.with_odds, 0) AS with_odds,
COALESCE(s.sel_count, 0) AS selections,
SAFE_DIVIDE(COALESCE(o.with_odds, 0), NULLIF(s.sel_count, 0)) AS coverage
FROM autobet-470818.autobet.tote_products p
LEFT JOIN sel s USING (product_id)
LEFT JOIN odded o USING (product_id);
