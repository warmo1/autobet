CREATE VIEW `autobet-470818.autobet.vw_probable_odds_by_event`
AS SELECT
p.event_id,
o.product_id AS win_product_id,
o.cloth_number,
o.selection_id,
o.decimal_odds,
o.ts_ms
FROM autobet-470818.autobet.vw_tote_probable_odds o
JOIN autobet-470818.autobet.tote_products p
ON p.product_id = o.product_id
WHERE UPPER(p.bet_type)='WIN';
