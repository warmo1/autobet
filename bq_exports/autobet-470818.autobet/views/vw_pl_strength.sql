CREATE VIEW `autobet-470818.autobet.vw_pl_strength`
AS SELECT
        product_id,
        selection_id,
        1.0/CAST(decimal_odds AS FLOAT64) as strength
        FROM `autobet-470818.autobet.vw_tote_probable_odds`
        WHERE decimal_odds IS NOT NULL AND CAST(decimal_odds AS FLOAT64) > 0;
