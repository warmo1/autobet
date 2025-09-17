CREATE VIEW `autobet-470818.autobet.vw_qc_missing_bet_rules`
AS SELECT p.product_id, p.event_id, p.bet_type, p.start_iso
        FROM `autobet-470818.autobet.tote_products` p
        LEFT JOIN `autobet-470818.autobet.tote_bet_rules` r ON r.product_id = p.product_id
        WHERE r.product_id IS NULL
           OR (r.min_line IS NULL AND r.line_increment IS NULL AND r.min_bet IS NULL);
