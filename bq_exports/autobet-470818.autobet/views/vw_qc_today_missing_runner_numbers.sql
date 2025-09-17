CREATE VIEW `autobet-470818.autobet.vw_qc_today_missing_runner_numbers`
AS SELECT DISTINCT p.product_id, p.event_id, p.event_name, p.venue, p.start_iso
        FROM `autobet-470818.autobet.tote_products` p
        LEFT JOIN `autobet-470818.autobet.tote_product_selections` s ON s.product_id = p.product_id
        WHERE DATE(SUBSTR(p.start_iso,1,10)) = CURRENT_DATE()
          AND s.number IS NULL;
