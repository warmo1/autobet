CREATE VIEW `autobet-470818.autobet.vw_qc_today_gb_sf_missing_snapshots`
AS SELECT v.product_id, v.event_id, v.event_name, v.venue, v.start_iso
        FROM `autobet-470818.autobet.vw_today_gb_superfecta` v
        LEFT JOIN (
          SELECT DISTINCT product_id FROM `autobet-470818.autobet.tote_pool_snapshots`
        ) s USING(product_id)
        WHERE s.product_id IS NULL;
