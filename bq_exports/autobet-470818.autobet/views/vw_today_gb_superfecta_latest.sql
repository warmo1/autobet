CREATE VIEW `autobet-470818.autobet.vw_today_gb_superfecta_latest`
AS WITH latest AS (
          SELECT product_id, MAX(ts_ms) AS ts_ms
          FROM `autobet-470818.autobet.tote_pool_snapshots`
          GROUP BY product_id
        )
        SELECT s.*
        FROM `autobet-470818.autobet.tote_pool_snapshots` s
        JOIN latest l USING(product_id, ts_ms)
        JOIN `autobet-470818.autobet.tote_products` p USING(product_id)
        LEFT JOIN `autobet-470818.autobet.tote_events` e ON e.event_id = p.event_id
        WHERE e.country = 'GB' AND DATE(SUBSTR(p.start_iso,1,10)) = CURRENT_DATE()
          AND UPPER(p.bet_type)='SUPERFECTA';
