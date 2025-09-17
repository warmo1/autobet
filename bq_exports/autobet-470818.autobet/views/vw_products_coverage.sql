CREATE VIEW `autobet-470818.autobet.vw_products_coverage`
AS SELECT
          p.product_id,
          p.event_id,
          UPPER(p.bet_type) AS bet_type,
          COALESCE(p.status, '') AS status,
          p.currency,
          p.start_iso,
          COALESCE(te.name, p.event_name) AS event_name,
          COALESCE(te.venue, p.venue) AS venue,
          COUNT(s.selection_id) AS n_selections,
          te.event_id IS NOT NULL AS event_present,
          (COUNT(s.selection_id) = 0) AS no_selections,
          (te.event_id IS NULL) AS missing_event,
          (p.start_iso IS NULL OR p.start_iso = '') AS missing_start_iso
        FROM `autobet-470818.autobet.tote_products` p
        LEFT JOIN `autobet-470818.autobet.tote_events` te ON te.event_id = p.event_id
        LEFT JOIN `autobet-470818.autobet.tote_product_selections` s ON s.product_id = p.product_id
        GROUP BY p.product_id, p.event_id, bet_type, status, p.currency, p.start_iso, event_name, venue, event_present, missing_event, missing_start_iso;
