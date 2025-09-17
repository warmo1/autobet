CREATE VIEW `autobet-470818.autobet.vw_today_gb_superfecta`
AS SELECT
          p.product_id,
          p.event_id,
          p.event_name,
          COALESCE(te.venue, p.venue) AS venue,
          te.country,
          p.start_iso,
          p.status,
          p.currency,
          p.total_gross,
          p.total_net,
          ARRAY_LENGTH(JSON_EXTRACT_ARRAY(te.competitors_json, '$')) AS n_competitors
        FROM `autobet-470818.autobet.tote_products` p
        LEFT JOIN `autobet-470818.autobet.tote_events` te USING(event_id)
        WHERE UPPER(p.bet_type) = 'SUPERFECTA' AND te.country = 'GB' AND DATE(SUBSTR(p.start_iso,1,10)) = CURRENT_DATE();
