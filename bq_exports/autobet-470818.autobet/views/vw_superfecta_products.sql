CREATE VIEW `autobet-470818.autobet.vw_superfecta_products`
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
          p.total_net
        FROM `autobet-470818.autobet.tote_products` p
        LEFT JOIN `autobet-470818.autobet.tote_events` te USING(event_id)
        WHERE UPPER(p.bet_type) = 'SUPERFECTA';
