CREATE VIEW `autobet-470818.autobet.vw_superfecta_training`
AS SELECT
          p.event_id,
          p.product_id,
          DATE(SUBSTR(COALESCE(te.start_iso, p.start_iso),1,10)) AS event_date,
          COALESCE(te.venue, p.venue) AS venue,
          te.country,
          p.total_net,
          rc.going,
          rc.weather_temp_c,
          rc.weather_wind_kph,
          rc.weather_precip_mm,
          r.horse_id,
          r.cloth_number,
          r.finish_pos,
          r.status
        FROM `autobet-470818.autobet.tote_products` p
        JOIN `autobet-470818.autobet.hr_horse_runs` r ON r.event_id = p.event_id
        LEFT JOIN `autobet-470818.autobet.race_conditions` rc ON rc.event_id = p.event_id
        LEFT JOIN `autobet-470818.autobet.tote_events` te ON te.event_id = p.event_id
        WHERE UPPER(p.bet_type) = 'SUPERFECTA' AND r.finish_pos IS NOT NULL;
