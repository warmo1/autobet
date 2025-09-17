CREATE VIEW `autobet-470818.autobet.vw_today_gb_events`
AS SELECT
          e.event_id,
          e.name,
          e.status,
          e.start_iso,
          e.venue,
          e.country,
          ARRAY_LENGTH(JSON_EXTRACT_ARRAY(e.competitors_json, '$')) AS n_competitors
        FROM `autobet-470818.autobet.tote_events` e
        WHERE e.country = 'GB' AND DATE(SUBSTR(e.start_iso,1,10)) = CURRENT_DATE();
