CREATE VIEW `autobet-470818.autobet.vw_horse_runs_by_name`
AS WITH ep AS (
          SELECT event_id,
                 ANY_VALUE(event_name) AS event_name,
                 ANY_VALUE(venue) AS venue,
                 ANY_VALUE(start_iso) AS start_iso
          FROM `autobet-470818.autobet.tote_products`
          GROUP BY event_id
        )
        SELECT
          h.name AS horse_name,
          r.horse_id AS horse_id,
          r.event_id AS event_id,
          DATE(COALESCE(SUBSTR(te.start_iso,1,10), SUBSTR(ep.start_iso,1,10))) AS event_date,
          COALESCE(te.venue, ep.venue) AS venue,
          te.country AS country,
          ep.event_name AS race_name,
          r.cloth_number AS cloth_number,
          r.finish_pos AS finish_pos,
          r.status AS status
        FROM `autobet-470818.autobet.hr_horse_runs` r
        JOIN `autobet-470818.autobet.hr_horses` h ON h.horse_id = r.horse_id
        LEFT JOIN `autobet-470818.autobet.tote_events` te ON te.event_id = r.event_id
        LEFT JOIN ep ON ep.event_id = r.event_id;
