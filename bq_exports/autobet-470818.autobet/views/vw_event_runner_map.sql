CREATE VIEW `autobet-470818.autobet.vw_event_runner_map`
AS WITH rn AS (
  SELECT
    r.event_id,
    r.horse_id,
    r.cloth_number,
    UPPER(REGEXP_REPLACE(h.name, r'[^A-Z0-9]', '')) AS horse_name_norm
  FROM `autobet-470818.autobet.hr_horse_runs` r
  LEFT JOIN `autobet-470818.autobet.hr_horses` h
    ON h.horse_id = r.horse_id
)
SELECT * FROM rn;
