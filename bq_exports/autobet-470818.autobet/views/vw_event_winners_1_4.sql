CREATE VIEW `autobet-470818.autobet.vw_event_winners_1_4`
AS WITH hr AS (
  SELECT
    event_id,
    MAX(IF(finish_pos=1, horse_id, NULL)) AS h1,
    MAX(IF(finish_pos=2, horse_id, NULL)) AS h2,
    MAX(IF(finish_pos=3, horse_id, NULL)) AS h3,
    MAX(IF(finish_pos=4, horse_id, NULL)) AS h4
  FROM `autobet-470818.autobet.hr_horse_runs`
  WHERE finish_pos BETWEEN 1 AND 4
  GROUP BY event_id
  HAVING COUNTIF(finish_pos=1)=1
     AND COUNTIF(finish_pos=2)=1
     AND COUNTIF(finish_pos=3)=1
     AND COUNTIF(finish_pos=4)=1
)
SELECT * FROM hr;
