-- Events for today
SELECT
  event_id,
  name,
  venue,
  country,
  start_iso,
  status,
  result_status
FROM `${BQ_PROJECT}.${BQ_DATASET}.tote_events`
WHERE DATE(SUBSTR(start_iso,1,10)) = CURRENT_DATE()
ORDER BY start_iso
LIMIT 200;

