-- Upcoming 60m GB superfecta overview (backend-enriched view if present)
SELECT *
FROM `${BQ_PROJECT}.${BQ_DATASET}.vw_gb_open_superfecta_next60_be`
ORDER BY start_iso
LIMIT 200;

