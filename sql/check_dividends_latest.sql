-- Latest dividend per selection (uses view created by ensure_views)
SELECT *
FROM `${BQ_PROJECT}.${BQ_DATASET}.vw_superfecta_dividends_latest`
ORDER BY ts DESC
LIMIT 200;

