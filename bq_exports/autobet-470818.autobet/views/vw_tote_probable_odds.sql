CREATE VIEW `autobet-470818.autobet.vw_tote_probable_odds`
AS WITH exploded AS (
SELECT
JSON_EXTRACT_SCALAR(prod, '$.id') AS product_id,
r.fetched_ts,
-- selectionId from legs (array first, fallback to object)
COALESCE(
JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(line, '$.legs')[SAFE_OFFSET(0)], '$.lineSelections[0].selectionId'),
JSON_EXTRACT_SCALAR(line, '$.legs.lineSelections[0].selectionId')
) AS selection_id,
-- odds from array first element, fallback to object
SAFE_CAST(
COALESCE(
JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(line, '$.odds')[SAFE_OFFSET(0)], '$.decimal'),
JSON_EXTRACT_SCALAR(line, '$.odds.decimal')
) AS FLOAT64
) AS decimal_odds
FROM autobet-470818.autobet.raw_tote_probable_odds r,
UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod,
UNNEST(JSON_EXTRACT_ARRAY(prod, '$.lines.nodes')) AS line
), latest AS (
SELECT
product_id,
selection_id,
ARRAY_AGG(decimal_odds ORDER BY fetched_ts DESC LIMIT 1)[OFFSET(0)] AS decimal_odds,
MAX(fetched_ts) AS ts_ms
FROM exploded
WHERE product_id IS NOT NULL AND selection_id IS NOT NULL
GROUP BY product_id, selection_id
)
SELECT
l.product_id,
COALESCE(s.selection_id, l.selection_id) AS selection_id,
s.number AS cloth_number,
l.decimal_odds,
l.ts_ms
FROM latest l
LEFT JOIN autobet-470818.autobet.tote_product_selections s
ON s.selection_id = l.selection_id AND s.product_id = l.product_id;
