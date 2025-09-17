CREATE VIEW `autobet-470818.autobet.vw_tote_probable_history`
AS WITH exploded AS (
SELECT
JSON_EXTRACT_SCALAR(prod, '$.id') AS product_id,
r.fetched_ts AS ts_ms,
COALESCE(
JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(line, '$.legs')[SAFE_OFFSET(0)], '$.lineSelections[0].selectionId'),
JSON_EXTRACT_SCALAR(line, '$.legs.lineSelections[0].selectionId')
) AS selection_id,
SAFE_CAST(
COALESCE(
JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(line, '$.odds')[SAFE_OFFSET(0)], '$.decimal'),
JSON_EXTRACT_SCALAR(line, '$.odds.decimal')
) AS FLOAT64
) AS decimal_odds
FROM autobet-470818.autobet.raw_tote_probable_odds r,
UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod,
UNNEST(JSON_EXTRACT_ARRAY(prod, '$.lines.nodes')) AS line
)
SELECT
e.product_id,
COALESCE(s.selection_id, e.selection_id) AS selection_id,
s.number AS cloth_number,
e.decimal_odds,
e.ts_ms
FROM exploded e
LEFT JOIN autobet-470818.autobet.tote_product_selections s
ON s.selection_id = e.selection_id AND s.product_id = e.product_id
WHERE e.product_id IS NOT NULL AND e.selection_id IS NOT NULL AND e.decimal_odds IS NOT NULL;
