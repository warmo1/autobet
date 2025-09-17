CREATE VIEW `autobet-470818.autobet.vw_superfecta_actual_results`
AS SELECT
    event_id,
    STRING_AGG(horse_name ORDER BY finish_pos ASC) AS actual_superfecta_order_string
FROM
    `autobet-470818.autobet.vw_horse_runs_by_name`
WHERE
    finish_pos IS NOT NULL -- Only for completed races
    AND finish_pos <= 4 -- Top 4 horses
GROUP BY
    event_id
HAVING
    COUNT(DISTINCT horse_id) = 4;
