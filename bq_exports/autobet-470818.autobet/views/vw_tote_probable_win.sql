CREATE VIEW `autobet-470818.autobet.vw_tote_probable_win`
AS WITH base AS (
          SELECT
            s.product_id,
            s.selection_id,
            s.number,
            s.competitor,
            s.total_units,
            s.units_ts_ms,
            ANY_VALUE(p.event_id) AS event_id,
            ANY_VALUE(p.event_name) AS event_name,
            ANY_VALUE(COALESCE(te.venue, p.venue)) AS venue,
            ANY_VALUE(p.start_iso) AS start_iso
          FROM `autobet-470818.autobet.tote_product_selections` s
          LEFT JOIN `autobet-470818.autobet.tote_products` p USING(product_id)
          LEFT JOIN `autobet-470818.autobet.tote_events` te USING(event_id)
          GROUP BY s.product_id, s.selection_id, s.number, s.competitor, s.total_units, s.units_ts_ms
        ), agg AS (
          SELECT product_id, SUM(COALESCE(total_units,0)) AS sum_units
          FROM base
          GROUP BY product_id
        )
        SELECT
          b.product_id,
          b.selection_id,
          b.number,
          b.competitor,
          b.event_id,
          b.event_name,
          b.venue,
          b.start_iso,
          b.total_units,
          b.units_ts_ms,
          SAFE_DIVIDE(COALESCE(b.total_units,0), NULLIF(a.sum_units,0)) AS pct_units
        FROM base b
        JOIN agg a USING(product_id);
