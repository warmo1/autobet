-- Routine type: TABLE FUNCTION
CREATE OR REPLACE TABLE FUNCTION `autobet-470818.autobet.tf_sf_products`(
  sel_date DATE,
  sel_country STRING,
  sel_status STRING,
  limit_n INT64
)
AS
SELECT
    p.product_id, p.event_id, p.event_name,
    COALESCE(e.venue, p.venue) AS venue,
    e.country,
    COALESCE(p.status,'') AS status,
    p.start_iso,
    p.total_net, p.total_gross
  FROM `autobet-470818.autobet.tote_products` p
  LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id)
  WHERE UPPER(p.bet_type) = 'SUPERFECTA'
    AND (sel_date    IS NULL OR DATE(SUBSTR(p.start_iso,1,10)) = sel_date)
    AND (sel_country IS NULL OR sel_country='' OR UPPER(COALESCE(e.country,'')) = UPPER(sel_country))
    AND (sel_status  IS NULL OR sel_status ='' OR UPPER(COALESCE(p.status,''))  = UPPER(sel_status))
  QUALIFY ROW_NUMBER() OVER (ORDER BY start_iso) <= limit_n
