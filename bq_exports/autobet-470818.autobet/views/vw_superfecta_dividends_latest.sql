CREATE VIEW `autobet-470818.autobet.vw_superfecta_dividends_latest`
AS SELECT
          product_id,
          selection,
          ARRAY_AGG(dividend ORDER BY ts DESC LIMIT 1)[OFFSET(0)] AS dividend,
          MAX(ts) AS ts
        FROM `autobet-470818.autobet.tote_product_dividends`
        GROUP BY product_id, selection;
