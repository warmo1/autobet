CREATE VIEW `autobet-470818.autobet.vw_products_coverage_issues`
AS SELECT *
        FROM `autobet-470818.autobet.vw_products_coverage`
        WHERE no_selections OR missing_event OR missing_start_iso;
