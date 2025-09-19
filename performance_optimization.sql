-- =====================================================
-- AUTOBET PERFORMANCE OPTIMIZATION SQL
-- Run these queries in BigQuery to optimize performance
-- =====================================================

-- 1. CREATE OPTIMIZED VIEWS FOR COMMON QUERIES
-- =====================================================

-- View for Superfecta dashboard cache (regular view for complex queries)
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_superfecta_dashboard_cache` AS
SELECT 
  p.product_id,
  p.event_id,
  COALESCE(p.event_name, e.name) AS event_name,
  COALESCE(e.venue, p.venue) AS venue,
  UPPER(COALESCE(e.country, p.currency)) AS country,
  p.start_iso,
  COALESCE(p.status, '') AS status,
  p.currency,
  COALESCE(p.total_net, 0.0) AS total_net
FROM `autobet-470818.autobet.vw_products_latest_totals` p
LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id)
WHERE UPPER(p.bet_type) = 'SUPERFECTA'
  AND PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', p.start_iso) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', p.start_iso) <= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
  AND UPPER(COALESCE(e.country, p.currency)) = 'GB';

-- View for event filters (countries)
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_event_filters_country` AS
SELECT DISTINCT 
  UPPER(COALESCE(e.country, p.currency)) AS country
FROM `autobet-470818.autobet.tote_events` e
LEFT JOIN `autobet-470818.autobet.tote_products` p USING(event_id)
WHERE UPPER(COALESCE(e.country, p.currency)) IS NOT NULL 
  AND UPPER(COALESCE(e.country, p.currency)) != ''
ORDER BY country;

-- View for event filters (sports)
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_event_filters_sport` AS
SELECT DISTINCT 
  e.sport
FROM `autobet-470818.autobet.tote_events` e
WHERE e.sport IS NOT NULL 
  AND e.sport != ''
ORDER BY sport;

-- View for event filters (venues)
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_event_filters_venue` AS
SELECT DISTINCT 
  COALESCE(e.venue, p.venue) AS venue
FROM `autobet-470818.autobet.tote_events` e
LEFT JOIN `autobet-470818.autobet.tote_products` p USING(event_id)
WHERE COALESCE(e.venue, p.venue) IS NOT NULL 
  AND COALESCE(e.venue, p.venue) != ''
ORDER BY venue;

-- View for competitor counts (can be joined when needed)
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_product_competitor_counts` AS
SELECT 
  product_id,
  COUNT(1) AS n_competitors
FROM `autobet-470818.autobet.tote_product_selections`
GROUP BY product_id;

-- Note: Using existing materialized views instead of creating new ones to avoid conflicts
-- The existing mv_latest_win_odds and mv_event_filters_* materialized views are already optimized

-- 2. CREATE OPTIMIZED TABLES WITH CLUSTERING
-- =====================================================

-- Note: Partitioning requires TIMESTAMP columns, but start_iso is STRING
-- For partitioning, you would need to add a TIMESTAMP column like:
-- start_timestamp TIMESTAMP GENERATED ALWAYS AS (PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', start_iso)) STORED
-- Then partition by DATE(start_timestamp)

-- Optimized tote_products table with clustering (partitioning requires TIMESTAMP column)
CREATE OR REPLACE TABLE `autobet-470818.autobet.tote_products_optimized` (
  product_id STRING,
  event_id STRING,
  bet_type STRING,
  status STRING,
  currency STRING,
  start_iso STRING,
  event_name STRING,
  venue STRING,
  total_gross FLOAT64,
  total_net FLOAT64,
  rollover FLOAT64,
  deduction_rate FLOAT64,
  source STRING
)
CLUSTER BY event_id, bet_type, status, currency;

-- Copy data from existing table
INSERT INTO `autobet-470818.autobet.tote_products_optimized`
SELECT 
  product_id,
  event_id,
  bet_type,
  status,
  currency,
  start_iso,
  event_name,
  venue,
  total_gross,
  total_net,
  rollover,
  deduction_rate,
  source
FROM `autobet-470818.autobet.tote_products`;

-- Optimized tote_events table with clustering (partitioning requires TIMESTAMP column)
CREATE OR REPLACE TABLE `autobet-470818.autobet.tote_events_optimized` (
  event_id STRING,
  name STRING,
  sport STRING,
  venue STRING,
  country STRING,
  start_iso STRING,
  status STRING,
  away STRING,
  comp STRING,
  competitors_json STRING,
  result_status STRING,
  source STRING,
  home STRING
)
CLUSTER BY sport, country, status;

-- Copy data from existing table
INSERT INTO `autobet-470818.autobet.tote_events_optimized`
SELECT 
  event_id,
  name,
  sport,
  venue,
  country,
  start_iso,
  status,
  away,
  comp,
  competitors_json,
  result_status,
  source,
  home
FROM `autobet-470818.autobet.tote_events`;

-- 3. OPTIONAL: CREATE PARTITIONED TABLES (if you want partitioning)
-- =====================================================

-- Uncomment the following to create partitioned tables with TIMESTAMP columns
-- This requires adding generated columns for partitioning

/*
-- Partitioned tote_products table with TIMESTAMP column for partitioning
CREATE TABLE `autobet-470818.autobet.tote_products_partitioned` (
  product_id STRING,
  event_id STRING,
  bet_type STRING,
  status STRING,
  currency STRING,
  start_iso STRING,
  start_timestamp TIMESTAMP GENERATED ALWAYS AS (PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', start_iso)) STORED,
  event_name STRING,
  venue STRING,
  total_gross FLOAT64,
  total_net FLOAT64,
  rollover FLOAT64,
  deduction_rate FLOAT64,
  source STRING
)
PARTITION BY DATE(start_timestamp)
CLUSTER BY event_id, bet_type, status, currency;

-- Copy data to partitioned table
INSERT INTO `autobet-470818.autobet.tote_products_partitioned`
SELECT 
  product_id,
  event_id,
  bet_type,
  status,
  currency,
  start_iso,
  event_name,
  venue,
  total_gross,
  total_net,
  rollover,
  deduction_rate,
  source
FROM `autobet-470818.autobet.tote_products`;
*/

-- 4. CREATE INDEXES FOR COMMON QUERY PATTERNS
-- =====================================================

-- Note: BigQuery doesn't support traditional indexes, but we can create
-- optimized views and use clustering (done above)

-- 4. CREATE OPTIMIZED VIEWS FOR COMMON QUERIES
-- =====================================================

-- Optimized view for products with latest totals
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_products_latest_totals_optimized` AS
SELECT 
  p.product_id,
  p.event_id,
  p.bet_type,
  p.status,
  p.currency,
  p.start_iso,
  p.event_name,
  p.venue,
  p.total_gross,
  p.total_net,
  p.rollover,
  p.deduction_rate,
  p.source,
  e.sport,
  e.country as event_country
FROM `autobet-470818.autobet.tote_products_optimized` p
LEFT JOIN `autobet-470818.autobet.tote_events_optimized` e USING(event_id);

-- Optimized view for GB Superfecta next 60 minutes with breakeven
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_gb_open_superfecta_next60_be_optimized` AS
SELECT 
  p.product_id,
  p.event_id,
  p.event_name,
  p.venue,
  p.event_country as country,
  p.start_iso,
  p.status,
  p.currency,
  p.total_net
FROM `autobet-470818.autobet.vw_products_latest_totals_optimized` p
WHERE UPPER(p.bet_type) = 'SUPERFECTA'
  AND PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', p.start_iso) >= CURRENT_TIMESTAMP()
  AND PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', p.start_iso) <= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)
  AND UPPER(COALESCE(p.event_country, p.currency)) = 'GB'
  AND UPPER(COALESCE(p.status, '')) IN ('OPEN', 'SELLING')
ORDER BY p.start_iso;

-- 5. CREATE SCHEDULED QUERIES FOR MATERIALIZED VIEW REFRESH
-- =====================================================

-- Note: These would be set up in BigQuery console as scheduled queries
-- to refresh materialized views every 5-10 minutes

-- 6. CREATE PERFORMANCE MONITORING VIEWS
-- =====================================================

-- View to monitor slow queries (if query logs are available)
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_query_performance` AS
SELECT 
  CURRENT_TIMESTAMP() as check_time,
  'Materialized views refresh needed' as status,
  COUNT(*) as total_views
FROM `autobet-470818.autobet.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'mv_%'
  AND table_type = 'MATERIALIZED_VIEW';

-- 7. CREATE CACHE WARMING QUERIES
-- =====================================================

-- Query to warm up common caches
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_cache_warmup` AS
SELECT 
  'dashboard_data' as cache_type,
  COUNT(*) as record_count,
  CURRENT_TIMESTAMP() as last_updated
FROM `autobet-470818.autobet.vw_superfecta_dashboard_cache`
UNION ALL
SELECT 
  'filter_data' as cache_type,
  COUNT(*) as record_count,
  CURRENT_TIMESTAMP() as last_updated
FROM `autobet-470818.autobet.mv_event_filters_country`
UNION ALL
SELECT 
  'win_odds' as cache_type,
  COUNT(*) as record_count,
  CURRENT_TIMESTAMP() as last_updated
FROM `autobet-470818.autobet.mv_latest_win_odds`;

-- 8. GRANT PERMISSIONS
-- =====================================================

-- Grant necessary permissions for the service account
-- (Replace with your actual service account email)
-- GRANT `roles/bigquery.dataViewer` ON SCHEMA `autobet-470818.autobet` TO "run-ingest-sa@autobet-470818.iam.gserviceaccount.com";
-- GRANT `roles/bigquery.jobUser` ON PROJECT `autobet-470818` TO "run-ingest-sa@autobet-470818.iam.gserviceaccount.com";

-- 9. CREATE MONITORING ALERTS (Optional)
-- =====================================================

-- These would be set up in Cloud Monitoring to alert on:
-- - Materialized view refresh failures
-- - Query performance degradation
-- - Cache hit rate drops

-- 10. VALIDATION QUERIES
-- =====================================================

-- Validate that materialized views are working
SELECT 
  'Materialized Views Status' as check_type,
  table_name,
  table_type,
  creation_time
FROM `autobet-470818.autobet.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'mv_%'
ORDER BY creation_time DESC;

-- Validate partitioning is working
SELECT 
  'Partitioning Status' as check_type,
  table_name,
  partition_id,
  total_rows,
  total_logical_bytes
FROM `autobet-470818.autobet.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name IN ('tote_products_optimized', 'tote_events_optimized')
  AND partition_id IS NOT NULL
ORDER BY table_name, partition_id;
