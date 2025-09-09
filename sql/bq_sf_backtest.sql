-- Historical backtesting for a Superfecta weighting model.
-- This script evaluates the Plackett-Luce model (`tf_superfecta_perms`)
-- against historical race results and dividends.

-- ====================================================================
-- Configuration
-- ====================================================================
DECLARE backtest_start_date DATE DEFAULT '2023-01-01';
DECLARE backtest_end_date DATE DEFAULT CURRENT_DATE();
-- The `top_n` parameter for the model: how many top runners to consider for permutations.
-- A higher number increases accuracy but also computational cost.
DECLARE model_top_n INT64 DEFAULT 10;

-- ====================================================================
-- Backtesting Logic
-- =================================================_==================

-- Step 1: Identify historical Superfecta products with resulted dividends.
CREATE OR REPLACE TEMP TABLE BacktestProducts AS
SELECT
  p.product_id,
  p.event_id,
  d.selection AS winning_selection,
  d.dividend
FROM `autobet-470818.autobet.tote_products` p
JOIN `autobet-470818.autobet.tote_product_dividends` d ON p.product_id = d.product_id
WHERE UPPER(p.bet_type) = 'SUPERFECTA'
  AND p.status IN ('RESULTED', 'CLOSED')
  AND d.dividend > 0
  AND DATE(SUBSTR(p.start_iso, 1, 10)) BETWEEN backtest_start_date AND backtest_end_date
  -- Ensure the dividend is for a valid Superfecta combination.
  AND REGEXP_CONTAINS(d.selection, r'^\d+-\d+-\d+-\d+$');

-- Step 2: Get the actual winning horse_ids for each event from the results table.
-- This is more reliable than parsing the dividend string.
CREATE OR REPLACE TEMP TABLE ActualWinners AS
SELECT
  r.event_id,
  STRING_AGG(r.horse_id ORDER BY r.finish_pos ASC) AS winning_horse_ids
FROM `autobet-470818.autobet.hr_horse_runs` r
WHERE r.event_id IN (SELECT event_id FROM BacktestProducts)
  AND r.finish_pos BETWEEN 1 AND 4
GROUP BY r.event_id
HAVING COUNT(r.horse_id) = 4;

-- Step 3: Run the Plackett-Luce model for each product and rank the predicted permutations.
-- This step inlines the logic from the `tf_superfecta_perms` function to avoid
-- performance issues and errors with correlated subqueries when calling table functions row-by-row.
CREATE OR REPLACE TEMP TABLE ModelPredictions AS
WITH
  -- Get all runners and their model-derived strengths for the products in our backtest set.
  AllRunners AS (
    SELECT
      s.product_id,
      s.runner_id,
      s.strength,
      ROW_NUMBER() OVER (PARTITION BY s.product_id ORDER BY s.strength DESC) AS rnk
    FROM `autobet-470818.autobet.vw_superfecta_runner_strength` s
    WHERE s.product_id IN (SELECT product_id FROM BacktestProducts)
  ),
  -- Filter to the top N runners per product to manage permutation complexity.
  TopRunners AS (
    SELECT * FROM AllRunners WHERE rnk <= model_top_n
  ),
  -- Pre-calculate the sum of strengths for the top N runners in each product.
  TotalStrength AS (
    SELECT product_id, SUM(strength) AS sum_s FROM TopRunners GROUP BY product_id
  ),
  -- Generate all 1-2-3-4 permutations from the top N runners and calculate their Plackett-Luce probability.
  Permutations AS (
    SELECT
      tr1.product_id,
      tr1.runner_id AS h1, tr2.runner_id AS h2, tr3.runner_id AS h3, tr4.runner_id AS h4,
      -- Plackett-Luce probability calculation
      SAFE_DIVIDE(tr1.strength, t.sum_s) *
      SAFE_DIVIDE(tr2.strength, (t.sum_s - tr1.strength)) *
      SAFE_DIVIDE(tr3.strength, (t.sum_s - tr1.strength - tr2.strength)) *
      SAFE_DIVIDE(tr4.strength, (t.sum_s - tr1.strength - tr2.strength - tr3.strength)) AS probability
    FROM TopRunners tr1
    JOIN TopRunners tr2 ON tr1.product_id = tr2.product_id AND tr2.runner_id != tr1.runner_id
    JOIN TopRunners tr3 ON tr1.product_id = tr3.product_id AND tr3.runner_id NOT IN (tr1.runner_id, tr2.runner_id)
    JOIN TopRunners tr4 ON tr1.product_id = tr4.product_id AND tr4.runner_id NOT IN (tr1.runner_id, tr2.runner_id, tr3.runner_id)
    JOIN TotalStrength t ON t.product_id = tr1.product_id
  )
-- Rank the generated permutations by their calculated probability.
SELECT *,
  CONCAT(h1, '-', h2, '-', h3, '-', h4) AS predicted_horse_ids,
  ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY probability DESC) AS pred_rank
FROM Permutations;

-- Step 4: Join everything to create a final results table.
-- This table can be persisted for deeper analysis if needed.
CREATE OR REPLACE TABLE `autobet-470818.autobet.superfecta_backtest_results` AS
SELECT
  bp.product_id,
  bp.event_id,
  e.start_iso,
  e.venue,
  aw.winning_horse_ids,
  bp.winning_selection,
  bp.dividend,
  mp.pred_rank AS winner_rank,
  mp.probability AS winner_probability,
  (SELECT COUNT(1) FROM `autobet-470818.autobet.tote_product_selections` s WHERE s.product_id = bp.product_id AND s.leg_index = 1) AS num_runners,
  (SELECT COUNT(1) FROM ModelPredictions WHERE product_id = bp.product_id) AS total_perms_considered
FROM BacktestProducts bp
JOIN `autobet-470818.autobet.tote_events` e ON bp.event_id = e.event_id
JOIN ActualWinners aw ON bp.event_id = aw.event_id
LEFT JOIN ModelPredictions mp ON bp.product_id = mp.product_id AND mp.predicted_horse_ids = aw.winning_horse_ids;

-- ====================================================================
-- Results & Summary
-- ====================================================================

-- Show some recent detailed results
SELECT * FROM `autobet-470818.autobet.superfecta_backtest_results` ORDER BY start_iso DESC LIMIT 100;

-- Generate a summary of the backtest performance
SELECT
  'Overall' AS summary_group,
  COUNT(product_id) AS total_races,
  -- Hit Rate: How often was the model's top prediction the actual winner?
  SAFE_DIVIDE(COUNTIF(winner_rank = 1), COUNT(product_id)) AS hit_rate_top_1,
  -- Coverage: How often was the winner in the top 10 predictions?
  SAFE_DIVIDE(COUNTIF(winner_rank <= 10), COUNT(product_id)) AS hit_rate_top_10,
  -- ROI: What was the return on investment if we bet £1 on the top prediction each time?
  -- (Total Dividends from Hits - Total Bets) / Total Bets
  SAFE_DIVIDE(
    SUM(IF(winner_rank = 1, dividend, 0)) - COUNT(product_id),
    COUNT(product_id)
  ) AS roi_top_1_bet,
  -- Average dividend when we hit the top prediction
  AVG(IF(winner_rank = 1, dividend, NULL)) AS avg_dividend_on_hit
FROM `autobet-470818.autobet.superfecta_backtest_results`;