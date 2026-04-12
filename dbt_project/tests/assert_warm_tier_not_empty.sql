-- Warm-tier table (workflow_cost_live) must have rows after dbt build.
-- The post-hook prunes old rows but must preserve rows that lack billing
-- data (attributed_cost_usd IS NULL). If this test fails, the post-hook
-- is too aggressive — it's deleting warm-tier rows before billing catches up.
--
-- This test runs AFTER the post-hook (dbt tests run after model + hooks).
-- Returns rows if the warm tier is completely empty, which should never
-- happen if pipelines ran within the last 24 hours.

SELECT 1 AS violation
WHERE (
    SELECT COUNT(*)
    FROM {{ source('observability', 'workflow_cost_live') }}
    WHERE state != 'RUNNING'
) = 0
AND (
    -- Only fail if there ARE recent runs in the fact table that should
    -- have preserved warm-tier rows (i.e., runs without billing data).
    SELECT COUNT(*)
    FROM {{ this }}
    WHERE attributed_cost_usd IS NULL
      AND usage_date >= CURRENT_DATE - INTERVAL 3 DAYS
) > 0
