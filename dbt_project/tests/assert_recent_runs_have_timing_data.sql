-- Recent pipeline runs (last 7 days) with warm-tier data must have timing
-- columns populated. This catches regressions where the tasks-driving-table
-- pattern is reverted to billing-driven (which gates timing on ~1 day lag).
--
-- Fails if any row from the last 7 days has pipeline_state (warm-tier joined)
-- but NULL execution_duration_seconds (cold-tier timing from lakeflow).
-- This combination should be impossible: if lakeflow has the task row (which
-- is the driving table), execution_duration_seconds is always populated.

SELECT
    task_key,
    usage_date,
    pipeline_state,
    execution_duration_seconds
FROM {{ ref('fct_workflow_costs') }}
WHERE
    usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
    AND pipeline_state IS NOT NULL
    AND execution_duration_seconds IS NULL
