-- Recent runs with warm-tier data must have effective_cost_usd populated.
-- effective_cost_usd = COALESCE(attributed_cost_usd, estimated_cost_usd).
-- If both are NULL, the warm-tier hook failed to write a cost estimate.
--
-- Fails if any row from the last 7 days has pipeline_state (warm-tier joined)
-- but NULL effective_cost_usd. SKIPPED runs have estimated_cost_usd = 0.0000
-- (not NULL), so they pass this test.

SELECT
    task_key,
    usage_date,
    pipeline_state,
    attributed_cost_usd,
    estimated_cost_usd,
    effective_cost_usd
FROM {{ ref('fct_workflow_costs') }}
WHERE
    usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
    AND pipeline_state IS NOT NULL
    AND effective_cost_usd IS NULL
