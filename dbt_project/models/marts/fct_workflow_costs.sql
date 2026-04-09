{{ config(
    materialized='table',
    liquid_clustered_by=['task_key', 'usage_date'],
    post_hook=[
        "DELETE FROM {{ this.database }}.observability.workflow_cost_live WHERE state != 'RUNNING' AND ended_at IS NOT NULL AND ended_at < (SELECT COALESCE(MAX(usage_date), DATE '1970-01-01') + INTERVAL 1 DAY FROM {{ this }})",
        "DELETE FROM {{ this.database }}.observability.workflow_cost_live WHERE state = 'RUNNING' AND started_at < CURRENT_TIMESTAMP - INTERVAL 24 HOURS"
    ]
) }}
-- fct_workflow_costs.sql
-- Gold-layer workflow cost attribution from Databricks system tables.
--
-- Joins billing usage with list prices and attributes cost per-task
-- proportionally by execution duration within each job run.
-- 90-day rolling window refreshed daily.
--
-- Warm-tier enrichment: LEFT JOINs workflow_cost_live (written by
-- CostEstimateHook) to capture lifecycle fields (duration, entity_count,
-- row_count, state, estimated_cost_usd) before the post-hook prunes them.
--
-- Post-hook cleanup removes redundant warm-tier rows from workflow_cost_live.
-- COALESCE sentinel: if table is empty (first build), threshold becomes
-- 1970-01-02 — no legitimate workflow ended in 1970, so DELETE matches zero rows.
-- Secondary cleanup: orphaned RUNNING rows >24h. This window is aligned to the
-- 2h compute task budget (CLAUDE.md) — a 24h-old RUNNING row is certainly orphaned.

WITH billing AS (
    SELECT
        usage_metadata.job_run_id AS job_run_id,
        usage_date,
        SUM(usage_quantity) AS dbu,
        SUM(
            usage_quantity
            * CAST(prices.pricing.effective_list.default AS DECIMAL(10, 4))
        ) AS cost_usd
    FROM system.billing.usage AS usage
    INNER JOIN system.billing.list_prices AS prices
        ON prices.sku_name = usage.sku_name
        AND usage.usage_end_time >= prices.price_start_time
        AND (
            prices.price_end_time IS NULL
            OR usage.usage_end_time < prices.price_end_time
        )
    WHERE
        usage.billing_origin_product = 'JOBS'
        AND usage.usage_date >= CURRENT_DATE - INTERVAL 90 DAYS
    GROUP BY 1, 2
),

tasks AS (
    SELECT
        job_run_id,
        task_key,
        SUM(execution_duration_seconds) AS execution_duration_seconds
    FROM system.lakeflow.job_task_run_timeline
    WHERE
        result_state IS NOT NULL
        AND period_start_time >= CURRENT_DATE - INTERVAL 90 DAYS
    GROUP BY job_run_id, task_key
),

workflow_ids AS (
    SELECT task_key, workflow_id
    FROM {{ ref('task_workflow_mapping') }}
),

warm_tier AS (
    SELECT
        job_run_id,
        task_key,
        duration_seconds,
        entity_count,
        row_count,
        state AS pipeline_state,
        estimated_cost_usd
    FROM {{ source('observability', 'workflow_cost_live') }}
    WHERE state != 'RUNNING'
)

SELECT
    tasks.task_key,
    billing.usage_date,
    CAST(billing.job_run_id AS BIGINT) AS job_run_id,
    wid.workflow_id,
    CAST(ROUND(
        billing.dbu * (
            tasks.execution_duration_seconds
            / NULLIF(SUM(tasks.execution_duration_seconds)
                OVER (PARTITION BY billing.job_run_id), 0)
        ),
        4
    ) AS DECIMAL(10, 4)) AS attributed_dbu,
    CAST(ROUND(
        billing.cost_usd * (
            tasks.execution_duration_seconds
            / NULLIF(SUM(tasks.execution_duration_seconds)
                OVER (PARTITION BY billing.job_run_id), 0)
        ),
        4
    ) AS DECIMAL(10, 4)) AS attributed_cost_usd,
    wt.duration_seconds,
    wt.entity_count,
    wt.row_count,
    wt.pipeline_state,
    wt.estimated_cost_usd
FROM billing
INNER JOIN tasks ON billing.job_run_id = tasks.job_run_id
LEFT JOIN workflow_ids AS wid
    ON wid.task_key = tasks.task_key
LEFT JOIN warm_tier AS wt
    ON CAST(billing.job_run_id AS BIGINT) = wt.job_run_id
    AND tasks.task_key = wt.task_key
