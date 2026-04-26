{{ config(
    materialized='table',
    liquid_clustered_by=['task_key', 'usage_date'],
    post_hook=[
        "DELETE FROM {{ this.database }}.observability.workflow_cost_live
         WHERE state != 'RUNNING'
           AND ended_at IS NOT NULL
           AND ended_at < CURRENT_TIMESTAMP - INTERVAL 7 DAYS",
        "DELETE FROM {{ this.database }}.observability.workflow_cost_live
         WHERE state = 'RUNNING'
           AND started_at < CURRENT_TIMESTAMP - INTERVAL 24 HOURS"
    ]
) }}
-- fct_workflow_costs.sql
-- Gold-layer workflow cost attribution from Databricks system tables.
--
-- Joins billing usage with list prices and attributes cost per-task
-- proportionally by execution duration within each job run.
-- 90-day rolling window refreshed daily.
--
-- Driving table: system_lakeflow_job_task_run_timeline view (tasks CTE).
-- Timing data (cold_start, duration, entity_count) is available immediately.
-- Billing data (attributed_cost_usd, attributed_dbu) arrives with ~1 day lag
-- via LEFT JOIN on the system_billing_usage view — NULL until billing catches up.
-- effective_cost_usd = COALESCE(attributed_cost_usd, estimated_cost_usd)
-- so the UI always has a cost value (actual when available, estimated until then).
--
-- All three system.* references go through definer's-rights views in
-- soccer_analytics.observability (see scripts/setup_system_billing_views.sql).
-- The system catalog is metastore-managed and cannot be granted to SPs/groups
-- via the standard UC API, so we interpose filtered views owned by an account
-- admin. Filters (billing_origin_product = 'JOBS', 90-day window,
-- result_state IS NOT NULL) are applied inside the views, not repeated here.
--
-- Warm-tier enrichment: LEFT JOINs workflow_cost_live (written by
-- CostEstimateHook) via workflow_id + temporal window (serverless exposes
-- no job_run_id or task_key — D55 investigation confirmed).
-- cold_start_seconds = warm.started_at - cold.task_started_at.
--
-- Post-hook 1 cleanup (D65 fix 2026-04-15): removes warm-tier rows older than
-- 7 days. The 7-day window gives billing more than enough time to land while
-- providing bounded warm-tier growth. A prior implementation used a date-based
-- watermark (MAX usage_date WHERE billing IS NOT NULL, advanced by one day)
-- which advanced monotonically as a SINGLE row landed with billing — pruning
-- sibling 2026-04-14 rows whose billing had not yet arrived. Two attempts at
-- EXISTS-correlated alternatives both surfaced edge cases (NULL workflow_id
-- in `grant_event_log`, sibling pruning under correlation). Time-based
-- retention is simpler, has no edge cases, and matches post-hook 2's pattern.
-- Post-hook 2: orphaned RUNNING rows >24h. This window is aligned to the
-- 2h compute task budget (CLAUDE.md) — a 24h-old RUNNING row is certainly orphaned.

WITH billing AS (
    SELECT
        usage.job_run_id AS job_run_id,
        usage.usage_date,
        SUM(usage.usage_quantity) AS dbu,
        SUM(usage.usage_quantity * prices.effective_list_default) AS cost_usd
    FROM {{ source('observability', 'system_billing_usage') }} AS usage
    INNER JOIN {{ source('observability', 'system_billing_list_prices') }} AS prices
        ON prices.sku_name = usage.sku_name
        AND usage.usage_end_time >= prices.price_start_time
        AND (
            prices.price_end_time IS NULL
            OR usage.usage_end_time < prices.price_end_time
        )
    GROUP BY 1, 2
),

tasks AS (
    SELECT
        job_run_id,
        task_key,
        SUM(execution_duration_seconds) AS execution_duration_seconds,
        MIN(period_start_time) AS task_started_at
    FROM {{ source('observability', 'system_lakeflow_job_task_run_timeline') }}
    GROUP BY job_run_id, task_key
),

workflow_ids AS (
    SELECT task_key, workflow_id
    FROM {{ ref('task_workflow_mapping') }}
),

warm_tier_raw AS (
    SELECT
        workflow_id,
        started_at,
        duration_seconds,
        entity_count,
        row_count,
        guard_duration_seconds,
        state AS pipeline_state,
        estimated_cost_usd,
        run_id
    FROM {{ source('observability', 'workflow_cost_live') }}
    WHERE state != 'RUNNING'
),

-- Deduplicate: if multiple warm-tier rows match the same workflow_id window,
-- pick the one with the latest run_id (most recent hook write). Prevents
-- row multiplication when a workflow retries within the temporal window.
warm_tier AS (
    SELECT * FROM (
        SELECT
            wtr.*,
            ROW_NUMBER() OVER (
                PARTITION BY wtr.workflow_id, wtr.started_at
                ORDER BY wtr.run_id DESC
            ) AS _rn
        FROM warm_tier_raw AS wtr
    )
    WHERE _rn = 1
)

SELECT
    tasks.task_key,
    COALESCE(billing.usage_date, CAST(tasks.task_started_at AS DATE)) AS usage_date,
    CAST(tasks.job_run_id AS BIGINT) AS job_run_id,
    wid.workflow_id,
    CAST(ROUND(
        billing.dbu * (
            tasks.execution_duration_seconds
            / NULLIF(SUM(tasks.execution_duration_seconds)
                OVER (PARTITION BY tasks.job_run_id), 0)
        ),
        4
    ) AS DECIMAL(10, 4)) AS attributed_dbu,
    CAST(ROUND(
        billing.cost_usd * (
            tasks.execution_duration_seconds
            / NULLIF(SUM(tasks.execution_duration_seconds)
                OVER (PARTITION BY tasks.job_run_id), 0)
        ),
        4
    ) AS DECIMAL(10, 4)) AS attributed_cost_usd,
    tasks.execution_duration_seconds,
    wt.duration_seconds,
    GREATEST(CAST(TIMESTAMPDIFF(SECOND, tasks.task_started_at, wt.started_at) AS INT), 0) AS cold_start_seconds,
    wt.entity_count,
    wt.row_count,
    wt.guard_duration_seconds,
    wt.pipeline_state,
    wt.estimated_cost_usd,
    COALESCE(
        CAST(ROUND(
            billing.cost_usd * (
                tasks.execution_duration_seconds
                / NULLIF(SUM(tasks.execution_duration_seconds)
                    OVER (PARTITION BY tasks.job_run_id), 0)
            ),
            4
        ) AS DECIMAL(10, 4)),
        wt.estimated_cost_usd
    ) AS effective_cost_usd
FROM tasks
LEFT JOIN billing ON billing.job_run_id = tasks.job_run_id
LEFT JOIN workflow_ids AS wid
    ON wid.task_key = tasks.task_key
LEFT JOIN warm_tier AS wt
    ON wid.workflow_id = wt.workflow_id
    AND wt.started_at BETWEEN tasks.task_started_at - INTERVAL 2 MINUTES
        AND tasks.task_started_at + INTERVAL 5 MINUTES
-- Retry tiebreaker: when a workflow attempt fails and the framework retries
-- within the 7-min temporal window, the warm-tier `workflow_cost_live` table
-- carries one row per attempt (CostEstimateHook fires per-attempt). The LEFT
-- JOIN above then matches every attempt to the single task row, multiplying
-- the fact grain. Pick the warm-tier row whose started_at is closest to the
-- task's task_started_at (the attempt most likely associated with the
-- original task invocation), with run_id as a stable secondary tiebreaker.
-- 2026-04-25 incident: `wf-model-validation` failed + retried 1m50s later on
-- 2026-04-21; both attempts matched the task's 7-min window, breaking the
-- fct_workflow_costs_synced PG primary-key constraint
-- (task_key, usage_date, job_run_id). See accompanying schema test
-- `unique_combination_of_columns` in _marts__models.yml — that's the
-- compile-time gate; this QUALIFY is the runtime guarantee.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY tasks.task_key,
                 COALESCE(billing.usage_date, CAST(tasks.task_started_at AS DATE)),
                 tasks.job_run_id
    ORDER BY abs(TIMESTAMPDIFF(SECOND, tasks.task_started_at, wt.started_at)) NULLS LAST,
             wt.run_id NULLS LAST
) = 1
