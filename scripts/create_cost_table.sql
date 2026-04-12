-- Create the workflow_cost_live Delta table for warm/hot cost tracking.
-- Run once via Databricks SQL or notebook.
-- Not managed by dbt (written by Spark CostEstimateHook, not dbt).
--
-- Usage: Replace {catalog} with the actual catalog name before running.
-- The table lives in the `observability` schema (platform operational metadata).
-- Example: soccer_analytics.observability.workflow_cost_live

CREATE TABLE IF NOT EXISTS {catalog}.observability.workflow_cost_live (
    workflow_id            STRING        NOT NULL,
    phase                  STRING        NOT NULL,
    run_id                 STRING        NOT NULL,
    runtime                STRING        NOT NULL,
    hf_job_id              STRING,
    state                  STRING        NOT NULL,
    started_at             TIMESTAMP     NOT NULL,
    ended_at               TIMESTAMP,
    duration_seconds       INT,
    row_count              INT,
    entity_count           INT,
    guard_duration_seconds INT,
    rate_usd_per_hour      DECIMAL(10,6),
    estimated_cost_usd     DECIMAL(10,4),
    cost_source            STRING        NOT NULL,
    updated_at             TIMESTAMP     NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
-- No liquid clustering: table is bounded at <100 rows at any time (active runs +
-- recent completions before daily cleanup sweep). Sequential scan is faster than
-- index maintenance at this scale.
