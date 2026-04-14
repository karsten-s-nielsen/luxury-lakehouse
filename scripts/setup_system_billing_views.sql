-- setup_system_billing_views.sql
-- One-time setup for definer's-rights views over system.billing and system.lakeflow.
--
-- Problem:
--   dbt model fct_workflow_costs queries system.billing.usage, system.billing.list_prices,
--   and system.lakeflow.job_task_run_timeline directly. These schemas are metastore-managed
--   and cannot be GRANT'ed to service principals or groups via the standard UC grant API
--   (returns 403 even for account admins). The ingestion SP (008b207b-96a8-4d54-b185-a77479a55abe)
--   and dbt-owners-dev group therefore cannot read them.
--
-- Solution:
--   Create filtered definer's-rights views in soccer_analytics.observability. UC views default
--   to definer's-rights semantics: the view OWNER must have SELECT on underlying tables, but
--   CONSUMERS only need SELECT on the view itself. Owner = an account admin (has SELECT on
--   system.* via account-users membership). Consumers = dbt-owners-dev group.
--
-- Execution:
--   MUST be run as an account admin (currently karstenskyt@gmail.com). Run via
--   WorkspaceClient.statement_execution against soccer-analytics-warehouse-dev.
--
-- Maintenance:
--   Views are owned by the executing user. If that user is removed from the workspace, views
--   stop working and must be recreated by the new account admin. Re-running this script
--   (CREATE OR REPLACE) is idempotent.

-- View 1: billing usage filtered to JOBS in last 90 days
CREATE OR REPLACE VIEW soccer_analytics.observability.system_billing_usage AS
SELECT
    usage_metadata.job_run_id AS job_run_id,
    usage_date,
    usage_quantity,
    sku_name,
    usage_end_time
FROM system.billing.usage
WHERE billing_origin_product = 'JOBS'
  AND usage_date >= CURRENT_DATE - INTERVAL 90 DAYS;

-- View 2: list prices flattened to the single DECIMAL we need
CREATE OR REPLACE VIEW soccer_analytics.observability.system_billing_list_prices AS
SELECT
    sku_name,
    CAST(pricing.effective_list.default AS DECIMAL(10, 4)) AS effective_list_default,
    price_start_time,
    price_end_time
FROM system.billing.list_prices;

-- View 3: job task run timeline filtered to completed tasks in last 90 days
CREATE OR REPLACE VIEW soccer_analytics.observability.system_lakeflow_job_task_run_timeline AS
SELECT
    job_run_id,
    task_key,
    execution_duration_seconds,
    period_start_time,
    result_state
FROM system.lakeflow.job_task_run_timeline
WHERE result_state IS NOT NULL
  AND period_start_time >= CURRENT_DATE - INTERVAL 90 DAYS;

-- Grant SELECT on all three views to dbt-owners-dev
-- Group membership propagates to both the developer user and the ingestion SP.
GRANT SELECT ON VIEW soccer_analytics.observability.system_billing_usage TO `dbt-owners-dev`;
GRANT SELECT ON VIEW soccer_analytics.observability.system_billing_list_prices TO `dbt-owners-dev`;
GRANT SELECT ON VIEW soccer_analytics.observability.system_lakeflow_job_task_run_timeline TO `dbt-owners-dev`;
