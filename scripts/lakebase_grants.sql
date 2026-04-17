-- ──────────────────────────────────────────────────────────────────────────────
-- Lakebase PostgreSQL Grants — Taipy App Service Principal
-- ──────────────────────────────────────────────────────────────────────────────
-- Grants SELECT on dev_gold and observability to the Taipy app service
-- principal. MUST be re-run after any synced-table recreation (dbt `table`
-- materialization, schema change, manual UI recreate).
--
-- Why re-run is mandatory (see docs/superpowers/adrs/ADR-005-lakebase-synced-table-grants.md):
--   Lakebase synced tables are owned by the internal role
--   databricks_writer_<instance_id> (not by databricks_superuser and not by
--   the admin user running this script). Postgres ALTER DEFAULT PRIVILEGES
--   rules are scoped FOR ROLE <grantor>, and we cannot target
--   databricks_writer_* because we are not a member of that role. There is
--   no auto-inherit mechanism available to us — grants must be re-applied
--   explicitly on every table recreation.
--
-- Canonical usage:
--   uv run python scripts/run_lakebase_grants.py            # apply
--   uv run python scripts/run_lakebase_grants.py --verify   # drift check
--
-- The Python script is the canonical mechanism. This .sql file is kept for
-- documentation and manual psql invocation in emergencies; the script reads
-- the SP UUID from terraform output so there is no hardcoded drift.
--
-- The :app_sp_uuid variable is the application_id of
-- module.service_principals.databricks_service_principal.hf_app, exposed as
-- terraform output hf_app_sp_application_id.
--
-- Manual psql usage (emergency):
--   terraform -chdir=terraform/environments/dev output -raw hf_app_sp_application_id
--   psql -v app_sp_uuid="'<uuid-from-above>'" \
--        -h <lakebase-read-write-dns> -U <admin-uuid> -d databricks_postgres \
--        -f scripts/lakebase_grants.sql
-- ──────────────────────────────────────────────────────────────────────────────

-- ── dev_gold schema — 35 gold-layer synced fact and dim tables ────────────────
GRANT USAGE ON SCHEMA dev_gold TO :app_sp_uuid;
GRANT SELECT ON ALL TABLES IN SCHEMA dev_gold TO :app_sp_uuid;

-- Future tables created by the CURRENT user (ad-hoc admin-created tables).
-- Does NOT cover synced-table recreations — see ADR-005 for the limitation.
ALTER DEFAULT PRIVILEGES IN SCHEMA dev_gold
    GRANT SELECT ON TABLES TO :app_sp_uuid;

-- ── observability schema — workflow_cost_live_synced ─────────────────────────
GRANT USAGE ON SCHEMA observability TO :app_sp_uuid;
GRANT SELECT ON ALL TABLES IN SCHEMA observability TO :app_sp_uuid;
ALTER DEFAULT PRIVILEGES IN SCHEMA observability
    GRANT SELECT ON TABLES TO :app_sp_uuid;
