-- ──────────────────────────────────────────────────────────────────────────────
-- Lakebase PostgreSQL Grants — Taipy App Service Principal
-- ──────────────────────────────────────────────────────────────────────────────
-- Grants SELECT access on dev_gold and observability schemas to the Taipy app
-- service principal. Must be run after:
--   1. Initial Lakebase setup (synced tables created)
--   2. OAuth M2M migration (new SP needs grants)
--   3. Synced table recreation (grants are dropped with tables)
--
-- The :app_sp_uuid variable is the service principal's UUID — the 'sub' claim
-- from its OAuth JWT. Find it via:
--   terraform output -raw hf_app_sp_application_id
--   (current: 1a1dbf08-df56-48de-b97a-276b2a4232d8)
--
-- Automated usage (via scripts/run_lakebase_grants.py):
--   uv run python scripts/run_lakebase_grants.py
--
-- Manual usage (psql):
--   psql -v app_sp_uuid="'1a1dbf08-df56-48de-b97a-276b2a4232d8'" \
--        -h <lakebase-read-write-dns> -U <admin-uuid> -d databricks_postgres \
--        -f scripts/lakebase_grants.sql
-- ──────────────────────────────────────────────────────────────────────────────

-- Grant schema-level access (required to see tables in dev_gold)
GRANT USAGE ON SCHEMA dev_gold TO :app_sp_uuid;

-- Grant read access on all current tables in dev_gold
GRANT SELECT ON ALL TABLES IN SCHEMA dev_gold TO :app_sp_uuid;

-- Grant read access on tables created in the future by the current user
ALTER DEFAULT PRIVILEGES IN SCHEMA dev_gold
    GRANT SELECT ON TABLES TO :app_sp_uuid;

-- Grant read access on tables created by databricks_superuser (synced tables).
-- Synced tables are created by this system role, not by the admin user running
-- this script. Without FOR ROLE, dbt table rebuilds (drop + recreate) lose the
-- SELECT grant because the new table is owned by databricks_superuser.
ALTER DEFAULT PRIVILEGES FOR ROLE databricks_superuser IN SCHEMA dev_gold
    GRANT SELECT ON TABLES TO :app_sp_uuid;

-- ── Observability schema — workflow cost tracking ───────────────────────────
-- workflow_cost_live_synced lives in the observability schema (not dev_gold).
-- The Taipy Workflows dashboard queries it for warm-tier cost data.
GRANT USAGE ON SCHEMA observability TO :app_sp_uuid;
GRANT SELECT ON ALL TABLES IN SCHEMA observability TO :app_sp_uuid;
ALTER DEFAULT PRIVILEGES IN SCHEMA observability
    GRANT SELECT ON TABLES TO :app_sp_uuid;
ALTER DEFAULT PRIVILEGES FOR ROLE databricks_superuser IN SCHEMA observability
    GRANT SELECT ON TABLES TO :app_sp_uuid;
