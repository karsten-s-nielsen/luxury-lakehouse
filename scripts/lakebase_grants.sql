-- ──────────────────────────────────────────────────────────────────────────────
-- Lakebase PostgreSQL Grants — Streamlit App Service Principal
-- ──────────────────────────────────────────────────────────────────────────────
-- Run this script against the Lakebase Autoscaling PostgreSQL 17 endpoint
-- after synced tables are created. Connects as the workspace admin user
-- (requires manual PG login via OAuth).
--
-- Usage (psql with Autoscaling endpoint):
--   psql -h <lakebase-read-write-dns> -U <admin-uuid> -d databricks_postgres -f scripts/lakebase_grants.sql
--
-- The :app_sp_uuid variable must be set to the Streamlit app service principal
-- UUID (the 'sub' claim from its OAuth JWT). Find it via:
--   terraform output -raw app_sp_application_id
--
-- Example:
--   psql -v app_sp_uuid="'be66af99-5296-4fd9-887a-c081bce38bfa'" \
--        -h <host> -U <admin> -d databricks_postgres -f scripts/lakebase_grants.sql
-- ──────────────────────────────────────────────────────────────────────────────

-- Grant schema-level access (required to see tables in dev_gold)
GRANT USAGE ON SCHEMA dev_gold TO :app_sp_uuid;

-- Grant read access on all current tables in dev_gold
GRANT SELECT ON ALL TABLES IN SCHEMA dev_gold TO :app_sp_uuid;

-- Grant read access on tables created in the future (by synced tables)
ALTER DEFAULT PRIVILEGES IN SCHEMA dev_gold
    GRANT SELECT ON TABLES TO :app_sp_uuid;
