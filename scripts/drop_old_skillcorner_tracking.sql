-- Drop the old kloppy-sourced bronze.skillcorner_tracking table.
-- This table is replaced by the new pining-for-the-data API source.
-- The new table (same name) is created by skillcorner_tracking.py.
--
-- This migration is idempotent: DROP TABLE IF EXISTS is a no-op if
-- the table was already dropped or never existed.
--
-- Run manually BEFORE the first new DAG execution:
--   databricks sql-cli execute --statement "$(cat scripts/drop_old_skillcorner_tracking.sql)"
DROP TABLE IF EXISTS soccer_analytics.bronze.skillcorner_tracking;
