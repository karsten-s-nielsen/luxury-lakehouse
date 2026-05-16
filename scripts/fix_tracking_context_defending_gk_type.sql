-- One-time fix: defending_gk_player_id was declared as DOUBLE but contains
-- native player ID strings (DFL-OBJ-*, PlayerN). The table has 0 rows so
-- DROP + ensure_table recreate is safe and avoids columnMapping one-way door.
--
-- Run BEFORE the next compute_tracking_context pipeline execution.
-- The preflight guard's ensure_table() will recreate with the corrected DDL.
--
-- Verify 0 rows first:
--   SELECT COUNT(*) FROM soccer_analytics.bronze.spadl_tracking_context;

DROP TABLE IF EXISTS soccer_analytics.bronze.spadl_tracking_context;
