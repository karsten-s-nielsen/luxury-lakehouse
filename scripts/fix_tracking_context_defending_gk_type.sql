-- One-time fix (PR #290):
--   1. defending_gk_player_id was declared DOUBLE but contains native STRING IDs.
--   2. Renamed to defending_gk_player_id_native per ADR-018 convention.
--
-- Delta does not support column rename without columnMapping (one-way door).
-- DROP + ensure_table recreate is safe: data is re-computable from bronze SPADL
-- actions + tracking frames. ensure_table() recreates with the corrected DDL.
--
-- Run BEFORE the next compute_tracking_context pipeline execution.
-- Verify current state first:
--   SELECT COUNT(*) FROM soccer_analytics.bronze.spadl_tracking_context;

DROP TABLE IF EXISTS soccer_analytics.bronze.spadl_tracking_context;
