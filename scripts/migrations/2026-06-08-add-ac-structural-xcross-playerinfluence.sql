-- AC-1 silly-kicks 4.19.2: add structural-pass, player-influence, and xCross columns to
-- bronze.spadl_action_context. Emitted by add_structural_pass (4.16.0) / add_player_influence /
-- add_xcross_attempt (4.18.0) in the enrichment chain; NULL until the next compute run.
--
-- Operator-applied (there is NO CI auto-apply — see CLAUDE.md / reference_bronze_migration_autoapply_gap).
-- Idempotent: the runner skips ADD COLUMNS when the leading column (structural_lbs) already exists
-- (DESCRIBE pre-check); Delta applies the column list atomically.
--
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-06-08-add-ac-structural-xcross-playerinfluence.sql

ALTER TABLE soccer_analytics.bronze.spadl_action_context
  ADD COLUMNS (
    structural_lbs BIGINT,
    structural_sgm DOUBLE,
    structural_sdi DOUBLE,
    actor_reachable_area_m2 DOUBLE,
    off_ball_xt_team DOUBLE,
    off_ball_xt_opponent DOUBLE,
    off_ball_xt_diff DOUBLE,
    reachable_area_team DOUBLE,
    reachable_area_opponent DOUBLE,
    reachable_area_diff DOUBLE,
    xcross_attempt DOUBLE
  );
