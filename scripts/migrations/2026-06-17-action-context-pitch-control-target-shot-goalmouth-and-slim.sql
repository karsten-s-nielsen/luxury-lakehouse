-- AC-1 silly-kicks 4.31.0/4.32.0 (ADR-056 + ADR-057): bring bronze.spadl_action_context to the
-- current code DDL. Three parts:
--   1. RENAME the pitch-control column family at_ball -> at_target (silly-kicks ADR-032 / lakehouse
--      ADR-056): the metric is now sampled at the action DESTINATION, not the ball. RENAME (not ADD)
--      preserves the column +
--      historical data lineage; an ensure_table ADD would orphan the old at_ball columns.
--   2. ADD the add_shot_goalmouth (TF-48, Anzer & Bauer 2021) tracking column family (11 cols),
--      emitted by enrich.py Step 21b. NULL until the next AC recompute repopulates the table.
--   3. DROP the 4 columns the Kimball slimming (ADR-056) removed from the AC contract — game_state +
--      the GK action-sequence flags — now served by fct_action_values (actions-level, frame-independent).
--
-- Operator-applied (there is NO CI auto-apply — see CLAUDE.md / reference_bronze_migration_autoapply_gap).
-- RUN-ONCE (NOT idempotent): the RENAME fails on re-run (at_ball no longer exists) and the DROP fails if
-- the columns were already removed. Column-mapping mode is ON (since 4.19.2) so RENAME/DROP are supported.
-- Before applying, VERIFY the current live schema:
--   DESCRIBE soccer_analytics.bronze.spadl_action_context;
-- Apply the at_ball->at_target RENAME + shot_* ADD only if at_ball exists; apply the DROP block only if the
-- 4 slimmed columns are present. (The full AC recompute repopulates every row afterwards.)
--
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-06-17-action-context-pitch-control-target-shot-goalmouth-and-slim.sql

-- 1. pitch_control at_ball -> at_target (ADR-056). RUN-ONCE.
ALTER TABLE soccer_analytics.bronze.spadl_action_context
  RENAME COLUMN pitch_control_at_ball__spearman TO pitch_control_at_target__spearman;
ALTER TABLE soccer_analytics.bronze.spadl_action_context
  RENAME COLUMN pitch_control_at_ball__fernandez_bornn TO pitch_control_at_target__fernandez_bornn;
ALTER TABLE soccer_analytics.bronze.spadl_action_context
  RENAME COLUMN pitch_control_at_ball__voronoi TO pitch_control_at_target__voronoi;

-- 2. add_shot_goalmouth column family (TF-48). Idempotent (runner DESCRIBE-skips when the leading
--    column shot_crossing_y already exists; Delta applies the list atomically).
ALTER TABLE soccer_analytics.bronze.spadl_action_context
  ADD COLUMNS (
    shot_crossing_y DOUBLE,
    shot_crossing_z DOUBLE,
    shot_speed DOUBLE,
    shot_time_to_goal_line DOUBLE,
    shot_on_target_derived BOOLEAN,
    shot_crossing_source STRING,
    shot_crossing_confidence DOUBLE,
    shot_fit_n_frames DOUBLE,
    shot_fit_rmse DOUBLE,
    shot_fit_end_reason STRING,
    shot_z_profile STRING
  );

-- 3. Kimball slim (ADR-056): drop the 4 actions-level columns now served by fct_action_values.
--    RUN-ONCE — only apply if DESCRIBE shows these columns present.
ALTER TABLE soccer_analytics.bronze.spadl_action_context DROP COLUMN game_state;
ALTER TABLE soccer_analytics.bronze.spadl_action_context DROP COLUMN gk_was_distributing;
ALTER TABLE soccer_analytics.bronze.spadl_action_context DROP COLUMN gk_was_engaged;
ALTER TABLE soccer_analytics.bronze.spadl_action_context DROP COLUMN gk_actions_in_possession;
