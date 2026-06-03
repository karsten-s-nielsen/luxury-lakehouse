-- Adds xShotOccurrence, gk_influence near/far-post closing-time zones, and the
-- pitch_control_method provenance column to bronze.spadl_action_context (ADR-039).
--
-- Idempotent: ALTER ... ADD COLUMNS is skip-if-exists handled by scripts/migrations/_runner.py.
-- Operator-applied post-merge (NOT auto-applied in CI on this branch — the dbt-live-ci migration
-- step is currently unwired; see ADR-039 / spec §11). Run BEFORE the AC-1 compute writes these
-- columns and BEFORE the next live dbt build selects them.
ALTER TABLE soccer_analytics.bronze.spadl_action_context ADD COLUMNS (
  gk_closing_time_mean_s__near_post DOUBLE,
  gk_closing_time_min_s__near_post DOUBLE,
  gk_closing_time_mean_s__far_post DOUBLE,
  gk_closing_time_min_s__far_post DOUBLE,
  xshot_occurrence DOUBLE,
  pitch_control_method STRING
);
