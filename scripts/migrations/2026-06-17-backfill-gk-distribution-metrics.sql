-- Backfill the GK distribution-metric columns (gk_pass_length_m, gk_pass_length_class, is_launch)
-- in bronze.spadl_actions + bronze.vaep_action_values WITHOUT a full SPADL re-conversion
-- (ADR-056; silly-kicks 4.32.0 add_gk_distribution_metrics, Lamberts 2025 GVM).
--
-- The 2026-06-17-add-spadl-gk-distribution-metrics.sql migration only ADDED the columns (all NULL).
-- This migration POPULATES them from the source SPADL geometry already present on every row, so the
-- live mart can JOIN gk_dist + derive gk_xt_delta ahead of the next full action-context recompute.
--
-- Mirror of add_gk_distribution_metrics (silly_kicks/spadl/utils.py) — validated row-for-row against
-- the Python helper on match 4020005 (2499 rows, len/class/launch all exact, 2026-06-17):
--   * distribution only        : gk_role == 'distribution' (else NULL length/class)
--   * length                   : Euclidean distance start->end in metres
--   * class                    : short (<32.0), long (>60.0), else medium    [thresholds 32.0 / 60.0]
--   * is_launch                : distribution AND type_id in {pass=0, freekick_crossed=3,
--                                freekick_short=4, goalkick=22} AND length > 60.0   (False elsewhere,
--                                never NULL — matches the helper's full boolean column)
--
-- Operator-applied (NO CI auto-apply — see CLAUDE.md / reference_bronze_migration_autoapply_gap).
-- Idempotent by construction: every column is recomputed from immutable source columns
-- (gk_role, start_x/y, end_x/y, type_id), so re-running yields identical values.
--
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-06-17-backfill-gk-distribution-metrics.sql

UPDATE soccer_analytics.bronze.spadl_actions
SET
  gk_pass_length_m = CASE
    WHEN gk_role = 'distribution'
    THEN sqrt(pow(end_x - start_x, 2) + pow(end_y - start_y, 2))
  END,
  gk_pass_length_class = CASE
    WHEN gk_role = 'distribution' THEN
      CASE
        WHEN sqrt(pow(end_x - start_x, 2) + pow(end_y - start_y, 2)) < 32.0 THEN 'short'
        WHEN sqrt(pow(end_x - start_x, 2) + pow(end_y - start_y, 2)) > 60.0 THEN 'long'
        ELSE 'medium'
      END
  END,
  is_launch = (
    COALESCE(gk_role = 'distribution', false)
    AND type_id IN (0, 3, 4, 22)
    AND COALESCE(sqrt(pow(end_x - start_x, 2) + pow(end_y - start_y, 2)) > 60.0, false)
  );

UPDATE soccer_analytics.bronze.vaep_action_values
SET
  gk_pass_length_m = CASE
    WHEN gk_role = 'distribution'
    THEN sqrt(pow(end_x - start_x, 2) + pow(end_y - start_y, 2))
  END,
  gk_pass_length_class = CASE
    WHEN gk_role = 'distribution' THEN
      CASE
        WHEN sqrt(pow(end_x - start_x, 2) + pow(end_y - start_y, 2)) < 32.0 THEN 'short'
        WHEN sqrt(pow(end_x - start_x, 2) + pow(end_y - start_y, 2)) > 60.0 THEN 'long'
        ELSE 'medium'
      END
  END,
  is_launch = (
    COALESCE(gk_role = 'distribution', false)
    AND type_id IN (0, 3, 4, 22)
    AND COALESCE(sqrt(pow(end_x - start_x, 2) + pow(end_y - start_y, 2)) > 60.0, false)
  );
