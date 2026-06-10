-- SPADL silly-kicks 4.21.0 + 4.22.0 (ADR-048): add result_source (SkillCorner native-completion
-- label tier — 'native'/'inferred'/'stopgap'; NULL on the other 5 providers) and the
-- Law-fixed-spot restart-coordinate enrichment (add_restart_coordinates, events-only tiers at
-- the bronze writer; ADDITIVE — canonical start_x/../end_y never mutated) to
-- bronze.spadl_actions AND bronze.vaep_action_values (carried through; _VAEP_SCHEMA parity).
-- NULL until each provider's next SPADL re-conversion (SkillCorner + IDSSE re-conversions are
-- the scheduled follow-up; see PLAN task #14 / project memory).
--
-- Operator-applied (there is NO CI auto-apply — see CLAUDE.md / reference_bronze_migration_autoapply_gap).
-- Idempotent: the runner skips each ADD COLUMNS when its leading column (result_source) already
-- exists (DESCRIBE pre-check); Delta applies each column list atomically.
--
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-06-10-add-spadl-result-source-restart-coords.sql

ALTER TABLE soccer_analytics.bronze.spadl_actions
  ADD COLUMNS (
    result_source STRING,
    enriched_start_x DOUBLE,
    enriched_start_y DOUBLE,
    enriched_end_x DOUBLE,
    enriched_end_y DOUBLE,
    start_coord_source STRING,
    end_coord_source STRING,
    start_coord_confidence DOUBLE,
    end_coord_confidence DOUBLE
  );

ALTER TABLE soccer_analytics.bronze.vaep_action_values
  ADD COLUMNS (
    result_source STRING,
    enriched_start_x DOUBLE,
    enriched_start_y DOUBLE,
    enriched_end_x DOUBLE,
    enriched_end_y DOUBLE,
    start_coord_source STRING,
    end_coord_source STRING,
    start_coord_confidence DOUBLE,
    end_coord_confidence DOUBLE
  );
