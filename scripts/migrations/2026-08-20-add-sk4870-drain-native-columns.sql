-- scripts/migrations/2026-08-20-add-sk4870-drain-native-columns.sql
--
-- silly-kicks 4.87.0 full-adoption (spec §7.1/§7.2, Chunk P2). Add the DRAIN-NATIVE action-context
-- columns to the already-materialized bronze tables so the gold marts' explicit SELECTs resolve on
-- the LIVE tables (fresh tables get them via ``ensure_table`` from the head schemas — ACTION_CONTEXT_DDL
-- for spadl_action_context; _SPADL_SCHEMA / _VAEP_SCHEMA for the SPADL surface).
--
--   * bronze.spadl_action_context — the 23 new drain-native AC columns:
--       obso_epv_source (real-xT OBSO provenance, spec §7.3), the TF-35 off-ball run-value family,
--       the TF-51 press-commitment family, the TF-49 packing family, das_source/ghost_gk_source
--       provenance free-rides, max_single_defender_player_id (cover-shadow id free-ride), and the 6
--       team_shape gap columns (defensive line height + inter-line gaps, attacking/defending).
--       (ghost_gk_density_spread + ghost_gk_method were RETIRED from the head schema at 4.87.0 —
--        this migration does NOT drop them from bronze; the physical columns remain and read NULL
--        post-recompute. A destructive DROP, if desired, is operator-driven per the migrations convention.)
--   * bronze.spadl_action_context — the 8 visibility-coverage columns (spec §7.1/§7.5, SB360-only):
--       visible_area_fraction/source + the 6 {nearest_defender_distance,receiver_zone_density,
--       defenders_in_triangle_to_goal}_observed_{fraction,source} companions. Separate ADD COLUMNS block.
--   * bronze.spadl_actions      — shot_blocked / cross_blocked (silly-kicks 4.56/4.86.0 SPADL block flags).
--   * bronze.vaep_action_values — shot_blocked / cross_blocked carried through the VAEP scoring writer.
--
-- NO backfill: these values require re-running the frame-based AC enrichment + the SPADL/VAEP conversion.
-- Existing rows stay NULL until the Phase-5 (Part B) recompute re-materializes them — the intended
-- phased-materialization design (mirrors the is_gk_distribution / access_tier migrations).
--
-- Idempotent by construction: one single-leading-column ADD COLUMNS per table — ``_runner.py``'s
-- DESCRIBE skip-if-exists makes each ALTER a no-op once its leading column exists.
--
-- Catalog hardcoded as `soccer_analytics` per the migrations convention (the runner does not perform
-- ${...} substitution). Operator-applied per the migrations convention (no CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-08-20-add-sk4870-drain-native-columns.sql

ALTER TABLE soccer_analytics.bronze.spadl_action_context
ADD COLUMNS (
    obso_epv_source STRING,
    run_value_target DOUBLE,
    run_value_disruptive_sum DOUBLE,
    run_value_enabled_pass DOUBLE,
    n_disruptive_runs BIGINT,
    n_valued_disruptive_runs BIGINT,
    press_commitment DOUBLE,
    press_commitment_closing_speed DOUBLE,
    press_commitment_source STRING,
    packing_made BIGINT,
    packing_goal_threat BIGINT,
    packing_net DOUBLE,
    packing_receiver_player_id STRING,
    packing_secured BOOLEAN,
    das_source STRING,
    ghost_gk_source STRING,
    max_single_defender_player_id STRING,
    team_shape_defensive_line_height_attacking DOUBLE,
    team_shape_defensive_line_height_defending DOUBLE,
    team_shape_inter_line_gap_1_attacking DOUBLE,
    team_shape_inter_line_gap_1_defending DOUBLE,
    team_shape_inter_line_gap_2_attacking DOUBLE,
    team_shape_inter_line_gap_2_defending DOUBLE
);

-- Visibility coverage (silly-kicks 4.87.0, spec §7.1/§7.5, Chunk P2 — visibility 8 columns). SB360-only
-- (empty until SB360 AC is enabled, ADR-058), but shipped in the schema/contract now. A SEPARATE
-- ADD COLUMNS block (leading column visible_area_fraction) so this is idempotent INDEPENDENTLY of the
-- drain-native block above — the runner's DESCRIBE skip-if-exists keys on each statement's leading
-- column, so this applies even if the block above was already run.
--   2 base cols (add_visible_area_coverage): observed pitch fraction + provenance vocabulary.
--   6 companions (add_action_context(visible_area=)): {nearest_defender_distance, receiver_zone_density,
--   defenders_in_triangle_to_goal}_observed_{fraction (DOUBLE), source (STRING)}.
ALTER TABLE soccer_analytics.bronze.spadl_action_context
ADD COLUMNS (
    visible_area_fraction DOUBLE,
    visible_area_source STRING,
    nearest_defender_distance_observed_fraction DOUBLE,
    nearest_defender_distance_observed_source STRING,
    receiver_zone_density_observed_fraction DOUBLE,
    receiver_zone_density_observed_source STRING,
    defenders_in_triangle_to_goal_observed_fraction DOUBLE,
    defenders_in_triangle_to_goal_observed_source STRING
);

ALTER TABLE soccer_analytics.bronze.spadl_actions
ADD COLUMNS (
    shot_blocked BOOLEAN,
    cross_blocked BOOLEAN
);

ALTER TABLE soccer_analytics.bronze.vaep_action_values
ADD COLUMNS (
    shot_blocked BOOLEAN,
    cross_blocked BOOLEAN
);
