-- scripts/migrations/2026-09-17-add-restdefense.sql
--
-- silly-kicks 4.102/4.103 P1 adoption (sk4118 cycle, Phase E): create the OWN bronze table for the
-- rest-defense family (restdefense L1 + L2 — the DEFENDING team's rest-defence geometry per in-possession
-- on-ball action). The gold mart fct_rest_defense + the dbt staging view stg_rest_defense resolve on the
-- LIVE warehouse BEFORE the tracking-marts drain (P2) populates the table (the phased-materialization
-- pattern, mirroring 2026-09-17-add-gk-decision.sql).
--
-- ADR-013 writer table. Column list mirrors the writer DDL constant — keep in sync:
--   * rest_defense -> ingestion.restdefense_writer.REST_DEFENSE_DDL / OUTPUT_COLUMNS. Grain:
--     (data_source, match_id, period_id, team_id, action_id) = the DEFENDING team's shape at that action.
--     11 Layer-1 descriptive cols + 5 Layer-2 xT/space-control cols (NaN without a fitted xt) +
--     rd_geometry_source; team_id_native is the mapped native defending-team id for the dim_teams join.
--     restdefense is DESCRIPTIVE / team-structural — NOT per-player-evaluative (no governance card).
--
-- Idempotent by construction (CREATE TABLE IF NOT EXISTS — a re-run is a no-op). Operator-applied WITH
-- the merge (there is NO CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/2026-09-17-add-restdefense.sql

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.rest_defense (
    data_source STRING,
    match_id STRING,
    team_id_native STRING,
    game_id BIGINT,
    period_id BIGINT,
    team_id STRING,
    action_id BIGINT,
    possession_id BIGINT,
    is_possession_loss BOOLEAN,
    rd_num_superiority BIGINT,
    rd_num_superiority_gk BIGINT,
    rd_zone_occupancy BIGINT,
    rd_line_height DOUBLE,
    rd_line_height_relative DOUBLE,
    rd_compactness_x DOUBLE,
    rd_width DOUBLE,
    rd_depth DOUBLE,
    rd_shape_2_3_vs_3_2 STRING,
    rd_gk_line_height DOUBLE,
    rd_gk_to_line_distance DOUBLE,
    rd_attacker_space_control DOUBLE,
    rd_danger_behind_line DOUBLE,
    rd_danger_behind_line_gk DOUBLE,
    rd_gk_coverage_behind_line DOUBLE,
    rd_gk_reachable_coverage_m2 DOUBLE,
    rd_geometry_source STRING,
    access_tier STRING,
    _ingested_at TIMESTAMP
) USING DELTA;
