-- scripts/migrations/2026-09-17-add-territory.sql
--
-- silly-kicks 4.120.0 P1 adoption (sk4118 cycle, Task C1): create the OWN bronze table for the
-- player-match territorial-dominance family (TF-54 / TF-54b compute_territorial_dominance). The gold
-- mart fct_player_match_metrics + its dbt staging view resolve on the LIVE warehouse BEFORE the Part-B
-- scoring run populates the table (the phased-materialization pattern, mirroring
-- 2026-09-17-add-team-metrics.sql).
--
-- ADR-013 writer table. Column list mirrors the writer DDL constant — keep in sync:
--   * territory -> ingestion.territory_writer.TERRITORY_DDL (the 20-col counterfactual union = 12 v1
--     metric + territory_hull_source + 4 cf metric + territory_target_source, + native ids + access_tier).
--
-- Idempotent by construction (CREATE TABLE IF NOT EXISTS — a re-run is a no-op). Operator-applied WITH
-- the merge (there is NO CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/2026-09-17-add-territory.sql

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.territory (
    data_source STRING,
    match_id STRING,
    player_id STRING,
    team_id STRING,
    access_tier STRING,
    territory_xt_conceded DOUBLE,
    territory_xt_prevented DOUBLE,
    territory_xt_net DOUBLE,
    territory_xt_conceded_forward DOUBLE,
    territory_xt_prevented_forward DOUBLE,
    territory_passes_into_hull BIGINT,
    territory_xt_conceded_rate DOUBLE,
    territory_xt_prevented_rate DOUBLE,
    territory_hull_area_m2 DOUBLE,
    territory_hull_centroid_x DOUBLE,
    territory_hull_centroid_y DOUBLE,
    territory_defensive_actions_in_hull BIGINT,
    territory_hull_source STRING,
    territory_expected_threat_faced DOUBLE,
    territory_xt_prevented_above_expectation DOUBLE,
    territory_passes_aimed_into_hull BIGINT,
    territory_mean_completion_faced DOUBLE,
    territory_target_source STRING,
    _ingested_at TIMESTAMP
) USING DELTA;
