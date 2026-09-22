-- scripts/migrations/2026-09-17-add-shot-stopping.sql
--
-- silly-kicks 4.120.0 P1 adoption (sk4118 cycle, Task B1): create the OWN bronze table for the
-- GK shot-stopping family (TF-59 compute_shot_stopping — Goals Prevented / GSAA per defending keeper).
-- The re-sourced GK marts (fct_gk_shot_stopping / _pooled / fct_goalkeeper_stats) + the dbt staging
-- view stg_shot_stopping resolve on the LIVE warehouse BEFORE the Part-B scoring run populates the
-- table (the phased-materialization pattern, mirroring 2026-09-17-add-team-metrics.sql).
--
-- ADR-013 writer table. Column list mirrors the writer DDL constant — keep in sync:
--   * shot_stopping -> ingestion.shot_stopping_writer.SHOT_STOPPING_DDL (8 metric cols + native ids +
--     access_tier). Grain: (data_source, match_id, player_id, team_id) — player_id is the NATIVE
--     defending keeper id (dim_players join), team_id the native keeper team.
--
-- Idempotent by construction (CREATE TABLE IF NOT EXISTS — a re-run is a no-op). Operator-applied WITH
-- the merge (there is NO CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/2026-09-17-add-shot-stopping.sql

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.shot_stopping (
    data_source STRING,
    match_id STRING,
    player_id STRING,
    team_id STRING,
    access_tier STRING,
    shots_faced BIGINT,
    goals_conceded BIGINT,
    psxg_faced DOUBLE,
    goals_prevented DOUBLE,
    shots_faced_excl_penalties BIGINT,
    goals_conceded_excl_penalties BIGINT,
    psxg_faced_excl_penalties DOUBLE,
    goals_prevented_excl_penalties DOUBLE,
    _ingested_at TIMESTAMP
) USING DELTA;
