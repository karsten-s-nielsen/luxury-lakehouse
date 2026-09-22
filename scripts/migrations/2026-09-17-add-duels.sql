-- scripts/migrations/2026-09-17-add-duels.sql
--
-- silly-kicks 4.120.0 P1 adoption (sk4118 cycle, Task C2): create the OWN bronze table for the
-- player-match ground-duel rating family (TF-55 compute_duel_ratings). The gold mart
-- fct_player_match_metrics + its dbt staging view resolve on the LIVE warehouse BEFORE the Part-B
-- scoring run populates the table (the phased-materialization pattern, mirroring
-- 2026-09-17-add-team-metrics.sql).
--
-- ADR-013 writer table. Column list mirrors the writer DDL constant — keep in sync:
--   * duels -> ingestion.duels_writer.DUELS_DDL (the 6 Glicko-2 metric cols + duel_winner_source
--     provenance + native ids + access_tier).
--
-- Idempotent by construction (CREATE TABLE IF NOT EXISTS — a re-run is a no-op). Operator-applied WITH
-- the merge (there is NO CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/2026-09-17-add-duels.sql

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.duels (
    data_source STRING,
    match_id STRING,
    player_id STRING,
    team_id STRING,
    access_tier STRING,
    duel_rating DOUBLE,
    duel_rating_deviation DOUBLE,
    duel_volatility DOUBLE,
    duels_contested BIGINT,
    duels_won BIGINT,
    duels_lost BIGINT,
    duel_winner_source STRING,
    _ingested_at TIMESTAMP
) USING DELTA;
