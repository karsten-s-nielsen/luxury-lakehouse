-- scripts/migrations/2026-08-25-create-gkdv-observations.sql
--
-- Tracking-marts drain fan-out (ADR-037/068 reuse): the gkdv scoring/pooling split. The per-unit
-- ``tracking_marts`` drain worker (ingestion.tracking_marts_processor.TrackingMartsProcessor) scores one
-- unit at a time and appends the per-scored-keeper-frame observations to THIS intermediate; the
-- whole-corpus ``pool_keepers`` reduce then runs later in a separate single-driver ``gkdv_pool`` task and
-- writes bronze.gkdv_keeper_pooled. Pooling is cross-game, so it cannot be done per unit.
--
-- Column list mirrors the writer schema constant — keep in sync:
--   * gkdv_observations -> schema derived from ingestion.tracking_marts_processor._GKDV_OBS_COLUMNS
--
-- Idempotent by construction: CREATE TABLE IF NOT EXISTS (a no-op once the table exists). Catalog
-- hardcoded as `soccer_analytics` per the migrations convention (the runner does not perform ${...}
-- substitution). Operator-applied per the migrations convention (no CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-08-25-create-gkdv-observations.sql

-- gkdv per-frame keeper observations (drain intermediate) — grain (unit, scored defending-keeper frame);
-- native (data_source, match_id/game_id, competition_id, season_id, player_id). game_id == match_id (the
-- native match id -> aggregate_by_keeper n_games); match_id + period_id carry the per-unit replaceWhere.
CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.gkdv_observations (
    data_source STRING,
    match_id STRING,
    game_id STRING,
    competition_id STRING,
    season_id STRING,
    player_id STRING,
    period_id BIGINT,
    frame_id BIGINT,
    delta_das DOUBLE,
    delta_threat_suppression DOUBLE,
    _ingested_at TIMESTAMP
) USING DELTA;
