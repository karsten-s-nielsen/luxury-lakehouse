-- scripts/migrations/2026-08-20-add-marts2-bravery-gkdv-bronze.sql
--
-- silly-kicks 4.87.0 full-adoption (spec §7.5, Chunk MARTS-2): create the OWN bronze tables for the
-- two evaluative families that EXTEND existing grain-marts, so their dbt staging views + gold marts
-- resolve on the LIVE warehouse BEFORE the Part-B scoring run (Task 22b) populates them (the phased-
-- materialization pattern, mirroring 2026-08-20-add-marts1-defensive-offball-bronze.sql).
--
-- These are ADR-013 writer tables. Column lists mirror the writer DDL constants — keep them in sync:
--   * bravery             -> ingestion.bravery_writer.BRAVERY_DDL           (Task 17g, fct_match_summary)
--   * gkdv_keeper_pooled  -> ingestion.gkdv_writer.GKDV_KEEPER_POOLED_DDL   (Task 17h, fct_gk_shot_stopping_pooled)
--
-- Idempotent by construction: CREATE TABLE IF NOT EXISTS (a no-op once the table exists). Catalog
-- hardcoded as `soccer_analytics` per the migrations convention (the runner does not perform ${...}
-- substitution). Operator-applied per the migrations convention (no CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-08-20-add-marts2-bravery-gkdv-bronze.sql

-- fct_match_summary bravery source (Task 17g) — grain (match, DEFENDING team); native (match_id, team_id).
CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.bravery (
    data_source STRING,
    match_id STRING,
    team_id STRING,
    bravery_shots DOUBLE,
    bravery_open_play_crosses DOUBLE,
    bravery_set_piece_crosses DOUBLE,
    bravery_pct_known_domain DOUBLE,
    n_shots_faced BIGINT,
    n_open_play_crosses_faced BIGINT,
    n_set_piece_crosses_faced BIGINT,
    n_blocks_known BIGINT,
    _ingested_at TIMESTAMP
) USING DELTA;

-- fct_gk_shot_stopping_pooled gkdv source (Task 17h) — per-keeper-pooled over (competition, season);
-- native (player_id, competition_id, season_id).
CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.gkdv_keeper_pooled (
    data_source STRING,
    player_id STRING,
    competition_id STRING,
    season_id STRING,
    gkdv_delta_das_mean DOUBLE,
    gkdv_delta_das_median DOUBLE,
    gkdv_delta_das_n BIGINT,
    gkdv_delta_das_n_nonzero BIGINT,
    gkdv_delta_das_n_games BIGINT,
    gkdv_delta_das_gate_eligible BOOLEAN,
    gkdv_delta_threat_mean DOUBLE,
    gkdv_delta_threat_median DOUBLE,
    gkdv_delta_threat_n BIGINT,
    gkdv_delta_threat_n_nonzero BIGINT,
    gkdv_delta_threat_n_games BIGINT,
    gkdv_delta_threat_gate_eligible BOOLEAN,
    _ingested_at TIMESTAMP
) USING DELTA;
