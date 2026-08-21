-- scripts/migrations/2026-08-20-add-marts1-defensive-offball-bronze.sql
--
-- silly-kicks 4.87.0 full-adoption (spec §7.5, Chunk MARTS-1): create the OWN bronze tables for the
-- three Rev-6 tracking grain-mart writers so their dbt staging views + gold marts resolve on the LIVE
-- warehouse BEFORE the Part-B scoring run (Task 22b) populates them (the phased-materialization pattern,
-- mirroring 2026-08-20-add-xt-gk-v2.sql).
--
-- These are ADR-013 writer tables (NOT columns on bronze.spadl_action_context): the metrics are at a
-- DIFFERENT grain than per-action AC. Column lists mirror the writer DDL constants — keep them in sync:
--   * off_ball_runs                -> ingestion.off_ball_runs_writer.OFF_BALL_RUNS_DDL
--   * action_defensive_credit      -> ingestion.defensive_credit_writer.ACTION_DEFENSIVE_DDL
--   * defensive_credit_attributions-> ingestion.defensive_credit_writer.DEFENSIVE_CREDIT_ATTRIBUTIONS_DDL
--
-- Idempotent by construction: CREATE TABLE IF NOT EXISTS (a no-op once the table exists). Catalog
-- hardcoded as `soccer_analytics` per the migrations convention (the runner does not perform ${...}
-- substitution). Operator-applied per the migrations convention (no CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-08-20-add-marts1-defensive-offball-bronze.sql

-- fct_off_ball_runs source (Task 17e) — grain (action, runner).
CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.off_ball_runs (
    data_source STRING,
    match_id STRING,
    game_id BIGINT,
    period_id BIGINT,
    action_id BIGINT,
    player_id STRING,
    run_start_x DOUBLE,
    run_start_y DOUBLE,
    run_end_x DOUBLE,
    run_end_y DOUBLE,
    displacement_m DOUBLE,
    duration_s DOUBLE,
    mean_speed_ms DOUBLE,
    peak_speed_ms DOUBLE,
    peak_speed_source STRING,
    toward_goal BOOLEAN,
    role STRING,
    is_receiver BOOLEAN,
    run_value DOUBLE,
    enabled_pass_credit DOUBLE,
    _ingested_at TIMESTAMP
) USING DELTA;

-- fct_action_defensive source (Task 17d) — per-action defending-team credit aggregate.
-- period_id carried for per-(match, period) replaceWhere idempotency (IDSSE per-period units).
CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.action_defensive_credit (
    data_source STRING,
    match_id STRING,
    period_id BIGINT,
    action_id BIGINT,
    defensive_credit_net DOUBLE,
    defensive_credit_plus DOUBLE,
    defensive_credit_minus DOUBLE,
    n_defensive_credits BIGINT,
    _ingested_at TIMESTAMP
) USING DELTA;

-- fct_defensive_credit_attributions source (Task 17f) — long-form (action, player, rule).
CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.defensive_credit_attributions (
    data_source STRING,
    match_id STRING,
    game_id BIGINT,
    period_id BIGINT,
    action_id BIGINT,
    player_id STRING,
    team_id STRING,
    rule STRING,
    signed_value DOUBLE,
    anchor_type STRING,
    frame_id BIGINT,
    sizing STRING,
    resolution STRING,
    _ingested_at TIMESTAMP
) USING DELTA;
