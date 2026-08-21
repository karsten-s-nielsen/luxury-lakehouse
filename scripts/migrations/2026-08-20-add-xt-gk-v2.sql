-- scripts/migrations/2026-08-20-add-xt-gk-v2.sql
--
-- silly-kicks 4.87.0 full-adoption (spec §7.4, Chunk 17b): create the xT-GK v2 writer's OWN bronze
-- table so the dbt staging view (stg_xt_gk_v2) + the fct_action_context LEFT JOIN resolve on the LIVE
-- warehouse BEFORE the Part-B scoring run populates it (the phased-materialization pattern).
--
-- This is the TWO-TIER schema split (review-3 H-1): the 6 v2 columns are NOT added to
-- bronze.spadl_action_context (they are writer-scored, not drain-native). They live on this dedicated
-- table, written by ingestion.xt_gk_v2_writer (ADR-013). The v1 xt_gk metric columns already on
-- bronze.spadl_action_context are RETIRED at the head schema (ACTION_CONTEXT_DDL) but their physical
-- columns are LEFT IN PLACE here — a destructive DROP, if desired, is operator-driven per the
-- migrations convention; they simply stop being read.
--
-- Idempotent by construction: CREATE TABLE IF NOT EXISTS (a no-op once the table exists). The column
-- list mirrors ingestion.xt_gk_v2_writer.XT_GK_V2_DDL — keep them in sync.
--
-- Catalog hardcoded as `soccer_analytics` per the migrations convention (the runner does not perform
-- ${...} substitution). Operator-applied per the migrations convention (no CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-08-20-add-xt-gk-v2.sql

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.xt_gk_v2_predictions (
    data_source STRING,
    match_id STRING,
    action_id BIGINT,
    xt_gk_v2_position DOUBLE,
    xt_gk_v2_pev DOUBLE,
    xt_gk_v2_retention_loss DOUBLE,
    xt_gk_v2_dzv DOUBLE,
    xt_gk_v2 DOUBLE,
    gk_geometry_source STRING,
    _ingested_at TIMESTAMP
) USING DELTA;
