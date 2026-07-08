-- scripts/migrations/2026-07-08-shot-freeze-frames-access-tier.sql
--
-- Add + backfill access_tier on bronze.shot_freeze_frames (ADR-064 per-match access tier).
--
-- The pre-shot freeze-frame set (Task 0.5) is written by the compute_shot_freeze_frames driver
-- (``ingestion.shot_freeze_frames``). This migration adds the ``access_tier STRING`` column (now the
-- canonical head schema in 2026-07-05-shot-freeze-frames-ddl.sql) to any already-materialized table
-- and backfills existing rows from the authoritative per-match tier in gold ``dim_matches``:
-- gradientsports / GS-RM / private skillcorner -> 'restricted'; A-League skillcorner + open-data
-- (statsbomb/wyscout/idsse/metrica) -> 'public'. A downstream HF publisher's ``split_restricted``
-- fail-safes NULL -> restricted, so an unstamped row is never leaked as public.
--
-- Idempotent by construction:
--   * single-leading-column ADD COLUMNS — ``_runner.py``'s DESCRIBE skip-if-exists makes the ALTER a
--     no-op once the column exists;
--   * the MERGE updates ONLY rows whose ``access_tier`` is still NULL, so re-running is a no-op once
--     stamped (mirrors 2026-07-06-backfill-fct-action-values-access-tier.sql).
--
-- Catalog hardcoded as `soccer_analytics` per the migrations convention (the runner does not perform
-- ${...} substitution). Operator-applied per the migrations convention (no CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-07-08-shot-freeze-frames-access-tier.sql

ALTER TABLE soccer_analytics.bronze.shot_freeze_frames
ADD COLUMNS (access_tier STRING);

MERGE INTO soccer_analytics.bronze.shot_freeze_frames sff
USING soccer_analytics.dev_gold.dim_matches dm
ON sff.match_key = dm.match_key
WHEN MATCHED AND sff.access_tier IS NULL THEN UPDATE SET sff.access_tier = dm.access_tier;
