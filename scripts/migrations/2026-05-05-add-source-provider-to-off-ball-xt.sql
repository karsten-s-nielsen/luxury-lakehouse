-- Migration: Add source_provider column to bronze.off_ball_xt_results
-- Date: 2026-05-05
-- Purpose: Write source_provider at bronze ingestion time instead of deriving
--          in staging from match_id patterns. Fixes silent misclassification
--          where bare match_ids defaulted to 'skillcorner'.
--
-- Idempotent: ALTER TABLE ADD COLUMNS skips if column exists.
-- Backfill: UPDATE only touches rows where source_provider IS NULL.

-- Step 1: Add the column (idempotent - Delta Lake ignores if exists)
ALTER TABLE soccer_analytics.bronze.off_ball_xt_results
ADD COLUMNS (source_provider STRING COMMENT 'Data source provider (idsse, metrica, skillcorner)');

-- Step 2: Backfill existing rows using the legacy derivation logic.
-- This matches the staging model's COALESCE fallback for backwards compatibility.
UPDATE soccer_analytics.bronze.off_ball_xt_results
SET source_provider = CASE
    WHEN match_id LIKE 'idsse_%'        THEN 'idsse'
    WHEN match_id LIKE 'Sample_Game_%'  THEN 'metrica'
    -- SkillCorner: numeric IDs or legacy bare IDSSE IDs (J03W*)
    -- For bare J03W* IDs, we KNOW they're IDSSE from the duplicate analysis.
    WHEN match_id RLIKE '^J0[0-9A-Z]+$' THEN 'idsse'
    ELSE 'skillcorner'
END
WHERE source_provider IS NULL;

-- Step 3: Delete legacy prefixed rows that conflict with unprefixed rows.
-- The new writer (post-PR-257) writes match_id without the 'idsse_' prefix.
-- After backfill, both 'idsse_J03WMX' and 'J03WMX' exist with source_provider='idsse'.
-- Staging normalizes with regexp_replace(match_id, '^idsse_', ''), causing duplicates.
-- Delete the prefixed rows since the new writer uses unprefixed IDs.
DELETE FROM soccer_analytics.bronze.off_ball_xt_results
WHERE match_id LIKE 'idsse_%';
