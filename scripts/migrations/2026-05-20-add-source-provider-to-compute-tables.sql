-- 2026-05-20-add-source-provider-to-compute-tables.sql
-- Add source_provider column to bronze compute tables that were missing it,
-- and backfill existing rows from match_id format (ADR-018 conventions).
--
-- Affected tables:
--   formation_labels      — never had source_provider; EFPI + shape graph
--                           writers now propagate it from fct_tracking_frames.
--   space_creation_values — imported from HF Hub parquet; import_space_creation
--                           now derives it at write time.
--
-- Also strips legacy provider prefixes from space_creation_values.match_id
-- (same root cause as the prefix-strip migration for the other 3 tables).
--
-- Idempotent: ALTER TABLE ADD COLUMNS is a no-op if column exists (runner
-- pre-check). UPDATE WHERE source_provider IS NULL is a no-op if backfilled.
-- regexp_replace on already-bare IDs is a no-op.

-- -----------------------------------------------------------------------
-- 1. formation_labels — add column + backfill
-- -----------------------------------------------------------------------

ALTER TABLE soccer_analytics.bronze.formation_labels
  ADD COLUMNS (source_provider STRING);

-- Backfill from bare match_id format (ADR-018 conventions).
-- Safe to run before or after the operator-run prefix-strip migration:
-- prefixed rows get NULL source_provider here, then get deleted by the
-- strip migration. No default — unknown formats stay NULL.
UPDATE soccer_analytics.bronze.formation_labels
   SET source_provider = CASE
         WHEN match_id LIKE 'Sample_Game_%'        THEN 'metrica'
         WHEN match_id RLIKE '^[0-9]+$'            THEN 'skillcorner'
         WHEN match_id RLIKE '^[A-Z]'              THEN 'idsse'
       END
 WHERE source_provider IS NULL;

-- -----------------------------------------------------------------------
-- 2. space_creation_values — add column + backfill + strip prefixes
-- -----------------------------------------------------------------------

ALTER TABLE soccer_analytics.bronze.space_creation_values
  ADD COLUMNS (source_provider STRING);

-- Backfill source_provider BEFORE stripping prefixes (so we can use the
-- prefix to identify the provider for prefixed rows).
UPDATE soccer_analytics.bronze.space_creation_values
   SET source_provider = CASE
         WHEN match_id LIKE 'idsse_%'              THEN 'idsse'
         WHEN match_id LIKE 'skillcorner_%'        THEN 'skillcorner'
         WHEN match_id LIKE 'Sample_Game_%'        THEN 'metrica'
         WHEN match_id RLIKE '^[0-9]+$'            THEN 'skillcorner'
         WHEN match_id RLIKE '^[A-Z]'              THEN 'idsse'
       END
 WHERE source_provider IS NULL;

-- Strip legacy provider prefixes from match_id so JOINs to dim_matches
-- resolve on bare native IDs per ADR-018.
UPDATE soccer_analytics.bronze.space_creation_values
   SET match_id = regexp_replace(match_id, '^(idsse_|skillcorner_)', '')
 WHERE match_id RLIKE '^(idsse_|skillcorner_)';
