-- PR-1.5 (SK3-MIG-B) — add source_provider to bronze.player_positions.
-- Ref: stg_shape_graphs__positions needed pattern-matching to derive provider
-- from match_id format. This migration adds the column and backfills existing
-- rows with the derived value so staging can use the column directly.
--
-- Idempotency: the executor wraps ALTER TABLE ADD COLUMNS in a DESCRIBE
-- pre-check and skips if the column already exists. The UPDATE WHERE IS NULL
-- pattern is also idempotent.

ALTER TABLE soccer_analytics.bronze.player_positions
  ADD COLUMNS (source_provider STRING);

-- Backfill existing rows using the same derivation logic the staging used.
-- SkillCorner: match_id like 'skillcorner_%'
-- Metrica: match_id like 'Sample_Game_%'
-- IDSSE: everything else (with or without 'idsse_' prefix)
UPDATE soccer_analytics.bronze.player_positions
   SET source_provider = CASE
         WHEN match_id LIKE 'skillcorner_%' THEN 'skillcorner'
         WHEN match_id LIKE 'Sample_Game_%' THEN 'metrica'
         WHEN match_id LIKE 'idsse_%' THEN 'idsse'
         ELSE 'idsse'
       END
 WHERE source_provider IS NULL;
