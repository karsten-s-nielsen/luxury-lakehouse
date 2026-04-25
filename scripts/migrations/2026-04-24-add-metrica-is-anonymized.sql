-- Kimball PR 5a — add is_anonymized flag to Metrica bronze tables.
-- Ref: docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §4
-- Current data is 100% sample CSV — backfill to true.
-- Future subscription-path ingestion sets false at write time.
--
-- Idempotency: the executor wraps ALTER TABLE ADD COLUMNS in a DESCRIBE
-- pre-check and skips if the column already exists. Databricks SQL does
-- not support ADD COLUMN IF NOT EXISTS in the ALTER TABLE clause.

ALTER TABLE soccer_analytics.bronze.metrica_tracking
  ADD COLUMNS (is_anonymized BOOLEAN);

UPDATE soccer_analytics.bronze.metrica_tracking
   SET is_anonymized = true
 WHERE is_anonymized IS NULL;

ALTER TABLE soccer_analytics.bronze.tracking_player_metadata
  ADD COLUMNS (is_anonymized BOOLEAN);

-- tracking_player_metadata covers IDSSE + SkillCorner today, both real identity. Default false.
UPDATE soccer_analytics.bronze.tracking_player_metadata
   SET is_anonymized = false
 WHERE is_anonymized IS NULL;
