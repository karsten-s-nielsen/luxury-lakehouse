-- silly-kicks 4.13.0 (sk ADR-018): add is_synthetic BOOLEAN provenance flag to the
-- SPADL/VAEP bronze tables.
--
-- is_synthetic marks rows synthesized by the converter rather than observed in
-- the source feed. silly-kicks emits it natively only on the Gradient Sports
-- converter (True on synthesized foul restarts + cross-goal shots); the SPADL
-- writer manufactures False on the other 5 providers so the column is a uniform,
-- non-NULL cross-provider flag (False = genuine observed action).
--
-- Backfill existing rows to false (maintainer decision, 2026-06-04): all rows
-- written before this change are non-synthetic by construction (GS row synthesis
-- is new in 4.13.0 and lands only on re-ingest), so false is correct, not NULL.
-- Incremental SPADL conversion + VAEP scoring only write NEW matches, so existing
-- rows are never rewritten — the UPDATE is the only path that reaches them.
--
-- Idempotent: ADD COLUMNS is skipped if the column already exists (runner
-- skip-if-exists pre-check); the UPDATE ... WHERE is_synthetic IS NULL is a no-op
-- once every row is backfilled.

ALTER TABLE soccer_analytics.bronze.spadl_actions
  ADD COLUMNS (is_synthetic BOOLEAN);

UPDATE soccer_analytics.bronze.spadl_actions
  SET is_synthetic = false
  WHERE is_synthetic IS NULL;

ALTER TABLE soccer_analytics.bronze.vaep_action_values
  ADD COLUMNS (is_synthetic BOOLEAN);

UPDATE soccer_analytics.bronze.vaep_action_values
  SET is_synthetic = false
  WHERE is_synthetic IS NULL;
