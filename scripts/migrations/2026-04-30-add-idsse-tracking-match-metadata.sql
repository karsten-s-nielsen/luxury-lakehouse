-- Session 69 (2026-04-30): close the asymmetric-coverage gap between
-- bronze.idsse_events (carries match-level metadata via PR-LL2) and
-- bronze.idsse_tracking (was missing them — staging compensated by
-- hardcoding the competition mapping in dbt SQL).
--
-- Adds 4 native-id columns sourced from the DFL <General> element of the
-- matchinformation XML. Wheel 0.3.27+ tracking writer (ingest_idsse) calls
-- _parse_match_metadata and emits these per row; finalize_bronze_df
-- guarantees they land in Delta.
--
-- Idempotent via scripts/migrations/_runner.py (the runner skips
-- ALTER TABLE ADD COLUMNS when the column already exists).
--
-- Post-deploy operator steps (NOT in this script — explicit operator
-- approval required, mirrors session 66 G7 / session 69 cleanup pattern):
--
--   1. Run this migration (idempotent ALTER).
--   2. DELETE FROM soccer_analytics.bronze.idsse_tracking — clears the 6
--      bare-format matches that were ingested with the old wheel and
--      lack the new metadata columns. The deep-clone backup at
--      bronze.idsse_tracking_pre_session69_backup remains as recovery.
--   3. Trigger soccer-analytics-ingestion-dev (job_id 302697362345215).
--      preflight_idsse detects all 7 matches missing → for_each_task
--      fan-out re-ingests with the new wheel + XML-sourced metadata.
--   4. After dbt_build green: drop bronze.idsse_tracking_pre_session69_backup.

ALTER TABLE soccer_analytics.bronze.idsse_tracking
  ADD COLUMNS (competition_native_id STRING);

ALTER TABLE soccer_analytics.bronze.idsse_tracking
  ADD COLUMNS (season_native_id STRING);

ALTER TABLE soccer_analytics.bronze.idsse_tracking
  ADD COLUMNS (home_team_id_native STRING);

ALTER TABLE soccer_analytics.bronze.idsse_tracking
  ADD COLUMNS (away_team_id_native STRING);
