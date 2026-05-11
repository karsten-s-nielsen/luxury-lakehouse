-- PR-LL3 S2: Add player_id_native STRING to bronze.spadl_actions.
-- Populated by all 4 SPADL UDFs (StatsBomb, Wyscout, IDSSE, Metrica)
-- post-deploy; NULL until re-ingest.
--
-- Idempotent: ADD COLUMNS is a no-op if column already exists
-- (runner handles skip-if-exists).

ALTER TABLE soccer_analytics.bronze.spadl_actions
  ADD COLUMNS (player_id_native STRING);
