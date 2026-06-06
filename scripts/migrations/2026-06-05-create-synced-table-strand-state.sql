-- scripts/migrations/2026-06-05-create-synced-table-strand-state.sql
--
-- Per-table strand-state for synced-table checkpoint self-heal (spec H3 / review P1).
--
-- Append-only event log: the daily detect task (SP) appends a `stranded` event each time it sees a
-- synced table checkpoint-broken (DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE / SQLSTATE XXKST);
-- the privileged maintenance heal pass appends a `healed` event on a successful recreate.
-- Recurrence-RED = a table whose latest `stranded` is newer than its latest `healed` (see
-- ingestion.synced_table_strand_state.StrandStateStore.was_stranded_unhealed).
--
-- Append-only (not upsert) so the two writing identities never MERGE-conflict — writes go through
-- ingestion.utils.write_delta_table (ADR-038 concurrent-commit retry). Reads take MAX per event type
-- and are fail-open (a missing table -> "no prior strand"), so the daily job survives the first run
-- before this migration is applied.
--
-- Lives in `observability` (platform operational metadata), NOT bronze. Catalog hardcoded
-- `soccer_analytics` per the migration convention.
--
-- Idempotent (CREATE TABLE IF NOT EXISTS). Auto-applied by .github/workflows/dbt-live-ci.yml
-- "Apply pending bronze migrations" step because this PR also touches dbt_project/** (the CDF model
-- changes); manual fallback: `uv run --extra sdk python scripts/migrations/_runner.py <this file>`.

CREATE TABLE IF NOT EXISTS soccer_analytics.observability.synced_table_strand_state (
  table_name STRING,
  event_type STRING,
  event_at TIMESTAMP,
  _ingested_at TIMESTAMP
)
USING DELTA;
