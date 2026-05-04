-- scripts/migrations/2026-05-03-create-bronze-sk3-mig-b-runs.sql
--
-- SK3-MIG-B telemetry table — orchestrator cycle log per spec §5.3.
--
-- Idempotent (CREATE TABLE IF NOT EXISTS); auto-applied by
-- .github/workflows/dbt-live-ci.yml "Apply pending bronze migrations" step.
--
-- ADR-002 §4 schema-drift guard via test_sk3_mig_b_runs_schema_parity.py
-- pinning the column list against src/ingestion/sk3_mig_b_telemetry.py.
--
-- Catalog hardcoded as `soccer_analytics` per existing migration convention
-- (see prior migrations in this directory; the runner does not perform
-- ${...} substitution).

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.sk3_mig_b_runs (
  cycle_id STRING,
  cycle_started_at TIMESTAMP,
  cycle_finished_at TIMESTAMP,
  wheel_at_start STRING,
  wheel_at_end STRING,
  silly_kicks_version STRING,
  cost_cap_usd DOUBLE,
  walltime_cap_hours DOUBLE,
  cycle_item STRING,
  cycle_item_kind STRING,
  hf_job_id STRING,
  champion_set_at TIMESTAMP,
  pre_mart_version BIGINT,
  post_mart_version BIGINT,
  pre_hf_revision_sha STRING,
  smoke_pass BOOLEAN,
  smoke_metrics MAP<STRING, DOUBLE>,
  smoke_metrics_str MAP<STRING, STRING>,
  wall_clock_seconds DOUBLE,
  cost_usd DOUBLE,
  recorded_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = 'true'
);
