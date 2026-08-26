-- scripts/migrations/2026-06-02-create-action-context-work-queue.sql
--
-- AC-1 worker-drain fan-out queue (ADR-037). Run-scoped orchestration scratch:
-- preflight LPT-bin-packs discovered units across N persistent workers; each
-- drain worker reads its slice by (run_id, worker_id) ORDER BY seq.
--
-- Lives in the `observability` schema (platform operational metadata), NOT bronze
-- (it is not ingested source truth). Catalog hardcoded `soccer_analytics` per the
-- migration convention.
--
-- Idempotent (CREATE TABLE IF NOT EXISTS); auto-applied by
-- .github/workflows/dbt-live-ci.yml "Apply pending bronze migrations" step.
--
-- Column list is parity-tested against ingestion.drain_adapters._QUEUE_COLUMNS
-- by src/tests/action_context/test_work_queue_schema_parity.py (single source of truth).

CREATE TABLE IF NOT EXISTS soccer_analytics.observability.action_context_work_queue (
  run_id STRING,
  worker_id INT,
  seq BIGINT,
  provider STRING,
  match_id STRING,
  period INT,
  frame_range_lo BIGINT,
  frame_range_hi BIGINT,
  est_cost DOUBLE,
  kde_backend STRING,
  _ingested_at TIMESTAMP
)
USING DELTA;
