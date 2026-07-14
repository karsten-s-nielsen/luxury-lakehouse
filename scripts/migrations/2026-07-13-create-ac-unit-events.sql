-- scripts/migrations/2026-07-13-create-ac-unit-events.sql
--
-- D9 — AC-1 per-unit lifecycle event log (ADR-068). Persists what the work queue cannot:
-- what actually RAN. The drain-completeness gate (D8) reads it to prove every enqueued unit
-- reached a terminal state, and that every worker (including sb360) said it finished.
--
-- TOPOLOGY: ONE TABLE PER WRITER + a UNION ALL VIEW.
-- D9 makes ~390 one-row commits from 8 concurrent drivers. Measured (2026-07-13): a single
-- shared table cost p50 9.7 s per append at 8-way concurrency vs 1.66 s uncontended -- 13x over
-- the pre-registered 750 ms threshold. So we take ADR-038's own elimination route (b), "split
-- into multiple tables": each writer owns its own _delta_log and contention is STRUCTURALLY
-- IMPOSSIBLE, not merely retried. Consumers read the VIEW (action_context_unit_events) only;
-- the per-worker table names must never leak into the gate.
--
-- PARTITIONED BY (event_date) is for RETENTION (a partition drop, not a tombstone-generating
-- DELETE) and read-pruning ONLY. Partitioning is NOT a contention control (ADR-038).
--
-- Lives in the `observability` schema (platform operational metadata), NOT bronze (it is not
-- ingested source truth). Catalog hardcoded `soccer_analytics` per the migration convention.
--
-- Idempotent (CREATE TABLE IF NOT EXISTS / CREATE OR REPLACE VIEW). Operator-applied WITH the
-- merge: `uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/2026-07-13-create-ac-unit-events.sql`
--
-- Column list is parity-tested against ingestion.action_context_queue._EVENT_COLUMNS by
-- src/tests/action_context/test_unit_event_sink.py (single source of truth).

CREATE TABLE IF NOT EXISTS soccer_analytics.observability.action_context_unit_events_w0 (
  run_id STRING,
  worker_id INT,
  provider STRING,
  match_id STRING,
  period INT,
  state STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  rows_written BIGINT,
  error STRING,
  write_failures INT,
  event_date DATE,
  _ingested_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS soccer_analytics.observability.action_context_unit_events_w1 (
  run_id STRING,
  worker_id INT,
  provider STRING,
  match_id STRING,
  period INT,
  state STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  rows_written BIGINT,
  error STRING,
  write_failures INT,
  event_date DATE,
  _ingested_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS soccer_analytics.observability.action_context_unit_events_w2 (
  run_id STRING,
  worker_id INT,
  provider STRING,
  match_id STRING,
  period INT,
  state STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  rows_written BIGINT,
  error STRING,
  write_failures INT,
  event_date DATE,
  _ingested_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS soccer_analytics.observability.action_context_unit_events_w3 (
  run_id STRING,
  worker_id INT,
  provider STRING,
  match_id STRING,
  period INT,
  state STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  rows_written BIGINT,
  error STRING,
  write_failures INT,
  event_date DATE,
  _ingested_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS soccer_analytics.observability.action_context_unit_events_w4 (
  run_id STRING,
  worker_id INT,
  provider STRING,
  match_id STRING,
  period INT,
  state STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  rows_written BIGINT,
  error STRING,
  write_failures INT,
  event_date DATE,
  _ingested_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS soccer_analytics.observability.action_context_unit_events_w5 (
  run_id STRING,
  worker_id INT,
  provider STRING,
  match_id STRING,
  period INT,
  state STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  rows_written BIGINT,
  error STRING,
  write_failures INT,
  event_date DATE,
  _ingested_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS soccer_analytics.observability.action_context_unit_events_w6 (
  run_id STRING,
  worker_id INT,
  provider STRING,
  match_id STRING,
  period INT,
  state STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  rows_written BIGINT,
  error STRING,
  write_failures INT,
  event_date DATE,
  _ingested_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS soccer_analytics.observability.action_context_unit_events_w7 (
  run_id STRING,
  worker_id INT,
  provider STRING,
  match_id STRING,
  period INT,
  state STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  rows_written BIGINT,
  error STRING,
  write_failures INT,
  event_date DATE,
  _ingested_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date);

-- sb360 EXITS the per-match drain (ADR-058): its own task, its own lifecycle, no queue rows and
-- no worker_id -- so it writes under the SB360_WORKER_ID sentinel (-1) to its own table.
CREATE TABLE IF NOT EXISTS soccer_analytics.observability.action_context_unit_events_sb360 (
  run_id STRING,
  worker_id INT,
  provider STRING,
  match_id STRING,
  period INT,
  state STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  rows_written BIGINT,
  error STRING,
  write_failures INT,
  event_date DATE,
  _ingested_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date);

-- The ONLY name a consumer (the D8 gate) reads. Same columns as a single table would have.
CREATE OR REPLACE VIEW soccer_analytics.observability.action_context_unit_events AS
SELECT * FROM soccer_analytics.observability.action_context_unit_events_w0
UNION ALL SELECT * FROM soccer_analytics.observability.action_context_unit_events_w1
UNION ALL SELECT * FROM soccer_analytics.observability.action_context_unit_events_w2
UNION ALL SELECT * FROM soccer_analytics.observability.action_context_unit_events_w3
UNION ALL SELECT * FROM soccer_analytics.observability.action_context_unit_events_w4
UNION ALL SELECT * FROM soccer_analytics.observability.action_context_unit_events_w5
UNION ALL SELECT * FROM soccer_analytics.observability.action_context_unit_events_w6
UNION ALL SELECT * FROM soccer_analytics.observability.action_context_unit_events_w7
UNION ALL SELECT * FROM soccer_analytics.observability.action_context_unit_events_sb360;
