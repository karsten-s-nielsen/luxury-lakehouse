-- scripts/migrations/2026-07-05-shot-freeze-frames-ddl.sql
--
-- Canonical-SPADL Pre-Shot xG Unification (Task 0.5) — persisted per-(shot, player)
-- pre-shot freeze-frame set for the tracking providers (gradientsports / skillcorner /
-- idsse / metrica). One row per (shot action, player) emitted by
-- ``analytics.action_context.tracking_snapshots.build_tracking_snapshots`` and written by
-- ``build_tracking_snapshots``'s Spark writer via ``ingestion.utils.write_delta_table``
-- (replaceWhere keyed on ``match_key`` for idempotency).
--
-- Column list is pinned to ``tracking_snapshots._SHOT_FF_COLUMNS`` by the ADR-002 §4
-- schema-drift guard ``src/tests/action_context/test_shot_freeze_frames_writer.py`` — the
-- two sources of truth cannot drift without a failing test. ``_ingested_at`` is appended by
-- ``write_delta_table`` (NOT emitted by the builder) and must stay the LAST column here.
--
-- Idempotent (IF NOT EXISTS guard). Operator-applied per the migrations convention
-- (no CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-07-05-shot-freeze-frames-ddl.sql
--
-- Liquid clustering on ``match_key`` mirrors the bronze mart-table default (the writer's
-- replaceWhere predicate filters on ``match_key``, so clustering enables file skipping).
--
-- Catalog hardcoded as `soccer_analytics` per existing migration convention (the runner
-- does not perform ${...} substitution).

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.shot_freeze_frames (
  action_id BIGINT,
  match_key BIGINT,
  data_source STRING,
  player_id STRING,
  x DOUBLE,
  y DOUBLE,
  is_keeper INT,
  is_teammate INT,
  set_cardinality INT,
  shooter_attacks_high_x BOOLEAN,
  team_attacking_direction STRING,
  _ingested_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = 'true',
  delta.autoOptimize.optimizeWrite = 'true',
  delta.autoOptimize.autoCompact = 'true'
);

-- Liquid clustering (metadata-only ALTER; re-runnable). Existing data is reorganized
-- lazily by Predictive Optimization (enabled at catalog level).
ALTER TABLE soccer_analytics.bronze.shot_freeze_frames
CLUSTER BY (match_key);
