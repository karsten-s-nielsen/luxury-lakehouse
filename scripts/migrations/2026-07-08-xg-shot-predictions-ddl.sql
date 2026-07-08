-- scripts/migrations/2026-07-08-xg-shot-predictions-ddl.sql
--
-- Canonical-SPADL Pre-Shot xG Unification (Task 1.9, spec §7) — the per-shot pre-shot xG
-- predictions written by the ``compute_xg_shot_scores`` mega-job task
-- (``ingestion.xg_shot_scorer``). One row per shot, keyed on ``(match_key, action_id)``.
-- The scorer loads ``xg_model_v3@Champion`` (raw xG) + the shipped per-provider OOF
-- calibrators, applies the single per-provider calibrator (pooled fallback -> ood_flag),
-- runs the two-mode gate, and writes here via ``ingestion.utils.write_delta_table``
-- (replaceWhere keyed on ``match_key`` for idempotency).
--
-- Column list is pinned to ``xg_shot_scorer._XG_SHOT_PRED_COLUMNS`` by the ADR-002 §4
-- schema-drift guard ``src/tests/test_xg_shot_scorer.py`` — the two sources of truth cannot
-- drift without a failing test. ``_ingested_at`` is appended by ``write_delta_table`` (NOT
-- emitted by the scorer) and must stay the LAST column here.
--
-- ``match_id_native`` is the ADR-013 native identifier carried through from
-- ``fct_action_values`` (bronze traceability); the Kimball surrogates are resolved
-- downstream in the ``fct_shot_xg`` mart via an INNER JOIN to ``fct_action_values`` on
-- ``(match_key, action_id)``.
--
-- Idempotent (IF NOT EXISTS guard). Operator-applied per the migrations convention
-- (no CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-07-08-xg-shot-predictions-ddl.sql
--
-- Liquid clustering on ``match_key`` mirrors the bronze mart-table default (the writer's
-- replaceWhere predicate filters on ``match_key``, so clustering enables file skipping).
--
-- Catalog hardcoded as `soccer_analytics` per existing migration convention (the runner
-- does not perform ${...} substitution).

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.xg_shot_predictions (
  match_id_native STRING,
  match_key BIGINT,
  action_id BIGINT,
  data_source STRING,
  xg DOUBLE,
  xg_ci_low DOUBLE,
  xg_ci_high DOUBLE,
  scoring_mode STRING,
  ood_flag BOOLEAN,
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
ALTER TABLE soccer_analytics.bronze.xg_shot_predictions
CLUSTER BY (match_key);
