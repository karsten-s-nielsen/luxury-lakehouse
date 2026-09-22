-- 2026-09-20-add-ac-elastic-receive-columns.sql
-- sk4118 P0 (silly-kicks 4.114 TF-57 ELASTIC v2): the AC drain's add_elastic_sync now additionally
-- anchors each action's RECEPTION touch, emitting 3 new columns. They are declared in
-- ACTION_CONTEXT_DDL (src/analytics/action_context/schema.py) and SELECTed by
-- stg_action_context__values -> fct_action_context, so live bronze.spadl_action_context must carry
-- them or the gold mart's SELECT breaks (and test_action_context_live_ddl_parity fails).
--
-- NO backfill: values require re-running the frame-based AC enrichment. Existing rows stay NULL
-- until the P2 AC re-materialize populates them — the intended phased-materialization design
-- (mirrors is_gk_distribution / access_tier / the sk4870 drain-native migrations).
--
-- Idempotent by construction: one single-leading-column ADD COLUMNS — _runner.py's DESCRIBE
-- skip-if-exists makes the ALTER a no-op once elastic_receive_frame_id exists. Catalog hardcoded
-- soccer_analytics per the migrations convention (the runner does no ${...} substitution).
-- Operator-applied per the migrations convention (no CI auto-apply); apply WITH the merge:
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-09-20-add-ac-elastic-receive-columns.sql
-- Verify: DESCRIBE soccer_analytics.bronze.spadl_action_context -- expect the 3 columns present.

ALTER TABLE soccer_analytics.bronze.spadl_action_context ADD COLUMNS (
    elastic_receive_frame_id BIGINT,
    elastic_receive_confidence DOUBLE,
    elastic_receive_error_seconds DOUBLE
);
