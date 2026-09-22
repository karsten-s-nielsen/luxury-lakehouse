-- 2026-09-21-add-vaep-adjusted-columns.sql
-- sk4118 Phase D (silly-kicks 4.116/4.121 TF-61 VAEP_adjusted + xSuccess): the VAEP scoring UDF now
-- additionally emits the outcome-bias-free adjusted VAEP decomposition + the per-action completion
-- probability, declared in _VAEP_SCHEMA (src/ingestion/spadl_vaep.py) and SELECTed by
-- stg_spadl__action_values -> fct_action_values, so live bronze.vaep_action_values must carry them.
--
-- write_delta_table(replace_where=...) already runs with mergeSchema=true, so the drain would evolve
-- these columns on its next write; this migration is applied WITH the merge anyway so the dbt
-- staging SELECT of the 4 columns resolves BEFORE the Phase D re-materialize re-scores (otherwise the
-- next daily dbt-live build casts a column that does not yet exist). Same phased-materialization
-- pattern as the sk4870 drain-native / elastic_receive_* migrations.
--
-- NO backfill: values require re-running VAEP scoring (rate_adjusted). Existing rows stay NULL until
-- the P2 VAEP re-materialize populates them.
--
-- Idempotent by construction: one ADD COLUMNS with a single leading column — _runner.py's DESCRIBE
-- skip-if-exists makes the whole ALTER a no-op once offensive_adjusted_value exists. Catalog hardcoded
-- soccer_analytics per the migrations convention (the runner does no ${...} substitution).
-- Operator-applied per the migrations convention (no CI auto-apply); apply WITH the merge:
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-09-21-add-vaep-adjusted-columns.sql
-- Verify: DESCRIBE soccer_analytics.bronze.vaep_action_values -- expect the 4 columns present.

ALTER TABLE soccer_analytics.bronze.vaep_action_values ADD COLUMNS (
    offensive_adjusted_value DOUBLE,
    defensive_adjusted_value DOUBLE,
    vaep_adjusted_value DOUBLE,
    xsuccess DOUBLE
);
