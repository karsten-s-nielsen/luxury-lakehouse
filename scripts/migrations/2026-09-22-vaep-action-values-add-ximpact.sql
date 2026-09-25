-- scripts/migrations/2026-09-22-vaep-action-values-add-ximpact.sql
--
-- sk4123 (SK-XT-COUNTS PR, commit 2 — TF-63 xImpact ride-along, ADR-101): add the two new per-action
-- columns to the VAEP writer's bronze table so the staging view + fct_action_values contract resolve on
-- the LIVE warehouse BEFORE the P2 compute_spadl_vaep re-run backfills them corpus-wide.
--
--   * ximpact           -- VAEP_adjusted × ΔP(win | goal) (match-context-weighted action value)
--   * win_prob_leverage -- ΔP(win | goal) at the pre-action state (goal_leverage), the xImpact weight
--
-- Column list mirrors ingestion.spadl_vaep._VAEP_SCHEMA + the applyInPandas StructType (ADR-016 parity,
-- test_spadl_vaep_writer_parity). Both DOUBLE, NULL on rows scored before the re-materialize, NaN where
-- the win-probability state is unresolved (honest-NaN).
--
-- Idempotent: the _runner makes a single-leading-column ADD COLUMNS skip-if-exists (DESCRIBE on the first
-- column, `ximpact`). Operator-applied WITH the merge (there is NO CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-09-22-vaep-action-values-add-ximpact.sql

ALTER TABLE soccer_analytics.bronze.vaep_action_values
    ADD COLUMNS (ximpact DOUBLE, win_prob_leverage DOUBLE);
