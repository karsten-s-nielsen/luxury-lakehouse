-- SPADL silly-kicks 4.31.0 (Lamberts 2025 GVM; ADR-056): add the GK distribution-metric column
-- family to bronze.spadl_actions + bronze.vaep_action_values. Emitted by
-- add_gk_distribution_metrics in apply_spadl_enrichments (spadl_enrichments.py) and carried through
-- the SPADL-conversion + VAEP-scoring UDFs. Actions-level: non-NULL only on successful GK
-- distribution passes (gk_role == 'distribution'); NULL elsewhere. gk_xt_delta is NOT stored here —
-- it is derived in fct_action_values via a JOIN to the canonical bronze.expected_threat_grids
-- (single xT source of truth, ADR-056).
--
-- Operator-applied (there is NO CI auto-apply — see CLAUDE.md / reference_bronze_migration_autoapply_gap).
-- Idempotent: the runner DESCRIBE-skips each ADD COLUMNS when the leading column (gk_pass_length_m)
-- already exists; Delta applies the column list atomically. NULL until the next SPADL re-conversion
-- repopulates the rows.
--
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-06-17-add-spadl-gk-distribution-metrics.sql

ALTER TABLE soccer_analytics.bronze.spadl_actions
  ADD COLUMNS (
    gk_pass_length_m DOUBLE,
    gk_pass_length_class STRING,
    is_launch BOOLEAN
  );

ALTER TABLE soccer_analytics.bronze.vaep_action_values
  ADD COLUMNS (
    gk_pass_length_m DOUBLE,
    gk_pass_length_class STRING,
    is_launch BOOLEAN
  );
