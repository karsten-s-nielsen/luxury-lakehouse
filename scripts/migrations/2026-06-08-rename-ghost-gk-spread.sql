-- AC-1 silly-kicks 4.14.0: rename ghost_gk_spread -> ghost_gk_density_spread on
-- bronze.spadl_action_context (served value is now the boosted-HGBR mean; the spread is the
-- conditional-density dispersion, not the standard error of the served point).
--
-- RUN ONCE. Operator-applied (there is NO CI auto-apply). NOT idempotent — the runner has no
-- RENAME idempotency (it runs the statement unconditionally). Before running, confirm the source
-- column still exists:
--   DESCRIBE soccer_analytics.bronze.spadl_action_context;   -- expect ghost_gk_spread present
-- Delta column-mapping is a one-way protocol bump (minReader=2/minWriter=5) — irreversible on this
-- table (see ADR-042).
--
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-06-08-rename-ghost-gk-spread.sql

ALTER TABLE soccer_analytics.bronze.spadl_action_context SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
);

ALTER TABLE soccer_analytics.bronze.spadl_action_context
  RENAME COLUMN ghost_gk_spread TO ghost_gk_density_spread;
