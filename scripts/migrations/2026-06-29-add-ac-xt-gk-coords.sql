-- AC silly-kicks 4.36.0: add the four xT-GK resolved-coordinate audit columns to
-- bronze.spadl_action_context. Emitted by compute_xt_gk's `_COORD_COLS` and copied onto the AC
-- frame by add_xt_gk (enrich.py Step 25) — the EXACT origin/destination the grid lookups used,
-- including the imputed ~67% of goal-kick origins. LTR SPADL meters; NULL/NaN off-scope. Additive:
-- 4.36.0 changes NO existing xt_gk_* value (CHANGELOG); audit-only, NOT VAEP features. NULL until
-- the next xt_gk recompute (the AC re-materialize wipes + repopulates this table anyway).
--
-- Operator-applied (there is NO CI auto-apply — see CLAUDE.md / reference_bronze_migration_autoapply_gap).
-- Idempotent: the runner skips ADD COLUMNS when the leading column (xt_gk_origin_x) already exists
-- (DESCRIBE pre-check); Delta applies the column list atomically.
--
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-06-29-add-ac-xt-gk-coords.sql

ALTER TABLE soccer_analytics.bronze.spadl_action_context
  ADD COLUMNS (
    xt_gk_origin_x DOUBLE,
    xt_gk_origin_y DOUBLE,
    xt_gk_dest_x DOUBLE,
    xt_gk_dest_y DOUBLE
  );
