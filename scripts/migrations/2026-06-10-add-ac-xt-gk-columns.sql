-- AC-1 silly-kicks 4.22.0 (ADR-048): add the xT-GK (Eyestone, upstream ADR-024) column family
-- to bronze.spadl_action_context. Emitted by add_xt_gk / compute_xt_gk preset re-valuations /
-- add_gk_completion in the enrichment chain (enrich.py Steps 25/25b/26); NULL until the next
-- compute run (the full AC recompute wipes + repopulates this table anyway).
-- Composites are stored per philosophy preset (xt_gk = library default) because delta enters the
-- stored rav term and eta the unstored temporal factor — presets are NOT client-side derivable.
--
-- Operator-applied (there is NO CI auto-apply — see CLAUDE.md / reference_bronze_migration_autoapply_gap).
-- Idempotent: the runner skips ADD COLUMNS when the leading column (xt_gk) already exists
-- (DESCRIBE pre-check); Delta applies the column list atomically.
--
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-06-10-add-ac-xt-gk-columns.sql

ALTER TABLE soccer_analytics.bronze.spadl_action_context
  ADD COLUMNS (
    xt_gk DOUBLE,
    xt_gk_possession DOUBLE,
    xt_gk_counter DOUBLE,
    xt_gk_direct DOUBLE,
    xt_gk_high_press DOUBLE,
    xt_gk_low_block DOUBLE,
    xt_gk_base DOUBLE,
    xt_gk_pev DOUBLE,
    xt_gk_rav DOUBLE,
    xt_gk_dzv DOUBLE,
    xt_gk_pressure DOUBLE,
    xt_gk_origin_source STRING,
    xt_gk_dest_source STRING,
    xt_gk_origin_confidence DOUBLE,
    xt_gk_completion_variant STRING,
    xt_gk_completion_source STRING,
    gk_completion DOUBLE
  );
