-- scripts/migrations/2026-07-11-add-is-gk-distribution-to-spadl-action-context.sql
--
-- Add is_gk_distribution BOOLEAN to bronze.spadl_action_context (F1 — GK-distribution domain marker).
--
-- The action-context driver (``ingestion.action_context``) now writes ``is_gk_distribution`` — True for
-- any goal-kick OR an open-play pass/throw-in whose actor is the acting-team GK (silly-kicks 4.43.0
-- ``gk_distribution_mask``). It is now part of ``ACTION_CONTEXT_DDL`` (the canonical head schema), so
-- fresh tables get it via ``ensure_table``; this migration adds it to the already-materialized live
-- table so the gold mart's ``SELECT is_gk_distribution`` resolves (mirrors the ghost_gk_* drift that
-- the live-DDL parity test, ``test_action_context_live_ddl_parity.py``, exists to catch).
--
-- NO backfill: unlike access_tier, this value cannot be derived by a MERGE from a dimension — it
-- requires re-running the frame-based enrichment (goal-kick OR acting-GK-pass resolution). Existing
-- rows therefore stay NULL until the Phase-5 AC recompute (GS/SC tracking + SB360) re-materializes
-- them. This is the intended phased-materialization design: silly-kicks' rho retention loader reads the
-- column with COALESCE(is_gk_distribution, FALSE), so pre-recompute NULLs are safe (never corrupt the
-- retrain — they simply read as not-a-distribution until the row is recomputed).
--
-- Idempotent by construction: single-leading-column ADD COLUMNS — ``_runner.py``'s DESCRIBE
-- skip-if-exists makes the ALTER a no-op once the column exists.
--
-- Catalog hardcoded as `soccer_analytics` per the migrations convention (the runner does not perform
-- ${...} substitution). Operator-applied per the migrations convention (no CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-07-11-add-is-gk-distribution-to-spadl-action-context.sql

ALTER TABLE soccer_analytics.bronze.spadl_action_context
ADD COLUMNS (is_gk_distribution BOOLEAN);
