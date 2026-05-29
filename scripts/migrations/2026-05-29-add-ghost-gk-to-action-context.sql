-- AC-1 silly-kicks 3.24.0+ ghost_gk: add the defending-GK "ghost" position +
-- spread columns to bronze.spadl_action_context. Emitted by add_ghost_gk in the
-- action-context enrichment chain (Step 12b); NULL until the next compute run.
--
-- Idempotent: ADD COLUMNS is a no-op if the columns already exist
-- (runner handles skip-if-exists). The CREATE migration
-- (2026-05-28-create-spadl-action-context.sql) predates ghost_gk and is already
-- applied, so this follow-up ALTER is the additive path.

ALTER TABLE soccer_analytics.bronze.spadl_action_context
  ADD COLUMNS (
    ghost_gk_x DOUBLE,
    ghost_gk_y DOUBLE,
    ghost_gk_spread DOUBLE
  );
