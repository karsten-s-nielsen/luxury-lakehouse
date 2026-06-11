-- scripts/migrations/2026-06-11-action-context-space-creation-lean.sql
--
-- silly-kicks 4.24.0 LEAN space-creation contract on bronze.spadl_action_context.
--
-- `add_space_creation` (4.24.0) now emits exactly two columns:
--   * space_created_m2          (>=0; actor LOO on own team's OBSO surface; attacking value)
--   * space_denied_m2_opponent  (>=0; actor LOO on the MIRRORED opponent surface; rest-defense)
-- replacing the 4.23.x pair `space_created_m2_team` + the structurally-zero
-- `space_created_m2_opponent` (retired upstream; removal-based LOO makes opponent-created
-- mathematically 0). See silly-kicks ADR-026 (amended) + lakehouse schema.py.
--
-- RUN-ONCE, OPERATOR-APPLIED (NOT CI-auto-applied), apply WITH the merge:
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-06-11-action-context-space-creation-lean.sql
--
-- RENAME / DROP COLUMN are non-idempotent (run-once); DELETE is the operator-authorised
-- clean-slate wipe (Karsten, 2026-06-11) so the next full action-context recompute writes
-- fresh new-schema data. Column-mapping (mode=name) is already enabled on this table from the
-- 4.19.2 migrations, so RENAME/DROP COLUMN resolve. ADD COLUMN appends `space_denied_m2_opponent`
-- last; the bronze writer matches by NAME (name-based column mapping) and dbt staging selects
-- columns explicitly, so positional order is cosmetic on the wiped table.
--
-- This ALTER-in-place path is deliberate over DROP+recreate: `ensure_table`
-- (ingestion/guards.py) only sets autoOptimize, so a recreate would silently lose the
-- delta.enableChangeDataFeed='true' the original 2026-05-28 create set per spec §8.1.
--
-- Verify post-apply:
--   DESCRIBE soccer_analytics.bronze.spadl_action_context;   -- space_created_m2 + space_denied_m2_opponent present; *_team / *_opponent gone
--   SELECT count(*) FROM soccer_analytics.bronze.spadl_action_context;  -- 0

ALTER TABLE soccer_analytics.bronze.spadl_action_context
  RENAME COLUMN space_created_m2_team TO space_created_m2;

ALTER TABLE soccer_analytics.bronze.spadl_action_context
  DROP COLUMN space_created_m2_opponent;

ALTER TABLE soccer_analytics.bronze.spadl_action_context
  ADD COLUMN space_denied_m2_opponent DOUBLE;

-- Clean slate: wipe all rows so the next full all-provider recompute repopulates with
-- new-schema data (operator-authorised 2026-06-11).
DELETE FROM soccer_analytics.bronze.spadl_action_context;
