-- Per-match HF redistribution: access_tier (spec 2026-06-29, phase 4).
--
-- Adds the per-row `access_tier` STRING to every bronze fact a restricted-aware publisher reads,
-- and the raw `visibility` + derived `access_tier` to the two match-info tables that carry the
-- pining/GS per-match signal. `access_tier` rides per-row through the SPADL/AC/tracking/psxg
-- passthrough (ADR-016) to the gold marts; `split_restricted` + the leak guard split on it.
--
-- Operator-applied (NO CI auto-apply). Apply WITH the merge:
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-06-30-add-access-tier-columns.sql
-- Idempotent: the runner skips an ADD COLUMNS whose LEADING column already exists (DESCRIBE).
-- Always verify with a live DESCRIBE post-apply.

-- ── Per-action / per-frame bronze facts (single leading column → runner-idempotent) ──
ALTER TABLE soccer_analytics.bronze.spadl_actions ADD COLUMNS (access_tier STRING);
ALTER TABLE soccer_analytics.bronze.vaep_action_values ADD COLUMNS (access_tier STRING);
ALTER TABLE soccer_analytics.bronze.spadl_action_context ADD COLUMNS (access_tier STRING);

-- ── Tracking-frames bronze (feeds fct_tracking_frames + the pitch-control publisher) ──
ALTER TABLE soccer_analytics.bronze.skillcorner_tracking ADD COLUMNS (access_tier STRING);
ALTER TABLE soccer_analytics.bronze.idsse_tracking ADD COLUMNS (access_tier STRING);
ALTER TABLE soccer_analytics.bronze.metrica_tracking ADD COLUMNS (access_tier STRING);
ALTER TABLE soccer_analytics.bronze.gradientsports_tracking ADD COLUMNS (access_tier STRING);

-- ── PSxG-shots bronze (feeds fct_shot_psxg) ──
ALTER TABLE soccer_analytics.bronze.psxg_tracking_predictions ADD COLUMNS (access_tier STRING);

-- ── Match-info bronze: raw visibility + derived access_tier (leading column = visibility) ──
ALTER TABLE soccer_analytics.bronze.skillcorner_matches ADD COLUMNS (visibility STRING, access_tier STRING);
ALTER TABLE soccer_analytics.bronze.gradientsports_metadata ADD COLUMNS (visibility STRING, access_tier STRING);
