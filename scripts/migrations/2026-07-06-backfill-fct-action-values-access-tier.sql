-- Backfill NULL access_tier on fct_action_values from dim_matches (ADR-064 recurrence, 2026-07-06).
--
-- Root cause: bronze.spadl_actions / bronze.vaep_action_values carry the CORRECT per-match
-- access_tier for every provider (statsbomb/wyscout/idsse/metrica = 'public', gradientsports =
-- 'restricted', skillcorner = per-match), but the GOLD mart fct_action_values only carried it on
-- rows built AFTER the mart's `av.access_tier` passthrough + bronze stamping existed. The old
-- event-provider rows (statsbomb 7.06M, wyscout 2.45M, idsse, metrica, gradientsports) were built
-- earlier and were never re-derived (incremental-skip), so they sat access_tier = NULL. The #420
-- backfill was TRACKING-only, so it never touched these event/action-fact rows.
--
-- Impact: split_restricted(access_tier) fail-safes NULL -> restricted, so a full-split HF republish
-- of spadl-vaep-action-values routed the PUBLIC open data into the private companion repo and swept
-- the public dataset. (No security leak -- the public repo held no restricted data -- but the public
-- open-data dataset was withheld.) See the build-gating guard
-- dbt_project/tests/assert_action_values_access_tier_not_blocking_public.sql added same day, which
-- blocks this class from recurring on the published fact.
--
-- dim_matches carries the authoritative per-match access_tier (diagnosed non-NULL for all providers).
-- Idempotent: only NULL rows are updated; re-running is a no-op once stamped.

MERGE INTO soccer_analytics.dev_gold.fct_action_values f
USING soccer_analytics.dev_gold.dim_matches dm
ON f.match_key = dm.match_key
WHEN MATCHED AND f.access_tier IS NULL THEN UPDATE SET f.access_tier = dm.access_tier;
