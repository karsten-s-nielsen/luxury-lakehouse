-- 2026-05-20-strip-skillcorner-prefix-from-bronze.sql
-- OPERATOR-RUN: Contains DELETE (destructive) — lives in scripts/, not
-- scripts/migrations/ (auto-apply). Run manually before dbt --full-refresh.
--
-- Remove ALL legacy provider-prefixed rows from bronze compute-output tables.
--
-- Root cause: the original tracking compute pipelines (pre-May 2026) wrote
-- match_id values with provider prefixes ('idsse_J03WMX', 'skillcorner_1886347').
-- The current ingestion writes bare native IDs per ADR-018 cross-table format
-- contract. The prefixed rows cannot JOIN to dim_matches.native_match_id,
-- causing NULL match_key in downstream marts.
--
-- With this migration, the staging layer no longer needs regexp_replace to
-- strip prefixes — removed in the same PR. If a future writer re-introduces
-- a prefix, the dbt not_null test on match_key fires immediately instead of
-- being silently masked by a staging compensating control.
--
-- The compute pipelines (off_ball_xt, shape_graph, formations) will re-process
-- these matches on next run, writing bare-ID rows that resolve correctly
-- through the staging -> mart -> dim_matches JOIN chain.
--
-- Idempotent: DELETE WHERE ... LIKE '<prefix>%' is a no-op if already cleaned.

DELETE FROM soccer_analytics.bronze.off_ball_xt_results
WHERE match_id LIKE 'skillcorner_%';

DELETE FROM soccer_analytics.bronze.player_positions
WHERE match_id LIKE 'skillcorner_%'
   OR match_id LIKE 'idsse_%';

DELETE FROM soccer_analytics.bronze.formation_labels
WHERE match_id LIKE 'skillcorner_%'
   OR match_id LIKE 'idsse_%';
