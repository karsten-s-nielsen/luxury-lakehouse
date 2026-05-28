-- scripts/migrations/2026-05-28-liquid-cluster-spadl-actions.sql
--
-- Enable liquid clustering on bronze.spadl_actions for (data_source, match_id_native).
--
-- The preflight guard for AC-1 (and spadl_vaep) queries this table with
-- filter predicates on data_source. Without clustering, every preflight
-- triggers a full scan (~5.5K matches × 1.2M rows). With liquid clustering,
-- Spark can skip irrelevant files via data-skipping stats.
--
-- Idempotent: ALTER TABLE CLUSTER BY is a metadata-only operation that
-- can be re-run safely. Existing data is reorganized lazily by
-- Predictive Optimization (enabled at catalog level).
--
-- Catalog hardcoded as `soccer_analytics` per existing migration convention.

ALTER TABLE soccer_analytics.bronze.spadl_actions
CLUSTER BY (data_source, match_id_native);
