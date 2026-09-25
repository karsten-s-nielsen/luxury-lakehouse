-- ExT-v2 single-canonical-surface migration (lakehouse ADR-085): bronze `expected_threat_grids`
-- moves from the retired v1 long-format zone rows (zone_x, zone_y, xt_value, competition_id) to one
-- row per competition (+ a `global` row) carrying the fitted silly-kicks `ExpectedThreat.to_dict()`
-- JSON (values + transition_matrix + prob matrices), reconstructed downstream via `from_dict`.
--
-- DESTRUCTIVE + RUN-ONCE (operator-applied WITH the merge, before the `wf-xt-grids` producer runs in
-- P2). The old zone-row payload is schema-incompatible with the new `xt_model_json` column and cannot
-- be back-derived to a fitted model (v1 stored values only, no transition_matrix), so the table is
-- dropped and recreated empty; the producer re-fits every per-competition + global grid on the next
-- run (the ADR-063 watermark-aware rebuild). Idempotent by construction (IF EXISTS / IF NOT EXISTS).
--
-- ALSO creates `expected_threat_grid_zones` — the DERIVED 16x12 physical-oriented long-form projection
-- of the canonical model (ADR-085 amendment, sk4118 G-fix). It is NOT a second fit: `_write_grid_if_material`
-- projects the SAME fitted `ExpectedThreat` via the sk `physical_grid` seam. It exists because the dbt
-- SQL consumers `fct_action_values.gk_xt_delta` (ADR-056) and `fct_goalkeeper_stats` do per-zone xT
-- lookups that a JSON `xt_model_json` blob cannot serve. The producer writes both tables together, so
-- they never diverge. Old-form zone rows lived in `expected_threat_grids`; they are dropped here and
-- repopulated into the NEW table on the P2 re-fit (starts empty until then).
--
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-09-21-expected-threat-grids-to-dict.sql

DROP TABLE IF EXISTS soccer_analytics.bronze.expected_threat_grids;

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.expected_threat_grids (
  competition_id STRING,
  xt_model_json  STRING,
  format_version INT,
  _ingested_at   TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.expected_threat_grid_zones (
  competition_id STRING,
  zone_x         INT,
  zone_y         INT,
  xt_value       DOUBLE,
  format_version INT,
  _ingested_at   TIMESTAMP
)
USING DELTA;
