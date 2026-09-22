-- scripts/migrations/2026-09-17-add-match-outcome.sql
--
-- silly-kicks 4.120.0 P1 adoption (sk4118 cycle, Task A2): create the OWN bronze table for the
-- match-outcome family (TF-53 compute_match_outcome). The gold mart fct_team_metrics + its dbt staging
-- view resolve on the LIVE warehouse BEFORE the Part-B scoring run populates the table (the phased-
-- materialization pattern, mirroring 2026-08-20-add-marts2-bravery-gkdv-bronze.sql).
--
-- ADR-013 writer table. Column list mirrors the writer DDL constant — keep in sync:
--   * match_outcome -> ingestion.match_outcome_writer.MATCH_OUTCOME_DDL (5 win-prob cols + native ids + access_tier).
--
-- Idempotent by construction (CREATE TABLE IF NOT EXISTS — a re-run is a no-op). Operator-applied WITH
-- the merge (there is NO CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/2026-09-17-add-match-outcome.sql

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.match_outcome (
    data_source STRING,
    match_id STRING,
    team_id STRING,
    access_tier STRING,
    p_win DOUBLE,
    p_draw DOUBLE,
    p_loss DOUBLE,
    xpoints DOUBLE,
    expected_goals DOUBLE,
    _ingested_at TIMESTAMP
) USING DELTA;
