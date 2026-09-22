-- scripts/migrations/2026-09-17-add-gk-decision.sql
--
-- silly-kicks 4.120.0 P1 adoption (sk4118 cycle, Task B2): create the OWN bronze table for the
-- GK decision-value family (TF-62 compute_gk_decision_value — the goalkeeper's build-up distribution
-- DECISION value, reconstruction tier). The gold mart fct_gk_decision + the dbt staging view
-- stg_gk_decision resolve on the LIVE warehouse BEFORE the tracking-marts drain (P2) populates the
-- table (the phased-materialization pattern, mirroring 2026-09-17-add-team-metrics.sql).
--
-- ADR-013 writer table. Column list mirrors the writer DDL constant — keep in sync:
--   * gk_decision -> ingestion.gk_decision_writer.GK_DECISION_DDL (5 metric cols + n_options +
--     option_set_source + native ids + access_tier). Grain: PER-DECISION (data_source, match_id,
--     period_id, decision_id, keeper) — decision_id == the SPADL action_id of the GK-distribution
--     decision; keeper is the NATIVE goalkeeper id (dim_players join). sk compute_gk_decision_value
--     returns one row per DECISION (not per keeper), so the bronze/mart are per-decision; a per-keeper
--     summary is a downstream aggregate. METRIC_CONTRACTS['gk_decision'].column_types is None, so the
--     lakehouse infers the DDL (OI-3).
--
-- SCOPE: reconstruction tier only (SB360 freeze-frames + full-tracking). The native SkillCorner GI
-- tier (option_set_source='native') is not-yet-sourced — no parse_passing_options ingestion — so no
-- native rows are written until GI lands (tracked follow-up in the model card + wf-gk-decision).
--
-- Idempotent by construction (CREATE TABLE IF NOT EXISTS — a re-run is a no-op). Operator-applied WITH
-- the merge (there is NO CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/2026-09-17-add-gk-decision.sql

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.gk_decision (
    data_source STRING,
    match_id STRING,
    period_id BIGINT,
    decision_id BIGINT,
    keeper STRING,
    team_id STRING,
    access_tier STRING,
    decision_value DOUBLE,
    chosen_ev DOUBLE,
    best_ev DOUBLE,
    sel_efficiency DOUBLE,
    decision_pct DOUBLE,
    n_options BIGINT,
    option_set_source STRING,
    _ingested_at TIMESTAMP
) USING DELTA;
