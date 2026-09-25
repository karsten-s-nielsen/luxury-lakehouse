-- scripts/migrations/2026-09-17-add-team-metrics.sql
--
-- silly-kicks 4.120.0 P1 adoption (sk4118 cycle, Task A1): create the OWN bronze table for the
-- team-match KPI family (TF-52 compute_team_kpis). The gold mart fct_team_metrics + its dbt staging
-- view resolve on the LIVE warehouse BEFORE the Part-B scoring run populates the table (the phased-
-- materialization pattern, mirroring 2026-08-20-add-marts2-bravery-gkdv-bronze.sql).
--
-- ADR-013 writer table. Column list mirrors the writer DDL constant — keep in sync:
--   * team_metrics -> ingestion.team_metrics_writer.TEAM_METRICS_DDL (44 KPI cols + native ids + access_tier).
--
-- Idempotent by construction (CREATE TABLE IF NOT EXISTS — a re-run is a no-op). Operator-applied WITH
-- the merge (there is NO CI auto-apply):
--   uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/2026-09-17-add-team-metrics.sql

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.team_metrics (
    data_source STRING,
    match_id STRING,
    team_id STRING,
    access_tier STRING,
    ppda DOUBLE,
    defensive_intensity DOUBLE,
    time_to_defensive_action_s DOUBLE,
    time_to_recovery_s DOUBLE,
    recoveries BIGINT,
    recoveries_within_ns_pct DOUBLE,
    counterpress_regains BIGINT,
    counterpress_regain_pct DOUBLE,
    field_tilt_pct DOUBLE,
    pass_tempo DOUBLE,
    long_ball_pct DOUBLE,
    defensive_action_height_m DOUBLE,
    recovery_line_height_m DOUBLE,
    turnover_line_height_m DOUBLE,
    poss_to_final_third_pct DOUBLE,
    final_third_entries BIGINT,
    final_third_to_box_pct DOUBLE,
    box_touches BIGINT,
    box_to_shot_pct DOUBLE,
    shots BIGINT,
    high_opportunity_shots BIGINT,
    breakout_left BIGINT,
    breakout_center BIGINT,
    breakout_right BIGINT,
    breakout_left_pct DOUBLE,
    breakout_center_pct DOUBLE,
    breakout_right_pct DOUBLE,
    possessions_retained_after_ns_pct DOUBLE,
    buildup_final_quarter BIGINT,
    buildup_next_phase BIGINT,
    buildup_opp_int_own_half BIGINT,
    buildup_stayed_phase_one BIGINT,
    buildup_opp_won_own_half BIGINT,
    buildup_led_opp_shot BIGINT,
    buildup_success_pct DOUBLE,
    post_regain_second_pass_pct DOUBLE,
    post_regain_failed_first_passes BIGINT,
    post_regain_forward_first_pct DOUBLE,
    switch_press_success_pct DOUBLE,
    switch_press_n BIGINT,
    final_third_entries_post_recovery BIGINT,
    box_touches_post_recovery BIGINT,
    shots_post_recovery BIGINT,
    high_opportunity_shots_post_recovery BIGINT,
    _ingested_at TIMESTAMP
) USING DELTA;
