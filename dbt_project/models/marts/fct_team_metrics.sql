-- fct_team_metrics.sql
-- Gold-layer wide-by-granularity team-match mart (silly-kicks 4.120.0 TF-52 + TF-53; sk4118 P1
-- Phase A). Grain: one row per (match_key, team_key). Carries BOTH team-match families:
--   * team_metrics (44 KPIs) — ingestion.team_metrics_writer / compute_team_kpis (event-only).
--   * match_outcome (5 win-prob cols) — ingestion.match_outcome_writer / compute_match_outcome
--     (event-only, per-shot xG integration).
--
-- Wide-by-granularity (AGENTS.md "metric marts = WIDE by GRANULARITY not per-feature"): the two
-- families share the (match, team) grain, so they land in ONE mart, not two per-feature marts. A
-- team-match may carry one family but not the other (match_outcome needs per-shot xG), so the two
-- staging views are FULL-OUTER-JOINed on the native grain, then the native ids are resolved to Kimball
-- surrogates from dim_matches / dim_teams (ADR-013: writers emit native ids only; surrogates resolve
-- here). data_source / access_tier / competition_key / season_id ride through; the surrogate PK
-- team_metrics_id is a deterministic hash of (data_source, native_match_id, team_id_native).
--
-- GOVERNANCE: team_metrics + match_outcome are per-TEAM aggregates, NOT per-player-evaluative systems.
-- They are DELIBERATELY EXCLUDED from PER_PLAYER_EVALUATIVE_CARDS (src/tests/test_ai_governance_md.py)
-- and carry no EU-AI-Act model card — the KPIs and win probabilities describe TEAM structure/outcome,
-- never an individual player's performance.

{{ config(
    materialized='table',
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true},
    tags=['marts', 'output_mart']
) }}

with team_kpis as (

    select * from {{ ref('stg_team_metrics') }}

),

outcome as (

    select * from {{ ref('stg_match_outcome') }}

),

-- FULL OUTER on the native grain: coalesce the shared identity/access_tier so a team-match present in
-- only one family still ships (COALESCE prefers team_kpis, falls back to outcome).
combined as (

    select
        coalesce(t.data_source, o.data_source)              as data_source,
        coalesce(t.native_match_id, o.native_match_id)      as native_match_id,
        coalesce(t.team_id_native, o.team_id_native)        as team_id_native,
        coalesce(t.access_tier, o.access_tier)              as access_tier,

        -- team_metrics (44 KPIs)
        t.ppda,
        t.defensive_intensity,
        t.time_to_defensive_action_s,
        t.time_to_recovery_s,
        t.recoveries,
        t.recoveries_within_ns_pct,
        t.counterpress_regains,
        t.counterpress_regain_pct,
        t.field_tilt_pct,
        t.pass_tempo,
        t.long_ball_pct,
        t.defensive_action_height_m,
        t.recovery_line_height_m,
        t.turnover_line_height_m,
        t.poss_to_final_third_pct,
        t.final_third_entries,
        t.final_third_to_box_pct,
        t.box_touches,
        t.box_to_shot_pct,
        t.shots,
        t.high_opportunity_shots,
        t.breakout_left,
        t.breakout_center,
        t.breakout_right,
        t.breakout_left_pct,
        t.breakout_center_pct,
        t.breakout_right_pct,
        t.possessions_retained_after_ns_pct,
        t.buildup_final_quarter,
        t.buildup_next_phase,
        t.buildup_opp_int_own_half,
        t.buildup_stayed_phase_one,
        t.buildup_opp_won_own_half,
        t.buildup_led_opp_shot,
        t.buildup_success_pct,
        t.post_regain_second_pass_pct,
        t.post_regain_failed_first_passes,
        t.post_regain_forward_first_pct,
        t.switch_press_success_pct,
        t.switch_press_n,
        t.final_third_entries_post_recovery,
        t.box_touches_post_recovery,
        t.shots_post_recovery,
        t.high_opportunity_shots_post_recovery,

        -- match_outcome (5 win-prob cols)
        o.p_win,
        o.p_draw,
        o.p_loss,
        o.xpoints,
        o.expected_goals

    from team_kpis t
    full outer join outcome o
        on  t.data_source = o.data_source
       and t.native_match_id = o.native_match_id
       and t.team_id_native = o.team_id_native

)

select
    {{ dbt_utils.generate_surrogate_key([
        'c.data_source',
        'c.native_match_id',
        'c.team_id_native'
    ]) }}                                                       as team_metrics_id,

    dm.match_key,
    dt.team_key,
    dm.competition_key,
    dm.season_id,
    c.data_source,
    c.access_tier,

    -- team_metrics (44 KPIs)
    c.ppda,
    c.defensive_intensity,
    c.time_to_defensive_action_s,
    c.time_to_recovery_s,
    c.recoveries,
    c.recoveries_within_ns_pct,
    c.counterpress_regains,
    c.counterpress_regain_pct,
    c.field_tilt_pct,
    c.pass_tempo,
    c.long_ball_pct,
    c.defensive_action_height_m,
    c.recovery_line_height_m,
    c.turnover_line_height_m,
    c.poss_to_final_third_pct,
    c.final_third_entries,
    c.final_third_to_box_pct,
    c.box_touches,
    c.box_to_shot_pct,
    c.shots,
    c.high_opportunity_shots,
    c.breakout_left,
    c.breakout_center,
    c.breakout_right,
    c.breakout_left_pct,
    c.breakout_center_pct,
    c.breakout_right_pct,
    c.possessions_retained_after_ns_pct,
    c.buildup_final_quarter,
    c.buildup_next_phase,
    c.buildup_opp_int_own_half,
    c.buildup_stayed_phase_one,
    c.buildup_opp_won_own_half,
    c.buildup_led_opp_shot,
    c.buildup_success_pct,
    c.post_regain_second_pass_pct,
    c.post_regain_failed_first_passes,
    c.post_regain_forward_first_pct,
    c.switch_press_success_pct,
    c.switch_press_n,
    c.final_third_entries_post_recovery,
    c.box_touches_post_recovery,
    c.shots_post_recovery,
    c.high_opportunity_shots_post_recovery,

    -- match_outcome (5 win-prob cols)
    c.p_win,
    c.p_draw,
    c.p_loss,
    c.xpoints,
    c.expected_goals

from combined c
inner join {{ ref('dim_matches') }} dm
    on  dm.provider = c.data_source
   and dm.native_match_id = c.native_match_id
left join {{ ref('dim_teams') }} dt
    on  dt.provider = c.data_source
   and dt.native_team_id = c.team_id_native
