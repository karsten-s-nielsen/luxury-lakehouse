-- stg_team_metrics.sql
-- Staging view for per-(match, team) team KPIs (silly-kicks 4.120.0 TF-52; sk4118 P1 Task A1; ADR-013).
-- Source: bronze.team_metrics, written by ingestion.team_metrics_writer (compute_team_kpis, 44 KPIs).
-- Deduplicates by (data_source, match_id, team_id), latest _ingested_at wins, and casts the native
-- identifiers + access_tier to canonical types. Kimball surrogates (match_key / team_key /
-- competition_key) resolve in the mart fct_team_metrics (INNER JOIN dim_matches / dim_teams on the
-- native ids) — a hashed BIGINT team_id would land all-NULL, so the native string id is carried through.
-- access_tier (ADR-064) rides through per-row from the SPADL bronze stamp.

with source as (

    select * from {{ source('team_metrics', 'team_metrics') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by data_source, match_id, team_id
            order by _ingested_at desc
        ) as _row_num
    from source

)

select
    cast(data_source as string)                             as data_source,
    cast(match_id as string)                                as native_match_id,
    cast(team_id as string)                                 as team_id_native,
    cast(access_tier as string)                             as access_tier,
    cast(ppda as double)                                    as ppda,
    cast(defensive_intensity as double)                     as defensive_intensity,
    cast(time_to_defensive_action_s as double)              as time_to_defensive_action_s,
    cast(time_to_recovery_s as double)                      as time_to_recovery_s,
    cast(recoveries as bigint)                              as recoveries,
    cast(recoveries_within_ns_pct as double)                as recoveries_within_ns_pct,
    cast(counterpress_regains as bigint)                    as counterpress_regains,
    cast(counterpress_regain_pct as double)                 as counterpress_regain_pct,
    cast(field_tilt_pct as double)                          as field_tilt_pct,
    cast(pass_tempo as double)                              as pass_tempo,
    cast(long_ball_pct as double)                           as long_ball_pct,
    cast(defensive_action_height_m as double)               as defensive_action_height_m,
    cast(recovery_line_height_m as double)                  as recovery_line_height_m,
    cast(turnover_line_height_m as double)                  as turnover_line_height_m,
    cast(poss_to_final_third_pct as double)                 as poss_to_final_third_pct,
    cast(final_third_entries as bigint)                     as final_third_entries,
    cast(final_third_to_box_pct as double)                  as final_third_to_box_pct,
    cast(box_touches as bigint)                             as box_touches,
    cast(box_to_shot_pct as double)                         as box_to_shot_pct,
    cast(shots as bigint)                                   as shots,
    cast(high_opportunity_shots as bigint)                  as high_opportunity_shots,
    cast(breakout_left as bigint)                           as breakout_left,
    cast(breakout_center as bigint)                         as breakout_center,
    cast(breakout_right as bigint)                          as breakout_right,
    cast(breakout_left_pct as double)                       as breakout_left_pct,
    cast(breakout_center_pct as double)                     as breakout_center_pct,
    cast(breakout_right_pct as double)                      as breakout_right_pct,
    cast(possessions_retained_after_ns_pct as double)       as possessions_retained_after_ns_pct,
    cast(buildup_final_quarter as bigint)                   as buildup_final_quarter,
    cast(buildup_next_phase as bigint)                      as buildup_next_phase,
    cast(buildup_opp_int_own_half as bigint)                as buildup_opp_int_own_half,
    cast(buildup_stayed_phase_one as bigint)                as buildup_stayed_phase_one,
    cast(buildup_opp_won_own_half as bigint)                as buildup_opp_won_own_half,
    cast(buildup_led_opp_shot as bigint)                    as buildup_led_opp_shot,
    cast(buildup_success_pct as double)                     as buildup_success_pct,
    cast(post_regain_second_pass_pct as double)             as post_regain_second_pass_pct,
    cast(post_regain_failed_first_passes as bigint)         as post_regain_failed_first_passes,
    cast(post_regain_forward_first_pct as double)           as post_regain_forward_first_pct,
    cast(switch_press_success_pct as double)                as switch_press_success_pct,
    cast(switch_press_n as bigint)                          as switch_press_n,
    cast(final_third_entries_post_recovery as bigint)       as final_third_entries_post_recovery,
    cast(box_touches_post_recovery as bigint)               as box_touches_post_recovery,
    cast(shots_post_recovery as bigint)                     as shots_post_recovery,
    cast(high_opportunity_shots_post_recovery as bigint)    as high_opportunity_shots_post_recovery

from deduplicated
where _row_num = 1
