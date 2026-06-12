{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='gk_action_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key'],
    tags=['marts', 'output_mart'],
    tblproperties={'delta.enableChangeDataFeed': 'true'}
) }}
-- fct_gk_tracking_actions.sql
-- Action-grain GK analytics projection of action-context, tracking providers only.
-- SIDE-BY-SIDE with the legacy GK marts (ADR-051) — nothing legacy is modified.
-- Grain: one row per (match_key, action_id).

with ac as (
    select *
    from {{ ref('stg_action_context__values') }}
    where data_source in ('gradientsports', 'idsse', 'skillcorner', 'metrica')
),

keyed as (
    select
        dm.match_key,
        dt.team_key,
        dp.player_key,
        dp_gk.player_key as defending_gk_player_key,
        ac.*
    from ac
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = ac.data_source and dm.native_match_id = ac.native_match_id
    left join {{ ref('dim_teams') }} dt
        on dt.provider = ac.data_source and dt.native_team_id = ac.team_id_native
    left join {{ ref('dim_players') }} dp
        on dp.provider = ac.data_source and dp.native_player_id = ac.player_id_native
    left join {{ ref('dim_players') }} dp_gk
        on dp_gk.provider = ac.data_source
       and dp_gk.native_player_id = ac.defending_gk_player_id_native
),

with_outcome as (
    select
        k.*,
        av.action_result
    from keyed k
    left join {{ ref('fct_action_values') }} av
        on av.match_key = k.match_key and av.action_id = k.action_id
)

select
    {{ dbt_utils.generate_surrogate_key(['match_key', 'action_id']) }} as gk_action_id,
    match_key,
    team_key,
    player_key,
    defending_gk_player_key,
    data_source,
    action_id,
    period_id,
    time_seconds,
    type_name,
    game_state,
    start_x,
    start_y,
    end_x,
    end_y,
    frame_id,
    action_result,
    -- distribution family (NULL off-domain by upstream design)
    gk_was_distributing,
    xt_gk,
    xt_gk_possession,
    xt_gk_counter,
    xt_gk_direct,
    xt_gk_high_press,
    xt_gk_low_block,
    xt_gk_base,
    xt_gk_pev,
    xt_gk_rav,
    xt_gk_dzv,
    xt_gk_pressure,
    gk_completion,
    pressure_on_actor__andrienko_oval,
    -- defensive family
    ghost_gk_x,
    ghost_gk_y,
    ghost_gk_density_spread,
    ghost_gk_method,
    gk_pitch_control_share_weighted,
    gk_reachable_area_m2,
    gk_closing_time_mean_s__six_yard_box,
    gk_closing_time_min_s__six_yard_box,
    gk_closing_time_mean_s__near_post,
    gk_closing_time_min_s__near_post,
    gk_closing_time_mean_s__far_post,
    gk_closing_time_min_s__far_post,
    defensive_line_x,
    pitch_control_method,
    -- shot family
    pre_shot_gk_x,
    pre_shot_gk_y,
    pre_shot_gk_distance_to_goal,
    pre_shot_gk_distance_to_shot,
    pre_shot_gk_angle_to_shot_trajectory,
    pre_shot_gk_angle_off_goal_line,
    -- computed (single-macro heuristic anchored on pre_shot_gk_distance_to_goal — review H3;
    -- ADR-051 section 4; architecture-audit A1: canonical actual position + mirror flag are
    -- STORED so the app never re-derives orientation)
    case when pre_shot_gk_x is not null and pre_shot_gk_distance_to_goal is not null
         then {{ gk_frame_mirror_flag('pre_shot_gk_x', 'pre_shot_gk_y', 'pre_shot_gk_distance_to_goal') }}
    end as gk_frame_mirrored,
    case when pre_shot_gk_x is not null and pre_shot_gk_distance_to_goal is not null
         then {{ gk_actual_canonical_x('pre_shot_gk_x', 'pre_shot_gk_y', 'pre_shot_gk_distance_to_goal') }}
    end as gk_actual_x,
    case when pre_shot_gk_x is not null and pre_shot_gk_distance_to_goal is not null
         then {{ gk_actual_canonical_y('pre_shot_gk_x', 'pre_shot_gk_y', 'pre_shot_gk_distance_to_goal') }}
    end as gk_actual_y,
    case when pre_shot_gk_x is not null and pre_shot_gk_distance_to_goal is not null
              and ghost_gk_x is not null
         then sqrt(
            pow({{ gk_actual_canonical_x('pre_shot_gk_x', 'pre_shot_gk_y', 'pre_shot_gk_distance_to_goal') }} - ghost_gk_x, 2)
          + pow({{ gk_actual_canonical_y('pre_shot_gk_x', 'pre_shot_gk_y', 'pre_shot_gk_distance_to_goal') }} - ghost_gk_y, 2))
    end as ghost_deviation_m,
    case when defensive_line_x is not null and pre_shot_gk_x is not null
              and pre_shot_gk_distance_to_goal is not null
         then {{ gk_line_height_m('defensive_line_x', 'pre_shot_gk_x', 'pre_shot_gk_y', 'pre_shot_gk_distance_to_goal') }}
    end as line_height_m
from with_outcome
