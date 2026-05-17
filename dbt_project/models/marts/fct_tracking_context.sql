{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='tracking_context_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key'],
    tags=['marts', 'output_mart']
) }}
-- fct_tracking_context.sql
-- Gold-layer unified tracking features per SPADL action.
-- Pure Kimball from day one — no legacy BIGINT identity columns.
-- Grain: one row per (match_key, action_id).

with tracking_raw as (

    select
        native_match_id,
        data_source,
        action_id,
        period_id,
        time_seconds,
        team_id_native,
        player_id_native,
        type_name,
        start_x,
        start_y,
        end_x,
        end_y,
        frame_id,
        time_offset_seconds,
        link_quality_score,
        n_candidate_frames,
        defending_gk_player_id_native,
        gk_was_distributing,
        gk_was_engaged,
        gk_actions_in_possession,
        pre_shot_gk_x,
        pre_shot_gk_y,
        pre_shot_gk_distance_to_goal,
        pre_shot_gk_distance_to_shot,
        pre_shot_gk_angle_to_shot_trajectory,
        pre_shot_gk_angle_off_goal_line,
        nearest_defender_distance,
        actor_speed,
        receiver_zone_density,
        defenders_in_triangle_to_goal,
        actor_arc_length_pre_window,
        actor_displacement_pre_window,
        pressure_on_actor__andrienko_oval,
        pressure_on_actor__link_zones,
        pressure_on_actor__bekkers_pi,
        pitch_control_at_ball__spearman,
        pitch_control_at_ball__fernandez_bornn,
        pitch_control_at_ball__voronoi,
        defensive_line_x,
        back_line_high_x,
        compactness_x,
        lateral_width,
        max_lateral_gap,
        back_n_count,
        line_break,
        n_attackers_behind_line,
        n_off_ball_runners_pre_window,
        max_off_ball_run_displacement_pre_window,
        mean_off_ball_run_speed_pre_window,
        n_off_ball_runners_toward_goal_pre_window,
        line_break__ward,
        lines_broken__ward,
        line_breaking_type__ward,
        team_shape_centroid_x_attacking,
        team_shape_centroid_y_attacking,
        team_shape_convex_hull_area_attacking,
        team_shape_team_length_attacking,
        team_shape_team_width_attacking,
        team_shape_stretch_index_attacking,
        team_shape_n_outfield_players_attacking,
        team_shape_centroid_x_defending,
        team_shape_centroid_y_defending,
        team_shape_convex_hull_area_defending,
        team_shape_team_length_defending,
        team_shape_team_width_defending,
        team_shape_stretch_index_defending,
        team_shape_n_outfield_players_defending,
        das_team,
        das_opponent,
        das_diff,
        gk_pitch_control_share_weighted,
        gk_reachable_area_m2,
        gk_closing_time_mean_s__six_yard_box,
        gk_closing_time_min_s__six_yard_box,
        n_blocked_receivers,
        n_potential_receivers,
        blocking_score,
        blocked_threat_fraction,
        max_single_defender_blocking_score,
        sync_score_min,
        sync_score_mean,
        sync_score_high_quality_frac
    from {{ ref('stg_spadl__tracking_context') }}

),

keyed as (

    select
        dm.match_key,
        dt.team_key,
        dp.player_key,
        dp_gk.player_key as defending_gk_player_key,
        tr.*
    from tracking_raw tr
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = tr.data_source
       and dm.native_match_id = tr.native_match_id
    left join {{ ref('dim_teams') }} dt
        on dt.provider = tr.data_source
       and dt.native_team_id = tr.team_id_native
    left join {{ ref('dim_players') }} dp
        on dp.provider = tr.data_source
       and dp.native_player_id = tr.player_id_native
    left join {{ ref('dim_players') }} dp_gk
        on dp_gk.provider = tr.data_source
       and dp_gk.native_player_id = tr.defending_gk_player_id_native

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['match_key', 'action_id']) }} as tracking_context_id,
        match_key,
        team_key,
        player_key,
        defending_gk_player_key,
        action_id,
        data_source,
        period_id,
        time_seconds,
        type_name,
        start_x,
        start_y,
        end_x,
        end_y,
        frame_id,
        time_offset_seconds,
        link_quality_score,
        n_candidate_frames,
        defending_gk_player_id_native,
        gk_was_distributing,
        gk_was_engaged,
        gk_actions_in_possession,
        pre_shot_gk_x,
        pre_shot_gk_y,
        pre_shot_gk_distance_to_goal,
        pre_shot_gk_distance_to_shot,
        pre_shot_gk_angle_to_shot_trajectory,
        pre_shot_gk_angle_off_goal_line,
        nearest_defender_distance,
        actor_speed,
        receiver_zone_density,
        defenders_in_triangle_to_goal,
        actor_arc_length_pre_window,
        actor_displacement_pre_window,
        pressure_on_actor__andrienko_oval,
        pressure_on_actor__link_zones,
        pressure_on_actor__bekkers_pi,
        pitch_control_at_ball__spearman,
        pitch_control_at_ball__fernandez_bornn,
        pitch_control_at_ball__voronoi,
        defensive_line_x,
        back_line_high_x,
        compactness_x,
        lateral_width,
        max_lateral_gap,
        back_n_count,
        line_break,
        n_attackers_behind_line,
        n_off_ball_runners_pre_window,
        max_off_ball_run_displacement_pre_window,
        mean_off_ball_run_speed_pre_window,
        n_off_ball_runners_toward_goal_pre_window,
        line_break__ward,
        lines_broken__ward,
        line_breaking_type__ward,
        team_shape_centroid_x_attacking,
        team_shape_centroid_y_attacking,
        team_shape_convex_hull_area_attacking,
        team_shape_team_length_attacking,
        team_shape_team_width_attacking,
        team_shape_stretch_index_attacking,
        team_shape_n_outfield_players_attacking,
        team_shape_centroid_x_defending,
        team_shape_centroid_y_defending,
        team_shape_convex_hull_area_defending,
        team_shape_team_length_defending,
        team_shape_team_width_defending,
        team_shape_stretch_index_defending,
        team_shape_n_outfield_players_defending,
        das_team,
        das_opponent,
        das_diff,
        gk_pitch_control_share_weighted,
        gk_reachable_area_m2,
        gk_closing_time_mean_s__six_yard_box,
        gk_closing_time_min_s__six_yard_box,
        n_blocked_receivers,
        n_potential_receivers,
        blocking_score,
        blocked_threat_fraction,
        max_single_defender_blocking_score,
        sync_score_min,
        sync_score_mean,
        sync_score_high_quality_frac

    from keyed
    -- No QUALIFY needed: staging dedup + single Kimball join = guaranteed unique grain.

)

select * from final
