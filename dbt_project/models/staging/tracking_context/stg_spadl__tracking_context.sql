-- stg_spadl__tracking_context.sql
-- Passthrough staging for TC-1 bronze tracking context.
-- Deduplicates by (match_id, action_id), latest _ingested_at wins.
-- Renames identity columns for Kimball FK resolution downstream.

with source as (

    select * from {{ source('tracking_context', 'spadl_tracking_context') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by match_id, action_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(data_source as string)         as data_source,
        cast(match_id as string)            as native_match_id,
        cast(action_id as bigint)           as action_id,
        cast(period_id as bigint)           as period_id,
        cast(time_seconds as double)        as time_seconds,
        cast(team_id as string)             as team_id_native,
        cast(player_id as string)           as player_id_native,
        cast(type_name as string)           as type_name,
        cast(start_x as double)             as start_x,
        cast(start_y as double)             as start_y,
        cast(end_x as double)              as end_x,
        cast(end_y as double)              as end_y,
        -- Linkage provenance
        cast(frame_id as bigint)            as frame_id,
        cast(time_offset_seconds as double) as time_offset_seconds,
        cast(link_quality_score as double)  as link_quality_score,
        cast(n_candidate_frames as bigint)  as n_candidate_frames,
        -- GK resolution (DOUBLE in bronze → BIGINT for dim_players join)
        cast(defending_gk_player_id as bigint) as defending_gk_player_id,
        cast(gk_was_distributing as boolean) as gk_was_distributing,
        cast(gk_was_engaged as boolean)     as gk_was_engaged,
        cast(gk_actions_in_possession as bigint) as gk_actions_in_possession,
        -- GK spatial
        cast(pre_shot_gk_x as double)      as pre_shot_gk_x,
        cast(pre_shot_gk_y as double)      as pre_shot_gk_y,
        cast(pre_shot_gk_distance_to_goal as double) as pre_shot_gk_distance_to_goal,
        cast(pre_shot_gk_distance_to_shot as double) as pre_shot_gk_distance_to_shot,
        cast(pre_shot_gk_angle_to_shot_trajectory as double) as pre_shot_gk_angle_to_shot_trajectory,
        cast(pre_shot_gk_angle_off_goal_line as double) as pre_shot_gk_angle_off_goal_line,
        -- Action context
        cast(nearest_defender_distance as double) as nearest_defender_distance,
        cast(actor_speed as double)         as actor_speed,
        cast(receiver_zone_density as bigint) as receiver_zone_density,
        cast(defenders_in_triangle_to_goal as bigint) as defenders_in_triangle_to_goal,
        -- Actor pre-window
        cast(actor_arc_length_pre_window as double) as actor_arc_length_pre_window,
        cast(actor_displacement_pre_window as double) as actor_displacement_pre_window,
        -- Pressure
        cast(pressure_on_actor__andrienko_oval as double) as pressure_on_actor__andrienko_oval,
        cast(pressure_on_actor__link_zones as double) as pressure_on_actor__link_zones,
        cast(pressure_on_actor__bekkers_pi as double) as pressure_on_actor__bekkers_pi,
        -- Pitch control
        cast(pitch_control_at_ball__spearman as double) as pitch_control_at_ball__spearman,
        cast(pitch_control_at_ball__fernandez_bornn as double) as pitch_control_at_ball__fernandez_bornn,
        cast(pitch_control_at_ball__voronoi as double) as pitch_control_at_ball__voronoi,
        -- Defensive line
        cast(defensive_line_x as double)    as defensive_line_x,
        cast(back_line_high_x as double)    as back_line_high_x,
        cast(compactness_x as double)       as compactness_x,
        cast(lateral_width as double)       as lateral_width,
        cast(max_lateral_gap as double)     as max_lateral_gap,
        cast(back_n_count as bigint)        as back_n_count,
        -- Off-ball context
        cast(line_break as boolean)         as line_break,
        cast(n_attackers_behind_line as bigint) as n_attackers_behind_line,
        cast(n_off_ball_runners_pre_window as bigint) as n_off_ball_runners_pre_window,
        cast(max_off_ball_run_displacement_pre_window as double) as max_off_ball_run_displacement_pre_window,
        cast(mean_off_ball_run_speed_pre_window as double) as mean_off_ball_run_speed_pre_window,
        cast(n_off_ball_runners_toward_goal_pre_window as bigint) as n_off_ball_runners_toward_goal_pre_window,
        -- Ward line-breaking
        cast(line_break__ward as boolean)   as line_break__ward,
        cast(lines_broken__ward as bigint)  as lines_broken__ward,
        cast(line_breaking_type__ward as string) as line_breaking_type__ward,
        -- Team shape
        cast(team_shape_centroid_x_attacking as double) as team_shape_centroid_x_attacking,
        cast(team_shape_centroid_y_attacking as double) as team_shape_centroid_y_attacking,
        cast(team_shape_convex_hull_area_attacking as double) as team_shape_convex_hull_area_attacking,
        cast(team_shape_team_length_attacking as double) as team_shape_team_length_attacking,
        cast(team_shape_team_width_attacking as double) as team_shape_team_width_attacking,
        cast(team_shape_stretch_index_attacking as double) as team_shape_stretch_index_attacking,
        cast(team_shape_n_outfield_players_attacking as bigint) as team_shape_n_outfield_players_attacking,
        cast(team_shape_centroid_x_defending as double) as team_shape_centroid_x_defending,
        cast(team_shape_centroid_y_defending as double) as team_shape_centroid_y_defending,
        cast(team_shape_convex_hull_area_defending as double) as team_shape_convex_hull_area_defending,
        cast(team_shape_team_length_defending as double) as team_shape_team_length_defending,
        cast(team_shape_team_width_defending as double) as team_shape_team_width_defending,
        cast(team_shape_stretch_index_defending as double) as team_shape_stretch_index_defending,
        cast(team_shape_n_outfield_players_defending as bigint) as team_shape_n_outfield_players_defending,
        -- DAS
        cast(das_team as double)            as das_team,
        cast(das_opponent as double)        as das_opponent,
        cast(das_diff as double)            as das_diff,
        -- GK influence
        cast(gk_pitch_control_share_weighted as double) as gk_pitch_control_share_weighted,
        cast(gk_reachable_area_m2 as double) as gk_reachable_area_m2,
        cast(gk_closing_time_mean_s__six_yard_box as double) as gk_closing_time_mean_s__six_yard_box,
        cast(gk_closing_time_min_s__six_yard_box as double) as gk_closing_time_min_s__six_yard_box,
        -- Cover shadows
        cast(n_blocked_receivers as bigint) as n_blocked_receivers,
        cast(n_potential_receivers as bigint) as n_potential_receivers,
        cast(blocking_score as double)      as blocking_score,
        cast(blocked_threat_fraction as double) as blocked_threat_fraction,
        cast(max_single_defender_blocking_score as double) as max_single_defender_blocking_score,
        -- Sync score
        cast(sync_score_min as double)      as sync_score_min,
        cast(sync_score_mean as double)     as sync_score_mean,
        cast(sync_score_high_quality_frac as double) as sync_score_high_quality_frac

    from deduplicated
    where _row_num = 1

)

select * from cleaned
