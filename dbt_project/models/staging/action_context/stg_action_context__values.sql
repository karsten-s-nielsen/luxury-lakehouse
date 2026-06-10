-- stg_action_context__values.sql
-- Passthrough staging for AC-1 bronze action context.
-- Deduplicates by (match_id, action_id), latest _ingested_at wins.
-- Renames identity columns for Kimball FK resolution downstream.

with source as (

    select * from {{ source('action_context', 'spadl_action_context') }}

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
        -- Metrica player_id normalization: kloppy outputs "Player6" (games 1/2)
        -- or "Player 6" (game 3), but dim_players uses synthesized format
        -- "metrica_{match}_{side}_{jersey}". Derive the dim-compatible format
        -- from team_id (already synthesized in bronze) + stripped jersey number.
        case
            when data_source = 'metrica'
            then concat(team_id, '_', regexp_replace(player_id, '^Player ?', ''))
            else cast(player_id as string)
        end                                 as player_id_native,
        cast(type_name as string)           as type_name,
        cast(start_x as double)             as start_x,
        cast(start_y as double)             as start_y,
        cast(end_x as double)              as end_x,
        cast(end_y as double)              as end_y,
        -- Game state (event-only + tracking providers)
        cast(game_state as string)          as game_state,
        -- Linkage provenance
        cast(frame_id as bigint)            as frame_id,
        cast(time_offset_seconds as double) as time_offset_seconds,
        cast(link_quality_score as double)  as link_quality_score,
        cast(n_candidate_frames as bigint)  as n_candidate_frames,
        -- GK resolution: defending GK is on the opposing team. For Metrica,
        -- synthesize dim-compatible format from match_id + opposing side + jersey.
        case
            when data_source = 'metrica' and defending_gk_player_id_native is not null
            then concat(
                'metrica_', match_id, '_',
                case when team_id like '%_home' then 'away' else 'home' end,
                '_', regexp_replace(defending_gk_player_id_native, '^Player ?', '')
            )
            else defending_gk_player_id_native
        end                                 as defending_gk_player_id_native,
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
        cast(gk_closing_time_mean_s__near_post as double) as gk_closing_time_mean_s__near_post,
        cast(gk_closing_time_min_s__near_post as double) as gk_closing_time_min_s__near_post,
        cast(gk_closing_time_mean_s__far_post as double) as gk_closing_time_mean_s__far_post,
        cast(gk_closing_time_min_s__far_post as double) as gk_closing_time_min_s__far_post,
        -- Cover shadows
        cast(n_blocked_receivers as bigint) as n_blocked_receivers,
        cast(n_potential_receivers as bigint) as n_potential_receivers,
        cast(blocking_score as double)      as blocking_score,
        cast(blocked_threat_fraction as double) as blocked_threat_fraction,
        cast(max_single_defender_blocking_score as double) as max_single_defender_blocking_score,
        -- Sync score
        cast(sync_score_min as double)      as sync_score_min,
        cast(sync_score_mean as double)     as sync_score_mean,
        cast(sync_score_high_quality_frac as double) as sync_score_high_quality_frac,
        -- OBSO
        cast(obso_actual as double)         as obso_actual,
        cast(obso_peak as double)           as obso_peak,
        cast(obso_optimal as double)        as obso_optimal,
        -- PAUSA
        cast(pausa_temporal as double)      as pausa_temporal,
        cast(pausa_spatial as double)       as pausa_spatial,
        cast(pausa_composite as double)     as pausa_composite,
        -- Space created
        cast(space_created_m2_team as double) as space_created_m2_team,
        cast(space_created_m2_opponent as double) as space_created_m2_opponent,
        -- Elastic frame linkage
        cast(elastic_frame_id as bigint)    as elastic_frame_id,
        cast(elastic_confidence as double)  as elastic_confidence,
        cast(elastic_error_seconds as double) as elastic_error_seconds,
        -- Shape graph
        cast(shape_graph_density_attacking as double) as shape_graph_density_attacking,
        cast(shape_graph_n_edges_attacking as bigint) as shape_graph_n_edges_attacking,
        cast(shape_graph_mean_stability_attacking as double) as shape_graph_mean_stability_attacking,
        cast(shape_graph_density_defending as double) as shape_graph_density_defending,
        cast(shape_graph_n_edges_defending as bigint) as shape_graph_n_edges_defending,
        cast(shape_graph_mean_stability_defending as double) as shape_graph_mean_stability_defending,
        -- Ghost GK (spread renamed to density_spread; silly-kicks 4.14.0)
        cast(ghost_gk_x as double) as ghost_gk_x,
        cast(ghost_gk_y as double) as ghost_gk_y,
        cast(ghost_gk_density_spread as double) as ghost_gk_density_spread,
        -- Structural pass (TF-45)
        cast(structural_lbs as bigint) as structural_lbs,
        cast(structural_sgm as double) as structural_sgm,
        cast(structural_sdi as double) as structural_sdi,
        -- Player influence
        cast(actor_reachable_area_m2 as double) as actor_reachable_area_m2,
        cast(off_ball_xt_team as double) as off_ball_xt_team,
        cast(off_ball_xt_opponent as double) as off_ball_xt_opponent,
        cast(off_ball_xt_diff as double) as off_ball_xt_diff,
        cast(reachable_area_team as double) as reachable_area_team,
        cast(reachable_area_opponent as double) as reachable_area_opponent,
        cast(reachable_area_diff as double) as reachable_area_diff,
        -- xCrossAttempt
        cast(xcross_attempt as double) as xcross_attempt,
        -- xShotOccurrence + pitch-control provenance (ADR-039)
        cast(xshot_occurrence as double) as xshot_occurrence,
        -- xT-GK (Eyestone; silly-kicks 4.21.0/4.22.0, ADR-048) + gk_completion
        cast(xt_gk as double) as xt_gk,
        cast(xt_gk_possession as double) as xt_gk_possession,
        cast(xt_gk_counter as double) as xt_gk_counter,
        cast(xt_gk_direct as double) as xt_gk_direct,
        cast(xt_gk_high_press as double) as xt_gk_high_press,
        cast(xt_gk_low_block as double) as xt_gk_low_block,
        cast(xt_gk_base as double) as xt_gk_base,
        cast(xt_gk_pev as double) as xt_gk_pev,
        cast(xt_gk_rav as double) as xt_gk_rav,
        cast(xt_gk_dzv as double) as xt_gk_dzv,
        cast(xt_gk_pressure as double) as xt_gk_pressure,
        cast(xt_gk_origin_source as string) as xt_gk_origin_source,
        cast(xt_gk_dest_source as string) as xt_gk_dest_source,
        cast(xt_gk_origin_confidence as double) as xt_gk_origin_confidence,
        cast(xt_gk_completion_variant as string) as xt_gk_completion_variant,
        cast(xt_gk_completion_source as string) as xt_gk_completion_source,
        cast(gk_completion as double) as gk_completion,
        cast(pitch_control_method as string) as pitch_control_method,
        -- ghost-GK backend provenance (ADR-035 amendment)
        cast(ghost_gk_method as string) as ghost_gk_method

    from deduplicated
    where _row_num = 1

)

select * from cleaned
