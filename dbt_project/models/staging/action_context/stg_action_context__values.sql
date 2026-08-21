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
            -- action_id is constant within the partition: a stability marker documenting
            -- intent, not a value-changing tiebreaker (AC bronze is 0-dup by M13 work-unit
            -- ownership). The REAL guard is assert_action_context_bronze_no_divergent_dups
            -- (bronze-source zero-dup singular test). See ADR-030 / ADR-068 / spec review-2.
            order by _ingested_at desc, action_id
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
        -- game_state removed (ADR-056): actions-level, served by fct_action_values.
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
        -- gk_was_distributing / gk_was_engaged / gk_actions_in_possession removed
        -- (ADR-056): actions-level (frame-independent), served by fct_action_values.
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
        cast(pitch_control_at_target__spearman as double) as pitch_control_at_target__spearman,
        cast(pitch_control_at_target__fernandez_bornn as double) as pitch_control_at_target__fernandez_bornn,
        cast(pitch_control_at_target__voronoi as double) as pitch_control_at_target__voronoi,
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
        -- Space creation (silly-kicks 4.24.0 lean contract: attacking LOO + rest-defense LOO)
        cast(space_created_m2 as double) as space_created_m2,
        cast(space_denied_m2_opponent as double) as space_denied_m2_opponent,
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
        -- Ghost GK (ghost_gk_density_spread retired at silly-kicks 4.87.0; ghost_gk_xfns 9->6)
        cast(ghost_gk_x as double) as ghost_gk_x,
        cast(ghost_gk_y as double) as ghost_gk_y,
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
        -- Shot goalmouth crossing (TF-48; Anzer & Bauer 2021) — tracking-derived post-shot geometry
        cast(shot_crossing_y as double) as shot_crossing_y,
        cast(shot_crossing_z as double) as shot_crossing_z,
        cast(shot_speed as double) as shot_speed,
        cast(shot_time_to_goal_line as double) as shot_time_to_goal_line,
        cast(shot_on_target_derived as boolean) as shot_on_target_derived,
        cast(shot_crossing_source as string) as shot_crossing_source,
        cast(shot_crossing_confidence as double) as shot_crossing_confidence,
        cast(shot_fit_n_frames as double) as shot_fit_n_frames,
        cast(shot_fit_rmse as double) as shot_fit_rmse,
        cast(shot_fit_end_reason as string) as shot_fit_end_reason,
        cast(shot_z_profile as string) as shot_z_profile,
        -- xT-GK v1 metric columns RETIRED (spec §7.4 — xt_gk_v2 replaces them as a mart-join fed by
        -- ingestion.xt_gk_v2_writer → stg_xt_gk_v2 → a LEFT JOIN in fct_action_context). The 4
        -- resolved-coordinate columns below are KEPT as the v2 writer's geometry bridge, and
        -- gk_completion is KEPT (a distinct add_gk_completion call).
        -- xT-GK resolved-coordinate audit (silly-kicks 4.36.0; LTR SPADL m, NaN off-scope)
        cast(xt_gk_origin_x as double) as xt_gk_origin_x,
        cast(xt_gk_origin_y as double) as xt_gk_origin_y,
        cast(xt_gk_dest_x as double) as xt_gk_dest_x,
        cast(xt_gk_dest_y as double) as xt_gk_dest_y,
        cast(gk_completion as double) as gk_completion,
        -- GK-distribution domain marker (silly-kicks 4.43.0 gk_distribution_mask): True for any
        -- goal-kick OR an open-play pass/throw-in by the acting-team GK. Full domain on tracking
        -- providers; goal-kicks-only on SB360 (frames=None). NULL on pre-F1 rows until recompute.
        cast(is_gk_distribution as boolean) as is_gk_distribution,
        cast(pitch_control_method as string) as pitch_control_method,
        -- ghost_gk_method (ADR-035 backend-provenance) RETIRED at silly-kicks 4.87.0 — the KDE backend
        -- is resolved upstream on the default predict_density path, so there is no per-row provenance to
        -- carry. The physical bronze column is retained (migration does not DROP it) but no longer read.
        -- === silly-kicks 4.87.0 DRAIN-NATIVE columns (spec §7.1) ===
        -- Real-xT OBSO provenance (4.52): "xt"/"synthetic"/"injected", NA off-domain.
        cast(obso_epv_source as string) as obso_epv_source,
        -- Off-ball run values (TF-35, 4.52)
        cast(run_value_target as double) as run_value_target,
        cast(run_value_disruptive_sum as double) as run_value_disruptive_sum,
        cast(run_value_enabled_pass as double) as run_value_enabled_pass,
        cast(n_disruptive_runs as bigint) as n_disruptive_runs,
        cast(n_valued_disruptive_runs as bigint) as n_valued_disruptive_runs,
        -- Press commitment (TF-51, 4.61)
        cast(press_commitment as double) as press_commitment,
        cast(press_commitment_closing_speed as double) as press_commitment_closing_speed,
        cast(press_commitment_source as string) as press_commitment_source,
        -- Packing (TF-49, 4.50) — receiver id is a native-id passthrough (string)
        cast(packing_made as bigint) as packing_made,
        cast(packing_goal_threat as bigint) as packing_goal_threat,
        cast(packing_net as double) as packing_net,
        cast(packing_receiver_player_id as string) as packing_receiver_player_id,
        cast(packing_secured as boolean) as packing_secured,
        -- Provenance (add_das / add_ghost_gk) + cover-shadow single-defender id free-rides
        cast(das_source as string) as das_source,
        cast(ghost_gk_source as string) as ghost_gk_source,
        cast(max_single_defender_player_id as string) as max_single_defender_player_id,
        -- team_shape gap columns free-ride (add_team_shape now carries 20)
        cast(team_shape_defensive_line_height_attacking as double) as team_shape_defensive_line_height_attacking,
        cast(team_shape_defensive_line_height_defending as double) as team_shape_defensive_line_height_defending,
        cast(team_shape_inter_line_gap_1_attacking as double) as team_shape_inter_line_gap_1_attacking,
        cast(team_shape_inter_line_gap_1_defending as double) as team_shape_inter_line_gap_1_defending,
        cast(team_shape_inter_line_gap_2_attacking as double) as team_shape_inter_line_gap_2_attacking,
        cast(team_shape_inter_line_gap_2_defending as double) as team_shape_inter_line_gap_2_defending,
        -- Visibility coverage (silly-kicks 4.87.0; spec §7.1/§7.5) — SB360-only, NULL elsewhere / until
        -- SB360 AC is enabled (ADR-058). 2 base (observed pitch fraction + provenance) + 6 *_observed_*
        -- companions (fraction DOUBLE + source STRING per count feature).
        cast(visible_area_fraction as double) as visible_area_fraction,
        cast(visible_area_source as string) as visible_area_source,
        cast(nearest_defender_distance_observed_fraction as double) as nearest_defender_distance_observed_fraction,
        cast(nearest_defender_distance_observed_source as string) as nearest_defender_distance_observed_source,
        cast(receiver_zone_density_observed_fraction as double) as receiver_zone_density_observed_fraction,
        cast(receiver_zone_density_observed_source as string) as receiver_zone_density_observed_source,
        cast(defenders_in_triangle_to_goal_observed_fraction as double) as defenders_in_triangle_to_goal_observed_fraction,
        cast(defenders_in_triangle_to_goal_observed_source as string) as defenders_in_triangle_to_goal_observed_source,
        -- Per-match HF redistribution tier (spec 2026-06-29 §6.4). Stamped per
        -- row at AC write time from the match's access_tier; rides through to
        -- fct_action_context for the publish-time split.
        cast(access_tier as string) as access_tier

    from deduplicated
    where _row_num = 1

)

select * from cleaned
