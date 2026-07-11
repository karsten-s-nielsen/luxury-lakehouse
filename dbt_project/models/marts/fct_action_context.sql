{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='action_context_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key'],
    tags=['marts', 'output_mart'],
    tblproperties={
        'delta.enableChangeDataFeed': 'true',
    }
) }}
-- fct_action_context.sql
-- Gold-layer unified action context features per SPADL action.
-- Pure Kimball from day one — no legacy BIGINT identity columns.
-- Grain: one row per (match_key, action_id).

with action_raw as (

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
        pitch_control_at_target__spearman,
        pitch_control_at_target__fernandez_bornn,
        pitch_control_at_target__voronoi,
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
        gk_closing_time_mean_s__near_post,
        gk_closing_time_min_s__near_post,
        gk_closing_time_mean_s__far_post,
        gk_closing_time_min_s__far_post,
        n_blocked_receivers,
        n_potential_receivers,
        blocking_score,
        blocked_threat_fraction,
        max_single_defender_blocking_score,
        sync_score_min,
        sync_score_mean,
        sync_score_high_quality_frac,
        obso_actual,
        obso_peak,
        obso_optimal,
        pausa_temporal,
        pausa_spatial,
        pausa_composite,
        space_created_m2,
        space_denied_m2_opponent,
        elastic_frame_id,
        elastic_confidence,
        elastic_error_seconds,
        shape_graph_density_attacking,
        shape_graph_n_edges_attacking,
        shape_graph_mean_stability_attacking,
        shape_graph_density_defending,
        shape_graph_n_edges_defending,
        shape_graph_mean_stability_defending,
        ghost_gk_x,
        ghost_gk_y,
        ghost_gk_density_spread,
        structural_lbs,
        structural_sgm,
        structural_sdi,
        actor_reachable_area_m2,
        off_ball_xt_team,
        off_ball_xt_opponent,
        off_ball_xt_diff,
        reachable_area_team,
        reachable_area_opponent,
        reachable_area_diff,
        xcross_attempt,
        xshot_occurrence,
        shot_crossing_y,
        shot_crossing_z,
        shot_speed,
        shot_time_to_goal_line,
        shot_on_target_derived,
        shot_crossing_source,
        shot_crossing_confidence,
        shot_fit_n_frames,
        shot_fit_rmse,
        shot_fit_end_reason,
        shot_z_profile,
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
        xt_gk_origin_source,
        xt_gk_dest_source,
        xt_gk_origin_confidence,
        xt_gk_completion_variant,
        xt_gk_completion_source,
        xt_gk_origin_x,
        xt_gk_origin_y,
        xt_gk_dest_x,
        xt_gk_dest_y,
        gk_completion,
        is_gk_distribution,
        pitch_control_method,
        ghost_gk_method,
        -- Per-match HF redistribution tier (spec 2026-06-29 §6.4) — per-row passthrough.
        access_tier
    from {{ ref('stg_action_context__values') }}

),

keyed as (

    select
        dm.match_key,
        dt.team_key,
        dp.player_key,
        dp_gk.player_key as defending_gk_player_key,
        ar.*
    from action_raw ar
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = ar.data_source
       and dm.native_match_id = ar.native_match_id
    left join {{ ref('dim_teams') }} dt
        on dt.provider = ar.data_source
       and dt.native_team_id = ar.team_id_native
    left join {{ ref('dim_players') }} dp
        on dp.provider = ar.data_source
       and dp.native_player_id = ar.player_id_native
    left join {{ ref('dim_players') }} dp_gk
        on dp_gk.provider = ar.data_source
       and dp_gk.native_player_id = ar.defending_gk_player_id_native

),

-- Mart-level GK-contamination guard (2026-07-01 handoff). The whole-squad xt_gk
-- contamination (non-keeper actors scored) is only visible CROSS-BATCH at the mart:
-- silly-kicks' per-call n_implausible_gk_teams guard runs per 250-frame batch (~1/team
-- there, silent), but the contamination is the union across ~164 batches. A (match, team)
-- carrying more distinct xt_gk-scored players than a squad has keepers (one, occasionally
-- a sub) is contaminated. At the mart we cannot tell which flagged player is the real
-- keeper, so the whole match is excluded fail-safe (its xt_gk value family NULLed below) —
-- better to lose a contaminated match than serve silently-wrong keeper metrics. Warn-and-
-- exclude, never crash (one bad match must not kill the rebuild). Threshold pinned from the
-- clean recompute via var (clean providers sit ≈1–2/team; contaminated ≈8–17).
xt_gk_contaminated_matches as (

    select distinct match_key as _contam_match_key
    from keyed
    where xt_gk is not null
    group by match_key, team_key
    having count(distinct player_key) > {{ var('xt_gk_max_scored_players_per_team', 4) }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['match_key', 'action_id']) }} as action_context_id,
        match_key,
        team_key,
        player_key,
        defending_gk_player_key,
        action_id,
        data_source,
        -- game_state + GK action-sequence flags removed (ADR-056): actions-level,
        -- served by fct_action_values. defending_gk_player_id_native/_key kept.
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
        pitch_control_at_target__spearman,
        pitch_control_at_target__fernandez_bornn,
        pitch_control_at_target__voronoi,
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
        gk_closing_time_mean_s__near_post,
        gk_closing_time_min_s__near_post,
        gk_closing_time_mean_s__far_post,
        gk_closing_time_min_s__far_post,
        n_blocked_receivers,
        n_potential_receivers,
        blocking_score,
        blocked_threat_fraction,
        max_single_defender_blocking_score,
        sync_score_min,
        sync_score_mean,
        sync_score_high_quality_frac,
        obso_actual,
        obso_peak,
        obso_optimal,
        pausa_temporal,
        pausa_spatial,
        pausa_composite,
        space_created_m2,
        space_denied_m2_opponent,
        elastic_frame_id,
        elastic_confidence,
        elastic_error_seconds,
        shape_graph_density_attacking,
        shape_graph_n_edges_attacking,
        shape_graph_mean_stability_attacking,
        shape_graph_density_defending,
        shape_graph_n_edges_defending,
        shape_graph_mean_stability_defending,
        ghost_gk_x,
        ghost_gk_y,
        ghost_gk_density_spread,
        structural_lbs,
        structural_sgm,
        structural_sdi,
        actor_reachable_area_m2,
        off_ball_xt_team,
        off_ball_xt_opponent,
        off_ball_xt_diff,
        reachable_area_team,
        reachable_area_opponent,
        reachable_area_diff,
        xcross_attempt,
        xshot_occurrence,
        shot_crossing_y,
        shot_crossing_z,
        shot_speed,
        shot_time_to_goal_line,
        shot_on_target_derived,
        shot_crossing_source,
        shot_crossing_confidence,
        shot_fit_n_frames,
        shot_fit_rmse,
        shot_fit_end_reason,
        shot_z_profile,
        -- xt_gk VALUE family: NULLed for guard-flagged contaminated matches (the actor is
        -- untrustworthy match-wide; the value itself is geometrically fine but must not be
        -- served as a keeper metric). Provenance/coords below are retained for audit.
        case when cm._contam_match_key is not null then null else xt_gk end as xt_gk,
        case when cm._contam_match_key is not null then null else xt_gk_possession end as xt_gk_possession,
        case when cm._contam_match_key is not null then null else xt_gk_counter end as xt_gk_counter,
        case when cm._contam_match_key is not null then null else xt_gk_direct end as xt_gk_direct,
        case when cm._contam_match_key is not null then null else xt_gk_high_press end as xt_gk_high_press,
        case when cm._contam_match_key is not null then null else xt_gk_low_block end as xt_gk_low_block,
        case when cm._contam_match_key is not null then null else xt_gk_base end as xt_gk_base,
        case when cm._contam_match_key is not null then null else xt_gk_pev end as xt_gk_pev,
        case when cm._contam_match_key is not null then null else xt_gk_rav end as xt_gk_rav,
        case when cm._contam_match_key is not null then null else xt_gk_dzv end as xt_gk_dzv,
        case when cm._contam_match_key is not null then null else xt_gk_pressure end as xt_gk_pressure,
        xt_gk_origin_source,
        xt_gk_dest_source,
        xt_gk_origin_confidence,
        xt_gk_completion_variant,
        xt_gk_completion_source,
        xt_gk_origin_x,
        xt_gk_origin_y,
        xt_gk_dest_x,
        xt_gk_dest_y,
        case when cm._contam_match_key is not null then null else gk_completion end as gk_completion,
        -- GK-distribution domain marker (silly-kicks 4.43.0). NOT gated by the xt_gk contamination
        -- guard: it is an actor-domain predicate (goal-kick OR acting-GK open-play pass), valid
        -- regardless of the whole-squad xt_gk scoring contamination. Full domain on tracking arms;
        -- goal-kicks-only on SB360 (frames=None). Consumed by silly-kicks' rho retention loader.
        is_gk_distribution,
        pitch_control_method,
        ghost_gk_method,
        -- Per-match HF redistribution tier (spec 2026-06-29 §6.4).
        access_tier,
        -- Mart-level GK-contamination guard flag (2026-07-01): true where this match's
        -- xt_gk value family was excluded (a (match, team) exceeded the scored-players bound).
        (cm._contam_match_key is not null) as xt_gk_match_contaminated

    from keyed
    left join xt_gk_contaminated_matches cm
        on cm._contam_match_key = keyed.match_key
    -- Grain unchanged: staging dedup + single Kimball join fix the (match_key, action_id) grain;
    -- the guard join is a semi-join on DISTINCT contaminated match_key (0-or-1 match), no fan-out.

)

select * from final
