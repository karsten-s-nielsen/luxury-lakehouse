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
        ac.*,
        -- xT-GK v2 (spec §7.4) — writer-scored into bronze.xt_gk_v2_predictions and LEFT-JOINed per
        -- action (ADR-013 writer-join; mirrors fct_action_context). Replaces the retired v1 xt_gk
        -- metric + 5 collinear philosophy presets with the composite xt_gk_v2 + its 4 additive terms.
        -- NULL off the GK-distribution domain (the writer pre-filters to is_gk_distribution). No
        -- mart-level contamination guard here — that gate lives on fct_action_context; this side-by-side
        -- projection reads the writer values directly (unchanged from the v1 stg passthrough behaviour).
        xtv2.xt_gk_v2,
        xtv2.xt_gk_v2_position,
        xtv2.xt_gk_v2_pev,
        xtv2.xt_gk_v2_retention_loss,
        xtv2.xt_gk_v2_dzv,
        xtv2.gk_geometry_source
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
    left join {{ ref('stg_xt_gk_v2') }} xtv2
        on xtv2.data_source = ac.data_source
       and xtv2.native_match_id = ac.native_match_id
       and xtv2.action_id = ac.action_id
),

with_outcome as (
    select
        k.*,
        av.action_result,
        -- game_state (winning/losing/drawing, from running score) and gk_was_distributing
        -- (GVM distribution flag, ADR-056) are VAEP-stream attributes — their canonical home
        -- is fct_action_values, NOT the action-context feature stream (ac.*). Sourced here
        -- explicitly so the mart no longer depends on the AC schema incidentally carrying
        -- them (it doesn't — that orphaned the final-select references and broke --full-refresh;
        -- the daily incremental masked it). Verified: game_state + gk_was_distributing are the
        -- ONLY av-sourced columns beyond action_result; all 43 others resolve from ac.*.
        av.game_state,
        av.gk_was_distributing
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
    -- xT-GK v2 distribution VALUE family (spec §7.4 — replaces the retired v1 xt_gk metric + 5 presets):
    -- composite xt_gk_v2 + its 4 additive terms (position / pev / retention_loss / dzv), signed xG units.
    -- gk_geometry_source is resolved-geometry provenance. GK distributions only (NULL off-domain).
    xt_gk_v2,
    xt_gk_v2_position,
    xt_gk_v2_pev,
    xt_gk_v2_retention_loss,
    xt_gk_v2_dzv,
    gk_geometry_source,
    gk_completion,
    pressure_on_actor__andrienko_oval,
    -- defensive family
    ghost_gk_x,
    ghost_gk_y,
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
    -- Geometry projection (ADR-052): silly-kicks 4.26.0 unified every per-action tracking-geometry
    -- output into the action LTR frame (defended goal at x≈105). The PR #376 residual-mirror macro
    -- is retired — both pre_shot_gk_* and ghost_gk_* now share that single frame, so the canonical
    -- (defended goal at x~0) projection the app draws is a fixed reflection: (105−x, 68−y). No
    -- per-row orientation decision survives, so gk_frame_mirrored is now a contract-stable constant.
    case when pre_shot_gk_x is not null then true end as gk_frame_mirrored,
    case when pre_shot_gk_x is not null then 105.0 - pre_shot_gk_x end as gk_actual_x,
    case when pre_shot_gk_y is not null then 68.0 - pre_shot_gk_y end as gk_actual_y,
    -- frame-invariant: euclidean distance between the actual and ghost GK in their shared LTR frame
    case when pre_shot_gk_x is not null and ghost_gk_x is not null
         then sqrt(pow(pre_shot_gk_x - ghost_gk_x, 2) + pow(pre_shot_gk_y - ghost_gk_y, 2))
    end as ghost_deviation_m,
    case when defensive_line_x is not null
         then 105.0 - defensive_line_x
    end as line_height_m
from with_outcome
