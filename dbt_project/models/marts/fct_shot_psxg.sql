{{ config(
    materialized='table',
    enabled=var('goalkeeper_enabled', false),
    liquid_clustered_by=['match_key'],
    tags=['marts', 'output_mart']
) }}
-- fct_shot_psxg.sql
-- Atomic shot-grain PSxG fact — one row per on-target shot, ALL providers.
-- The single source of truth for post-shot expected goals (replaces the inlined
-- psxg_agg CTE in fct_goalkeeper_stats; see spec 2026-06-20-psxg-tracking-extension D-A).
-- Grain: (match_key, action_id). Provider/modality is a column (psxg_input_source),
-- not a code fork. Gate-failed shots are KEPT with psxg NULL + psxg_gated=true (D-F)
-- so coverage is computable downstream. dim_players is a role-playing dimension
-- (shooter player_key + defending_gk_player_key).
--
-- PHASE 1: tracking modality (GradientSports / SkillCorner / IDSSE). Metrica is
-- excluded upstream (no ball-z). PHASE 2 unions the StatsBomb event-shot source.

with tracking_pred as (

    select * from {{ ref('stg_psxg_tracking__predictions') }}

),

ac_shots as (

    select
        match_key,
        action_id,
        data_source,
        player_key,
        team_key,
        defending_gk_player_key,
        shot_crossing_y,
        shot_crossing_z,
        shot_speed,
        shot_z_profile,
        shot_crossing_confidence,
        shot_fit_rmse
    from {{ ref('fct_action_context') }}
    where type_name in ('shot', 'shot_freekick', 'shot_penalty')
      and shot_on_target_derived = true
      and shot_crossing_z is not null

),

shot_goal as (

    select
        match_key,
        action_id,
        case when action_result = 'success' then true else false end as is_goal
    from {{ ref('fct_action_values') }}
    where action_type in ('shot', 'shot_freekick', 'shot_penalty')

),

match_attrs as (

    -- access_tier is a per-match attribute on dim_matches (spec 2026-06-29 §6.4);
    -- resolved here for BOTH the tracking and StatsBomb shot branches.
    select match_key, competition_key, season_id, access_tier from {{ ref('dim_matches') }}

),

tracking as (

    select
        {{ dbt_utils.generate_surrogate_key(['ac.match_key', 'ac.action_id']) }} as shot_psxg_id,
        cast(ac.match_key as bigint)                                  as match_key,
        cast(ac.action_id as bigint)                                  as action_id,
        cast(ac.data_source as string)                                as data_source,
        cast('tracking_trajectory' as string)                         as psxg_input_source,
        cast(ac.player_key as bigint)                                 as player_key,
        cast(ac.team_key as bigint)                                   as team_key,
        cast(ac.defending_gk_player_key as bigint)                    as defending_gk_player_key,
        cast(ma.competition_key as bigint)                            as competition_key,
        cast(ma.season_id as int)                                     as season_id,
        cast(ac.shot_crossing_y as double)                            as shot_crossing_y,
        cast(ac.shot_crossing_z as double)                            as shot_crossing_z,
        cast((ac.shot_crossing_y - 30.34) / 7.32 as double)           as y_norm,
        cast(ac.shot_crossing_z / 7.32 as double)                     as z_norm,
        cast(ac.shot_speed as double)                                 as shot_speed,
        cast(ac.shot_z_profile as string)                             as shot_z_profile,
        cast(ac.shot_crossing_confidence as double)                   as shot_crossing_confidence,
        cast(ac.shot_fit_rmse as double)                              as shot_fit_rmse,
        cast(p.psxg as double)                                        as psxg,
        cast(p.psxg_recalibrated as double)                           as psxg_recalibrated,
        cast(p.psxg_gated as boolean)                                 as psxg_gated,
        cast(p.psxg_calibration as string)                            as psxg_calibration,
        cast(coalesce(sg.is_goal, false) as boolean)                  as is_goal,
        cast(p.model_version as string)                               as model_version,
        cast(p.platt_version as string)                               as platt_version,
        cast(p.normalization_version as string)                       as normalization_version,
        -- Per-match HF redistribution tier (spec 2026-06-29 §6.4).
        cast(ma.access_tier as string)                                as access_tier
    from tracking_pred p
    inner join ac_shots ac
        on p.match_key = ac.match_key and p.action_id = ac.action_id
    left join shot_goal sg
        on p.match_key = sg.match_key and p.action_id = sg.action_id
    left join match_attrs ma
        on p.match_key = ma.match_key

),

-- PHASE 2: StatsBomb source. psxg comes from the (retrained, on-target) StatsBomb model
-- via stg_psxg__predictions. The 2-hop bridge resolves the SPADL (match_key, action_id):
--   stg_psxg.event_id (= fct_shots.shot_id) -> fct_shots.event_id (native)
--   -> fct_action_values.original_event_id -> action_id.
-- Defending GK is the per-shot defending_gk_player_id from the action context (matches the
-- legacy lineup attribution for single-GK matches; sub-match differences are documented by
-- the A2 attribution-parity test before the value-parity guard). No Platt (psxg_calibration='none').

sb_pred as (

    select event_id, psxg from {{ ref('stg_psxg__predictions') }}

),

sb_shots as (

    select shot_id, event_id, match_key, player_key, team_key, end_location_y, end_location_z, is_goal
    from {{ ref('fct_shots') }}
    where data_source = 'statsbomb'
      and shot_outcome in ('Goal', 'Saved', 'Post', 'Saved to Post')
      and end_location_z is not null

),

sb_action as (

    select original_event_id, action_id
    from {{ ref('fct_action_values') }}
    where data_source = 'statsbomb'
      and action_type in ('shot', 'shot_freekick', 'shot_penalty')

),

-- Defending GK per (StatsBomb match, team) via the LINEUP — the GK who played for
-- each team (the opposing team's GK is who faces a shot). Mirrors the legacy
-- psxg_agg `gk_matches` attribution: the per-shot `fct_action_values.defending_gk_player_id`
-- is ~97% NULL ("GK not identifiable"), which collapsed per-GK psxg_faced. For multi-GK
-- (sub) matches the GK with the most actions is chosen (the primary keeper); sub-window
-- mis-attribution is a documented limitation (A2). Tracking providers are unaffected —
-- they carry defending_gk_player_key directly from fct_action_context.
sb_gk_per_match as (

    select match_key, team_key, gk_player_key
    from (
        select
            av.match_key,
            av.team_key,
            dp.player_key as gk_player_key,
            row_number() over (
                partition by av.match_key, av.team_key
                order by count(*) desc, dp.player_key
            ) as rn
        from {{ ref('fct_action_values') }} av
        inner join {{ ref('dim_players') }} dp
            on dp.player_id = av.player_id
           and dp.position_group = 'Goalkeeper'
        where av.data_source = 'statsbomb'
          and av.team_key is not null
        group by av.match_key, av.team_key, dp.player_key
    )
    where rn = 1

),

statsbomb as (

    select
        {{ dbt_utils.generate_surrogate_key(['s.match_key', 'a.action_id']) }} as shot_psxg_id,
        cast(s.match_key as bigint)                                   as match_key,
        cast(a.action_id as bigint)                                   as action_id,
        cast('statsbomb' as string)                                   as data_source,
        cast('statsbomb_freeze_frame' as string)                      as psxg_input_source,
        cast(s.player_key as bigint)                                  as player_key,
        cast(s.team_key as bigint)                                    as team_key,
        cast(gk.gk_player_key as bigint)                              as defending_gk_player_key,
        cast(ma.competition_key as bigint)                            as competition_key,
        cast(ma.season_id as int)                                     as season_id,
        cast(null as double)                                          as shot_crossing_y,
        cast(null as double)                                          as shot_crossing_z,
        cast((s.end_location_y - 36.0) / 8.0 as double)               as y_norm,
        cast(s.end_location_z / 8.0 as double)                        as z_norm,
        cast(null as double)                                          as shot_speed,
        cast(null as string)                                          as shot_z_profile,
        cast(null as double)                                          as shot_crossing_confidence,
        cast(null as double)                                          as shot_fit_rmse,
        cast(p.psxg as double)                                        as psxg,
        cast(p.psxg as double)                                        as psxg_recalibrated,
        cast(false as boolean)                                        as psxg_gated,
        cast('none' as string)                                        as psxg_calibration,
        cast(s.is_goal = 1 as boolean)                                as is_goal,
        cast(null as string)                                          as model_version,
        cast(null as string)                                          as platt_version,
        cast(null as string)                                          as normalization_version,
        -- Per-match HF redistribution tier (spec 2026-06-29 §6.4). StatsBomb is
        -- always public, but resolve from dim_matches for uniform provenance.
        cast(ma.access_tier as string)                                as access_tier
    from sb_pred p
    inner join sb_shots s
        on p.event_id = s.shot_id
    inner join sb_action a
        on a.original_event_id = s.event_id
    left join sb_gk_per_match gk
        on gk.match_key = s.match_key
       and gk.team_key != s.team_key
    left join match_attrs ma
        on s.match_key = ma.match_key

)

select * from tracking
union all
select * from statsbomb
