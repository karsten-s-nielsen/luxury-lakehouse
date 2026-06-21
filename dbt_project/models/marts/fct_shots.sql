{{ config(
    materialized='incremental',
    unique_key='shot_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['marts', 'output_mart']
) }}
-- fct_shots.sql
-- Gold-layer shot fact table with xG features for ML model training.
-- Keyed on match_key (Kimball surrogate per ADR-011, PR 3). Each row
-- represents a single shot with all features needed for expected-goals
-- prediction: distance/angle geometry, body part, shot type, StatsBomb xG
-- benchmark, is_goal target, and game-state context. Legacy competition_id
-- INT retained nullable for bronze back-compat and non-Taipy consumers;
-- scheduled for removal in PR 8 sweep.
--
-- Source set: StatsBomb + Wyscout (2 providers). Downstream consumers:
-- fct_xg_predictions (v1), fct_xg_predictions_v2, fct_player_stats,
-- fct_match_summary, Shot-Map dashboard, xG training pipelines.

with unified_shots as (

    select * from {{ ref('int_unified_shots') }}

),

match_attrs as (

    select
        match_key,
        competition_key,
        competition_id,
        season_id
    from {{ ref('dim_matches') }}

),

running_score as (

    select * from {{ ref('int_running_score') }}

),

shots_with_score as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['unified_shots.event_id', 'unified_shots.data_source']) }} as shot_id,

        -- Native provider event id (PSxG tracking-extension Phase 2 bridge: resolves
        -- the (match_key, action_id) shot via fct_action_values.original_event_id).
        cast(unified_shots.event_id as string) as event_id,

        -- Kimball keys (from dim_matches join)
        unified_shots.match_key,
        match_attrs.competition_key,

        -- Legacy INT keys (nullable; retained for back-compat)
        cast(match_attrs.competition_id as int) as competition_id,
        cast(match_attrs.season_id as int)      as season_id,

        -- Entity FKs
        unified_shots.player_id,
        unified_shots.team_id,

        -- PR 7 (ADR-011): Kimball surrogate FKs resolved via dim_teams /
        -- dim_players JOINs on (provider, native_id). SB+WS native IDs are
        -- real BIGINTs cast to string for the JOIN. PR 8 drops the legacy
        -- INT cols.
        dt.team_key,
        dp.player_key,

        -- Temporal context
        unified_shots.period,
        unified_shots.minute,
        unified_shots.second,

        -- Shot location
        unified_shots.location_x,
        unified_shots.location_y,
        unified_shots.end_location_x,
        unified_shots.end_location_y,
        unified_shots.end_location_z,

        -- Shot classification
        unified_shots.shot_outcome,
        unified_shots.shot_body_part,
        unified_shots.shot_technique,
        unified_shots.shot_type,

        -- Binary outcome for ML target
        case
            when unified_shots.shot_outcome = 'Goal' then 1
            else 0
        end                                             as is_goal,

        -- xG features (geometry)
        unified_shots.distance_to_goal,
        unified_shots.shot_angle,

        -- xG features (situational)
        unified_shots.is_first_time,

        -- Play pattern context
        unified_shots.play_pattern,

        -- StatsBomb xG (benchmark / label comparison)
        unified_shots.statsbomb_xg,

        -- Data provenance
        unified_shots.data_source,

        -- Running score columns for game state derivation
        rs.home_score_after,
        rs.away_score_after,
        rs.home_team_id_native as _rs_home_team_id_native,

        row_number() over (
            partition by unified_shots.event_id, unified_shots.data_source
            order by rs.period desc, rs.minute desc, rs.second desc
        ) as _score_rn

    from unified_shots
    left join match_attrs
        on unified_shots.match_key = match_attrs.match_key
    left join running_score rs
        on unified_shots.match_key = rs.match_key
        and (
            rs.period < unified_shots.period
            or (rs.period = unified_shots.period
                and (rs.minute * 60 + rs.second)
                    <= (unified_shots.minute * 60 + unified_shots.second))
        )
    -- PR 7 (ADR-011): dim_teams / dim_players JOINs by (provider, native_id).
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = unified_shots.data_source
       and dt.native_team_id = cast(unified_shots.team_id as string)
    left join {{ ref('dim_players') }} dp
        on  dp.provider = unified_shots.data_source
       and dp.native_player_id = cast(unified_shots.player_id as string)

),

final as (

    select
        shot_id,
        event_id,
        match_key,
        player_id,
        team_id,
        team_key,
        player_key,
        competition_key,
        competition_id,
        season_id,
        period,
        minute,
        second,
        location_x,
        location_y,
        end_location_x,
        end_location_y,
        end_location_z,
        shot_outcome,
        shot_body_part,
        shot_technique,
        shot_type,
        is_goal,
        distance_to_goal,
        shot_angle,
        is_first_time,
        play_pattern,
        statsbomb_xg,
        case
            when coalesce(home_score_after, 0) = coalesce(away_score_after, 0)
                then 'drawing'
            when (cast(team_id as string) = _rs_home_team_id_native
                      and home_score_after > away_score_after)
                 or (cast(team_id as string) != _rs_home_team_id_native
                      and away_score_after > home_score_after)
                then 'winning'
            else 'losing'
        end as game_state,
        data_source
    from shots_with_score
    where _score_rn = 1

)

select * from final
