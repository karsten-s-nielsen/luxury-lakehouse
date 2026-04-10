{{ config(
    materialized='incremental',
    unique_key='shot_id',
    liquid_clustered_by=['match_id'],
    incremental_strategy='merge'
) }}
-- fct_shots.sql
-- Gold-layer shot fact table with xG features for ML model training.
--
-- This table is the primary input for the xG model. Each row represents
-- a single shot with all features needed for expected goals prediction.
--
-- xG Feature Engineering (from Soccermatics Chapter 02):
--   - distance_to_goal: Euclidean distance from shot location to goal center
--   - shot_angle: Angle subtended by the goal posts from the shot location
--   - body_part: Categorical — Right Foot, Left Foot, Head (one-hot in ML)
--   - situation: Shot type — Open Play, Set Piece, Free Kick, Penalty, Corner
--   - statsbomb_xg: StatsBomb's proprietary xG value (benchmark comparison)
--   - is_first_time: Whether the shot was taken first-time (no control)
--   - defenders_in_frame: Number of defenders between shooter and goal
--   - distance_to_nearest_defender: Spatial pressure metric
--
-- Downstream consumers:
--   - xG model training pipeline (Python/MLflow)
--   - fct_player_stats (aggregated xG per player)
--   - fct_match_summary (aggregated xG per match/team)
--   - Dashboard visualizations (shot maps, xG timelines)

with unified_shots as (

    select * from {{ ref('int_unified_shots') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}

),

sb_matches as (

    select match_id, competition_id, season_id
    from {{ ref('stg_statsbomb__matches') }}

),

ws_matches as (

    select match_id, competition_id, season_id
    from {{ ref('stg_wyscout__matches') }}

),

running_score as (

    select * from {{ ref('int_running_score') }}

),

shots_with_score as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['unified_shots.event_id', 'unified_shots.data_source']) }} as shot_id,

        -- Foreign keys
        unified_shots.match_id,
        unified_shots.player_id,
        unified_shots.team_id,

        -- Match context (StatsBomb first, Wyscout fallback)
        cast(coalesce(sb_matches.competition_id, ws_matches.competition_id) as int) as competition_id,
        cast(coalesce(sb_matches.season_id, ws_matches.season_id) as int)           as season_id,

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

        -- Binary outcome for ML target variable
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
        rs.home_team_id as _rs_home_team_id,

        row_number() over (
            partition by unified_shots.event_id, unified_shots.data_source
            order by rs.period desc, rs.minute desc, rs.second desc
        ) as _score_rn

    from unified_shots
    left join sb_matches
        on unified_shots.match_id = sb_matches.match_id
    left join ws_matches
        on unified_shots.match_id = ws_matches.match_id
    left join running_score rs
        on unified_shots.match_id = rs.match_id
        and (
            rs.period < unified_shots.period
            or (rs.period = unified_shots.period
                and (rs.minute * 60 + rs.second)
                    <= (unified_shots.minute * 60 + unified_shots.second))
        )

),

final as (

    select
        shot_id,
        match_id,
        player_id,
        team_id,
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
            when (team_id = _rs_home_team_id
                      and home_score_after > away_score_after)
                 or (team_id != _rs_home_team_id
                      and away_score_after > home_score_after)
                then 'winning'
            else 'losing'
        end as game_state,
        data_source
    from shots_with_score
    where _score_rn = 1

)

select * from final
