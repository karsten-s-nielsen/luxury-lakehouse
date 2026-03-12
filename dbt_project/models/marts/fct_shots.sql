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

matches as (

    select * from {{ ref('stg_statsbomb__matches') }}

),

final as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['unified_shots.event_id', 'unified_shots.data_source']) }} as shot_id,

        -- Foreign keys
        unified_shots.match_id,
        unified_shots.player_id,
        unified_shots.team_id,

        -- Match context
        matches.competition_id,
        matches.season_id,

        -- Temporal context
        unified_shots.period,
        unified_shots.minute,
        unified_shots.second,

        -- Shot location
        unified_shots.location_x,
        unified_shots.location_y,
        unified_shots.end_location_x,
        unified_shots.end_location_y,

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

        -- StatsBomb xG (benchmark / label comparison)
        unified_shots.statsbomb_xg,

        -- Data provenance
        unified_shots.data_source

    from unified_shots
    left join matches
        on unified_shots.match_id = matches.match_id

)

select * from final
