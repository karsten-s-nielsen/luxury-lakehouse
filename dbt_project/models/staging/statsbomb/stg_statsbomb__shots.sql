-- stg_statsbomb__shots.sql
-- Extract shot-specific attributes from StatsBomb event data.
--
-- Key transformations needed:
--   1. Filter events to type.name = 'Shot' only
--   2. Extract shot-specific nested fields from the `shot` JSON object:
--      - shot.outcome.name        → shot_outcome (Goal, Saved, Off T, Blocked, etc.)
--      - shot.technique.name      → shot_technique (Normal, Volley, Half Volley, Lob, etc.)
--      - shot.body_part.name      → shot_body_part (Right Foot, Left Foot, Head)
--      - shot.type.name           → shot_type (Open Play, Free Kick, Corner, Penalty)
--      - shot.statsbomb_xg        → statsbomb_xg (float between 0 and 1)
--      - shot.end_location        → end_location_x, end_location_y, end_location_z
--      - shot.first_time           → is_first_time (boolean)
--      - shot.one_on_one           → is_one_on_one (boolean)
--   3. Extract freeze_frame data (array of defender/teammate positions at moment of shot)
--      - This powers downstream pitch control and xG features
--   4. Calculate basic shot geometry features via macros:
--      - distance_to_goal (Euclidean distance from shot location to goal center)
--      - shot_angle (angle subtended at the goal posts)
--
-- Freeze frame structure (shot.freeze_frame):
--   [{ "location": [x, y], "player": {"id": ..., "name": ...},
--      "position": {"id": ..., "name": ...}, "teammate": true/false }]

with events as (

    select * from {{ ref('stg_statsbomb__events') }}

),

source as (

    select * from {{ source('statsbomb', 'statsbomb_events') }}

),

shots as (

    select
        -- Keys from the cleaned events model
        events.event_id,
        events.match_id,
        events.team_id,
        events.team_name,
        events.player_id,
        events.player_name,
        events.period,
        events.minute,
        events.second,

        -- Shot location (from events model)
        events.location_x,
        events.location_y,

        -- Shot-specific fields
        -- TODO: Extract from source.shot nested JSON object
        cast(null as string)                            as shot_outcome,
        cast(null as string)                            as shot_technique,
        cast(null as string)                            as shot_body_part,
        cast(null as string)                            as shot_type,
        cast(null as double)                            as statsbomb_xg,
        cast(null as boolean)                           as is_first_time,
        cast(null as boolean)                           as is_one_on_one,

        -- End location (where the shot ended up)
        -- TODO: Extract from shot.end_location array [x, y, z]
        cast(null as double)                            as end_location_x,
        cast(null as double)                            as end_location_y,
        cast(null as double)                            as end_location_z,

        -- Computed geometry features
        -- TODO: Uncomment once location fields are populated
        -- {{ distance_to_goal('events.location_x', 'events.location_y') }} as distance_to_goal,
        -- {{ shot_angle('events.location_x', 'events.location_y') }}      as shot_angle,
        cast(null as double)                            as distance_to_goal,
        cast(null as double)                            as shot_angle,

        -- Number of defenders in freeze frame (useful xG feature)
        -- TODO: Count elements in shot.freeze_frame where teammate = false
        cast(null as int)                               as defenders_in_frame,
        cast(null as int)                               as teammates_in_frame

    from events
    inner join source
        on events.event_id = source.id
    where events.event_type = 'Shot'

)

select * from shots
