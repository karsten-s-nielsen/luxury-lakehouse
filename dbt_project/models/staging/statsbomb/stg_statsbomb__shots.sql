-- stg_statsbomb__shots.sql
-- Extract shot-specific attributes from StatsBomb event data.
--
-- The ingestion layer already flattens most shot fields into top-level
-- columns (shot_outcome, shot_body_part, shot_statsbomb_xg, etc.).
-- This model filters to shots, parses the shot_end_location JSON array,
-- and calculates geometry features.

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

        -- Shot-specific fields (already flat columns on source)
        source.shot_outcome,
        source.shot_technique,
        source.shot_body_part,
        source.shot_type,
        source.shot_statsbomb_xg                        as statsbomb_xg,
        source.shot_first_time                          as is_first_time,
        source.shot_one_on_one                          as is_one_on_one,

        -- End location (parse JSON string "[x, y, z]" — use get() for safe access)
        get(from_json(source.shot_end_location, 'ARRAY<DOUBLE>'), 0) as end_location_x,
        get(from_json(source.shot_end_location, 'ARRAY<DOUBLE>'), 1) as end_location_y,
        get(from_json(source.shot_end_location, 'ARRAY<DOUBLE>'), 2) as end_location_z,

        -- Computed geometry features
        {{ distance_to_goal('events.location_x', 'events.location_y') }} as distance_to_goal,
        {{ shot_angle('events.location_x', 'events.location_y') }}       as shot_angle,

        -- Number of defenders/teammates in freeze frame
        coalesce(
            size(filter(
                from_json(source.shot_freeze_frame, 'ARRAY<STRUCT<teammate:BOOLEAN>>'),
                f -> f.teammate = false
            )),
            0
        )                                               as defenders_in_frame,
        coalesce(
            size(filter(
                from_json(source.shot_freeze_frame, 'ARRAY<STRUCT<teammate:BOOLEAN>>'),
                f -> f.teammate = true
            )),
            0
        )                                               as teammates_in_frame

    from events
    inner join source
        on events.event_id = source.id
    where events.event_type = 'Shot'

)

select * from shots
