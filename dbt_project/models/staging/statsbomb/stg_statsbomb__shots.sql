-- stg_statsbomb__shots.sql
-- Extract shot-specific attributes from StatsBomb event data.
--
-- All raw columns are now available from stg_statsbomb__events,
-- so this model uses only ref() — no direct source() access needed.

with events as (

    select * from {{ ref('stg_statsbomb__events') }}

),

shots as (

    select
        -- Keys
        event_id,
        match_id,
        team_id,
        team_name,
        player_id,
        player_name,
        period,
        minute,
        second,

        -- Shot location (already parsed in events model)
        location_x,
        location_y,

        -- Shot-specific fields (pass-through from events)
        shot_outcome,
        shot_technique,
        shot_body_part,
        shot_type,
        shot_statsbomb_xg                                  as statsbomb_xg,
        shot_first_time                                    as is_first_time,
        shot_one_on_one                                    as is_one_on_one,

        -- Play pattern (e.g., Regular Play, From Corner)
        play_pattern,

        -- End location (parse JSON string "[x, y, z]" — use get() for safe access)
        get(from_json(shot_end_location, 'ARRAY<DOUBLE>'), 0) as end_location_x,
        get(from_json(shot_end_location, 'ARRAY<DOUBLE>'), 1) as end_location_y,
        get(from_json(shot_end_location, 'ARRAY<DOUBLE>'), 2) as end_location_z,

        -- Computed geometry features
        {{ distance_to_goal('location_x', 'location_y') }} as distance_to_goal,
        {{ shot_angle('location_x', 'location_y') }}       as shot_angle,

        -- Number of defenders/teammates in freeze frame
        coalesce(
            size(filter(
                from_json(shot_freeze_frame, 'ARRAY<STRUCT<teammate:BOOLEAN>>'),
                f -> f.teammate = false
            )),
            0
        )                                                   as defenders_in_frame,
        coalesce(
            size(filter(
                from_json(shot_freeze_frame, 'ARRAY<STRUCT<teammate:BOOLEAN>>'),
                f -> f.teammate = true
            )),
            0
        )                                                   as teammates_in_frame

    from events
    where event_type = 'Shot'

)

select * from shots
