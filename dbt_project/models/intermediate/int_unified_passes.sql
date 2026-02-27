-- int_unified_passes.sql
-- Union StatsBomb and Wyscout pass data into a common schema.
--
-- Materialized as ephemeral (CTE).
--
-- The StatsBomb ingestion already flattens pass fields into top-level
-- columns (pass_type, pass_height, pass_body_part, pass_length, etc.).
-- Only pass_end_location needs JSON parsing.
--
-- Progressive pass definition:
--   A pass is "progressive" if the end point is at least 25% closer
--   to the opponent's goal center than the start point (by distance).

with statsbomb_raw as (

    select * from {{ source('statsbomb', 'statsbomb_events') }}

),

statsbomb_events as (

    select * from {{ ref('stg_statsbomb__events') }}

),

statsbomb_passes as (

    select
        e.event_id,
        e.match_id,
        e.player_id,
        e.team_id,
        e.period,
        e.minute,
        e.second,
        e.location_x                                    as start_x,
        e.location_y                                    as start_y,

        -- Parse pass end location from JSON string (use get() for safe access)
        get(from_json(raw.pass_end_location, 'ARRAY<DOUBLE>'), 0) as end_x,
        get(from_json(raw.pass_end_location, 'ARRAY<DOUBLE>'), 1) as end_y,

        -- Pass attributes (already flat columns from ingestion)
        raw.pass_type                                   as pass_type,
        raw.pass_height                                 as pass_height,
        raw.pass_body_part                              as body_part,
        raw.pass_length                                 as pass_length,
        raw.pass_angle                                  as pass_angle_radians,
        raw.pass_outcome,
        coalesce(raw.pass_cross, false)                 as is_cross,
        coalesce(raw.pass_switch, false)                as is_switch,
        coalesce(raw.pass_through_ball, false)          as is_through_ball,

        -- Progressive pass flag
        {{ distance_to_goal(
            'get(from_json(raw.pass_end_location, \'ARRAY<DOUBLE>\'), 0)',
            'get(from_json(raw.pass_end_location, \'ARRAY<DOUBLE>\'), 1)'
        ) }}
            < {{ var('progressive_pass_ratio') }} * {{ distance_to_goal('e.location_x', 'e.location_y') }}
                                                        as is_progressive,

        'statsbomb'                                     as data_source

    from statsbomb_events e
    inner join statsbomb_raw raw
        on e.event_id = raw.id
    where e.event_type = 'Pass'

),

wyscout_passes as (

    select
        event_sk                                        as event_id,
        cast(match_id as bigint)                        as match_id,
        cast(player_id as int)                          as player_id,
        cast(team_id as int)                            as team_id,
        period,
        cast(floor(event_sec / 60) as int)              as minute,
        cast(cast(event_sec as int) % 60 as int)        as second,
        start_x,
        start_y,
        end_x,
        end_y,

        -- Pass attributes from Wyscout
        sub_event_type                                  as pass_type,
        cast(null as string)                            as pass_height,
        cast(null as string)                            as body_part,
        sqrt(power(end_x - start_x, 2) + power(end_y - start_y, 2)) as pass_length,
        atan2(end_y - start_y, end_x - start_x)        as pass_angle_radians,
        case
            when is_accurate then 'Complete'
            else 'Incomplete'
        end                                             as pass_outcome,
        sub_event_type in ('Cross', 'Head cross')       as is_cross,
        sub_event_type = 'Launch'                       as is_switch,
        sub_event_type = 'Through pass'                 as is_through_ball,

        -- Progressive pass flag
        {{ distance_to_goal('end_x', 'end_y') }}
            < {{ var('progressive_pass_ratio') }} * {{ distance_to_goal('start_x', 'start_y') }}
                                                        as is_progressive,

        'wyscout'                                       as data_source

    from {{ ref('stg_wyscout__events') }}
    where event_type = 'Pass'

),

unified as (

    select * from statsbomb_passes
    union all
    select * from wyscout_passes

)

select * from unified
