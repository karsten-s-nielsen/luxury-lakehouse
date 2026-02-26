-- int_unified_passes.sql
-- Union StatsBomb and Wyscout pass data into a common schema.
--
-- This intermediate model creates a single unified passes table for
-- cross-source pass analysis, progressive pass identification, and
-- pass network construction.
--
-- Materialized as ephemeral (CTE).
--
-- Progressive pass definition (from Wyscout/StatsBomb analytics):
--   A pass is "progressive" if the end point is at least 25% closer
--   to the opponent's goal center than the start point (by distance).
--   This is a widely used metric in modern football analytics.

with statsbomb_passes as (

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
        -- TODO: Extract pass.end_location from raw event JSON
        -- Need to join back to source for pass-specific nested fields
        cast(null as double)                            as end_x,
        cast(null as double)                            as end_y,

        -- Pass attributes
        -- TODO: Extract from pass nested JSON:
        --   pass.type.name       → pass_type (Ground Pass, High Pass, etc.)
        --   pass.height.name     → pass_height (Ground Pass, Low Pass, High Pass)
        --   pass.body_part.name  → body_part
        --   pass.length           → pass_length
        --   pass.angle            → pass_angle
        --   pass.outcome.name    → pass_outcome (null = complete, Incomplete, Out, etc.)
        --   pass.recipient.name  → recipient_name
        --   pass.cross            → is_cross
        --   pass.switch            → is_switch
        --   pass.through_ball     → is_through_ball
        cast(null as string)                            as pass_type,
        cast(null as string)                            as pass_height,
        cast(null as string)                            as body_part,
        cast(null as double)                            as pass_length,
        cast(null as double)                            as pass_angle_radians,
        cast(null as string)                            as pass_outcome,
        cast(null as boolean)                           as is_cross,
        cast(null as boolean)                           as is_switch,
        cast(null as boolean)                           as is_through_ball,

        -- Progressive pass flag
        -- TODO: Calculate once start/end locations are available
        -- A pass is progressive if end_distance_to_goal < 0.75 * start_distance_to_goal
        cast(null as boolean)                           as is_progressive,

        'statsbomb'                                     as data_source

    from {{ ref('stg_statsbomb__events') }} e
    where e.event_type = 'Pass'

),

wyscout_passes as (

    select
        event_id,
        match_id,
        player_id,
        team_id,
        period,
        cast(floor(event_sec / 60) as int)              as minute,
        cast(mod(cast(event_sec as int), 60) as int)    as second,
        start_x,
        start_y,
        end_x,
        end_y,

        -- Pass attributes from Wyscout
        sub_event_type                                  as pass_type,
        cast(null as string)                            as pass_height,
        cast(null as string)                            as body_part,
        -- TODO: Calculate pass length from coordinates
        cast(null as double)                            as pass_length,
        cast(null as double)                            as pass_angle_radians,
        case
            when is_accurate then 'Complete'
            else 'Incomplete'
        end                                             as pass_outcome,
        -- TODO: Derive from sub_event_type
        -- sub_event_type IN ('Cross', 'Head cross') → is_cross
        cast(null as boolean)                           as is_cross,
        cast(null as boolean)                           as is_switch,
        cast(null as boolean)                           as is_through_ball,

        -- Progressive pass flag
        -- TODO: Calculate once coordinates are populated
        cast(null as boolean)                           as is_progressive,

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
