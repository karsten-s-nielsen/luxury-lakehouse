-- int_unified_shots.sql
-- Union StatsBomb and Wyscout shot data into a common schema.
--
-- This intermediate model creates a single unified shots table that
-- combines shots from multiple data providers, enabling cross-source
-- analysis and model training.
--
-- Materialized as ephemeral (CTE) -- no physical table created.
-- Downstream mart models (fct_shots) reference this.

with statsbomb_shots as (

    select
        event_id,
        match_id,
        player_id,
        team_id,
        period,
        minute,
        second,
        location_x,
        location_y,
        end_location_x,
        end_location_y,
        shot_outcome,
        shot_body_part,
        shot_technique,
        shot_type,
        statsbomb_xg,
        is_first_time,
        distance_to_goal,
        shot_angle,
        'statsbomb'                                     as data_source

    from {{ ref('stg_statsbomb__shots') }}

),

wyscout_shots as (

    select
        event_sk                                        as event_id,
        cast(match_id as bigint)                        as match_id,
        cast(player_id as int)                          as player_id,
        cast(team_id as int)                            as team_id,
        period,
        cast(floor(event_sec / 60) as int)              as minute,
        cast(cast(event_sec as int) % 60 as int)        as second,
        start_x                                         as location_x,
        start_y                                         as location_y,
        end_x                                           as end_location_x,
        end_y                                           as end_location_y,
        -- Map Wyscout outcome tags to standardized outcome values
        case
            when is_goal then 'Goal'
            else 'No Goal'
        end                                             as shot_outcome,
        -- Extract body part from sub_event_type
        case
            when sub_event_type like '%Head%' then 'Head'
            when sub_event_type like '%Right%' then 'Right Foot'
            when sub_event_type like '%Left%' then 'Left Foot'
            else 'Unknown'
        end                                             as shot_body_part,
        cast(null as string)                            as shot_technique,
        sub_event_type                                  as shot_type,
        cast(null as double)                            as statsbomb_xg,
        cast(null as boolean)                           as is_first_time,
        -- Compute geometry via macros
        {{ distance_to_goal('start_x', 'start_y') }}   as distance_to_goal,
        {{ shot_angle('start_x', 'start_y') }}         as shot_angle,
        'wyscout'                                       as data_source

    from {{ ref('stg_wyscout__events') }}
    where event_type = 'Shot'

),

unified as (

    select * from statsbomb_shots
    union all
    select * from wyscout_shots

)

select * from unified
