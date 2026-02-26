-- int_unified_shots.sql
-- Union StatsBomb and Wyscout shot data into a common schema.
--
-- This intermediate model creates a single unified shots table that
-- combines shots from multiple data providers, enabling cross-source
-- analysis and model training.
--
-- Materialized as ephemeral (CTE) — no physical table created.
-- Downstream mart models (fct_shots) reference this.
--
-- Schema alignment approach:
--   1. Map each source's shot fields to a common column set
--   2. Add a `data_source` column to track provenance
--   3. Standardize outcome values (Goal, Saved, Blocked, Off Target, etc.)
--   4. Ensure all coordinates are in the 120x80 system
--   5. NULL out fields that don't exist in one source
--      (e.g. statsbomb_xg only exists for StatsBomb data)

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
        event_id,
        match_id,
        player_id,
        team_id,
        period,
        -- TODO: Convert event_sec to minute and second
        cast(floor(event_sec / 60) as int)              as minute,
        cast(mod(cast(event_sec as int), 60) as int)    as second,
        start_x                                         as location_x,
        start_y                                         as location_y,
        end_x                                           as end_location_x,
        end_y                                           as end_location_y,
        -- TODO: Map Wyscout outcome tags to standardized outcome values
        case
            when is_goal then 'Goal'
            else 'No Goal'
        end                                             as shot_outcome,
        -- TODO: Extract body part from sub_event_type (e.g. "Head shot" → "Head")
        cast(null as string)                            as shot_body_part,
        cast(null as string)                            as shot_technique,
        sub_event_type                                  as shot_type,
        cast(null as double)                            as statsbomb_xg,   -- Not available in Wyscout
        cast(null as boolean)                           as is_first_time,  -- Not available in Wyscout
        -- TODO: Compute via macros once locations are populated
        -- {{ distance_to_goal('start_x', 'start_y') }} as distance_to_goal,
        -- {{ shot_angle('start_x', 'start_y') }}       as shot_angle,
        cast(null as double)                            as distance_to_goal,
        cast(null as double)                            as shot_angle,
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
