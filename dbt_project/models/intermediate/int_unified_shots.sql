{{ config(materialized='view', schema='silver') }}
-- int_unified_shots.sql
-- Union StatsBomb and Wyscout shot events into a Kimball-conformed shape.
-- Each per-source CTE emits an identical schema; the final `keyed` CTE joins
-- `dim_matches` to assign `match_key` (Kimball surrogate BIGINT FK, ADR-011).
-- Native `match_id` / `provider` / `native_match_id` are not present in the
-- output — recover via JOIN dim_matches ON match_key.
--
-- Materialization: view (upgraded from ephemeral in PR 3, mirroring PR 2's
-- int_unified_passes upgrade). Allows debuggability and direct test assertion
-- via test_dbt_shots_kimball_migration.py.

with statsbomb_shots as (

    select
        cast(event_id as string)                     as event_id,
        cast(match_id as string)                     as native_match_id,
        'statsbomb'                                  as provider,
        cast(player_id as bigint)                    as player_id,
        cast(team_id as bigint)                      as team_id,
        cast(period as bigint)                       as period,
        cast(minute as bigint)                       as minute,
        cast(second as bigint)                       as second,
        cast(location_x as double)                   as location_x,
        cast(location_y as double)                   as location_y,
        cast(end_location_x as double)               as end_location_x,
        cast(end_location_y as double)               as end_location_y,
        cast(end_location_z as double)               as end_location_z,
        shot_outcome,
        shot_body_part,
        shot_technique,
        shot_type,
        cast(statsbomb_xg as double)                 as statsbomb_xg,
        is_first_time,
        play_pattern,
        cast(distance_to_goal as double)             as distance_to_goal,
        cast(shot_angle as double)                   as shot_angle,
        'statsbomb'                                  as data_source

    from {{ ref('stg_statsbomb__shots') }}

),

wyscout_shots as (

    select
        cast(event_sk as string)                     as event_id,
        cast(match_id as string)                     as native_match_id,
        'wyscout'                                    as provider,
        cast(player_id as bigint)                    as player_id,
        cast(team_id as bigint)                      as team_id,
        cast(period as bigint)                       as period,
        cast(floor(event_sec / 60) as bigint)        as minute,
        cast(cast(event_sec as int) % 60 as bigint)  as second,
        cast(start_x as double)                      as location_x,
        cast(start_y as double)                      as location_y,
        cast(end_x as double)                        as end_location_x,
        cast(end_y as double)                        as end_location_y,
        cast(null as double)                         as end_location_z,
        case when is_goal then 'Goal' else 'No Goal' end as shot_outcome,
        case
            when sub_event_type like '%Head%' then 'Head'
            when sub_event_type like '%Right%' then 'Right Foot'
            when sub_event_type like '%Left%' then 'Left Foot'
            else 'Unknown'
        end                                          as shot_body_part,
        cast(null as string)                         as shot_technique,
        sub_event_type                               as shot_type,
        cast(null as double)                         as statsbomb_xg,
        cast(null as boolean)                        as is_first_time,
        cast(null as string)                         as play_pattern,
        {{ distance_to_goal('start_x', 'start_y') }} as distance_to_goal,
        {{ shot_angle('start_x', 'start_y') }}       as shot_angle,
        'wyscout'                                    as data_source

    from {{ ref('stg_wyscout__events') }}
    where event_type = 'Shot'
      -- PR 7 hotfix #3: Wyscout open-data uses `playerId: 0` as an "unknown
      -- player" sentinel (3 of 131,077 shots = 0.002%). Same pattern as
      -- int_unified_passes' Wyscout filter (PR #215 hotfix). Drop here at the
      -- staging boundary so dim_players LEFT JOIN in fct_shots resolves 100%
      -- on every Wyscout row.
      and player_id is not null and player_id <> 0

),

unioned as (

    select * from statsbomb_shots
    union all
    select * from wyscout_shots

),

keyed as (

    select
        u.event_id,
        dm.match_key,
        u.player_id,
        u.team_id,
        u.period,
        u.minute,
        u.second,
        u.location_x,
        u.location_y,
        u.end_location_x,
        u.end_location_y,
        u.end_location_z,
        u.shot_outcome,
        u.shot_body_part,
        u.shot_technique,
        u.shot_type,
        u.statsbomb_xg,
        u.is_first_time,
        u.play_pattern,
        u.distance_to_goal,
        u.shot_angle,
        u.data_source

    from unioned u
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = u.provider
       and dm.native_match_id = u.native_match_id

)

select * from keyed
