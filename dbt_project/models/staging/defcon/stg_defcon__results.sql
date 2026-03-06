-- stg_defcon__results.sql
-- Clean and deduplicate DEFCON-lite results from the bronze layer.
--
-- Dedup: ROW_NUMBER partitioned by (event_id, defender_player_id, data_source),
-- latest _ingested_at wins.

{{ config(enabled=var('defcon_enabled', false)) }}

with source as (

    select * from {{ source('defcon', 'defcon_results') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by event_id, defender_player_id, data_source
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(event_id as string)                as event_id,
        cast(match_id as string)                as match_id,
        cast(competition_id as int)             as competition_id,
        cast(season_id as int)                  as season_id,
        cast(defender_player_id as int)         as defender_player_id,
        cast(defender_team_id as int)           as defender_team_id,
        cast(defender_x as double)              as defender_x,
        cast(defender_y as double)              as defender_y,
        cast(action_player_id as int)           as action_player_id,
        cast(action_type as string)             as action_type,
        cast(action_x as double)               as action_x,
        cast(action_y as double)               as action_y,
        cast(credit_type as string)             as credit_type,
        cast(confidence as string)              as confidence,
        cast(defcon_value as double)            as defcon_value,
        cast(dist_to_ball as double)            as dist_to_ball,
        cast(pitch_control_at_action as double) as pitch_control_at_action,
        cast(data_source as string)             as data_source

    from deduplicated
    where _row_num = 1

)

select * from cleaned
