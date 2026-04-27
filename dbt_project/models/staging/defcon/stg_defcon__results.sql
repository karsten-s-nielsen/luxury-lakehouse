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

    -- All five integer-bearing columns are cast to BIGINT to match the
    -- bronze.defcon_results contract (`_RESULTS_SCHEMA` declares BIGINT
    -- for competition_id, season_id, defender_player_id, defender_team_id,
    -- and action_player_id). The previous `as int` casts were latent
    -- CAST_OVERFLOW bombs that detonated 2026-04-27 once PRs #209/#210
    -- and the StringType→LongType followup widened bronze ID columns to
    -- their declared BIGINT types — `monotonically_increasing_id()`
    -- returns 64-bit values and Spark ANSI rejects LONG→INT statically.
    -- defcon_value is widened FLOAT→DOUBLE (lossless) for downstream
    -- arithmetic; bronze remains FLOAT per the writer schema parity test.
    select
        cast(event_id as string)                as event_id,
        cast(match_id as string)                as match_id,
        cast(competition_id as bigint)          as competition_id,
        cast(season_id as bigint)               as season_id,
        cast(defender_player_id as bigint)      as defender_player_id,
        cast(defender_team_id as bigint)        as defender_team_id,
        cast(defender_x as double)              as defender_x,
        cast(defender_y as double)              as defender_y,
        cast(action_player_id as bigint)        as action_player_id,
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
