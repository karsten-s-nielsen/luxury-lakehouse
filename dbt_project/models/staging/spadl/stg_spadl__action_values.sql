-- stg_spadl__action_values.sql
-- Clean and deduplicate VAEP action values from the bronze layer.
--
-- SPADL coordinate system:
--   - Pitch is 105 x 68 meters (academic standard)
--   - Origin (0,0) is bottom-left
--   - All actions normalized to attack left-to-right by socceraction
--
-- Dedup: ROW_NUMBER partitioned by natural key, latest _ingested_at wins.

with source as (

    select * from {{ source('spadl', 'vaep_action_values') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by game_id, period_id, time_seconds, player_id, type_id, data_source
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        -- Identifiers
        game_id                                         as match_id,
        cast(player_id as int)                          as player_id,
        cast(team_id as int)                            as team_id,
        original_event_id,

        -- Temporal
        cast(period_id as int)                          as period,
        time_seconds,
        cast(floor(time_seconds / 60) as int)           as minute,
        cast(floor(time_seconds % 60) as int)           as second,

        -- SPADL coordinates (105x68 meters)
        start_x,
        start_y,
        end_x,
        end_y,

        -- Action classification
        cast(type_id as int)                            as type_id,
        action_type,
        cast(result_id as int)                          as result_id,
        action_result,
        cast(bodypart_id as int)                        as bodypart_id,
        bodypart,

        -- VAEP scores
        offensive_value,
        defensive_value,
        vaep_value,

        -- Provenance
        data_source,
        cast(competition_id as int)                     as competition_id,
        cast(season_id as int)                          as season_id

    from deduplicated
    where _row_num = 1

)

select * from cleaned
