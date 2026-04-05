-- stg_tracking__player_metadata.sql
-- Deduplicate tracking player metadata from source extraction.
--
-- Dedup: ROW_NUMBER partitioned by (match_id, player_id),
-- latest _ingested_at wins. Handles pipeline re-runs.

with source as (

    select * from {{ source('tracking', 'tracking_player_metadata') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by match_id, player_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(match_id as string)              as match_id,
        cast(player_id as string)             as player_id,
        cast(player_display_name as string)   as player_display_name,
        cast(team_side as string)             as team_side,
        cast(team_display_name as string)     as team_display_name,
        cast(jersey_number as int)            as jersey_number,
        cast(provider as string)              as provider,
        cast(_ingested_at as timestamp)       as _ingested_at

    from deduplicated
    where _row_num = 1

)

select * from cleaned
