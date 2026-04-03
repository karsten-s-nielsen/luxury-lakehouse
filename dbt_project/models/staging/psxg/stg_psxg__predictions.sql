-- stg_psxg__predictions.sql
-- Clean and deduplicate PSxG predictions by event_id (latest _ingested_at wins).
-- Source: bronze.psxg_predictions (imported from HF Hub).

{{ config(enabled=var('goalkeeper_enabled', false)) }}

with source as (

    select * from {{ source('psxg', 'psxg_predictions') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by event_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(event_id as string)        as event_id,
        cast(match_id as string)        as match_id,
        cast(player_id as string)       as player_id,
        -- NaN from off-target shots must become NULL so SUM() ignores them
        case when isnan(cast(psxg as double)) then null
             else cast(psxg as double) end as psxg,
        cast(_ingested_at as timestamp) as _ingested_at

    from deduplicated
    where _row_num = 1

)

select * from cleaned
