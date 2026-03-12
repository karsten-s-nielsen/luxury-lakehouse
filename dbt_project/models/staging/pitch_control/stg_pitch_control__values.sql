-- stg_pitch_control__values.sql
-- Clean and deduplicate pitch control values from the bronze layer.
--
-- Dedup: ROW_NUMBER partitioned by tracking_id, latest _ingested_at wins.

with source as (

    select * from {{ source('pitch_control', 'pitch_control_values') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by tracking_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(tracking_id as string)              as tracking_id,
        cast(match_id as string)                 as match_id,
        cast(pitch_control_value as double)      as pitch_control_value

    from deduplicated
    where _row_num = 1

)

select * from cleaned
