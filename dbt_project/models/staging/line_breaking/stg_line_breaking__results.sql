-- stg_line_breaking__results.sql
-- Clean and deduplicate line-breaking detection results from the bronze layer.
--
-- Dedup: ROW_NUMBER partitioned by event_id, latest _ingested_at wins.

with source as (

    select * from {{ source('line_breaking', 'line_breaking_results') }}

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
        cast(event_id as string)            as event_id,
        cast(match_id as string)            as match_id,
        cast(is_line_breaking as boolean)   as is_line_breaking,
        cast(lines_broken as int)           as lines_broken,
        cast(line_breaking_type as string)  as line_breaking_type,
        cast(data_source as string)         as data_source

    from deduplicated
    where _row_num = 1

)

select * from cleaned
