-- stg_formations__labels.sql
-- Clean and deduplicate formation detection results from the bronze layer.
--
-- Dedup: ROW_NUMBER partitioned by (match_id, period, team, window_start_s),
-- latest _ingested_at wins. Handles pipeline re-runs producing duplicate rows.
--
-- Reference: Shaw, L. & Glickman, M. (2019). "Dynamic analysis of team
-- strategy in professional football."

with source as (

    select * from {{ source('formations', 'formation_labels') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by match_id, period, team, window_start_s
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(match_id as string)           as match_id,
        cast(period as int)                as period,
        cast(team as string)               as team,
        cast(window_start_s as double)     as window_start_s,
        cast(window_end_s as double)       as window_end_s,
        cast(formation_label as string)    as formation_label,
        cast(cost as double)               as cost,
        cast(_ingested_at as timestamp)    as _ingested_at

    from deduplicated
    where _row_num = 1

)

select * from cleaned
