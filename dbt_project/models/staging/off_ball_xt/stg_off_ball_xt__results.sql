-- stg_off_ball_xt__results.sql
-- Clean and deduplicate Off-Ball xT results from the bronze layer.
--
-- Dedup: ROW_NUMBER partitioned by (player_id, match_id), latest _ingested_at wins.

with source as (

    select * from {{ source('off_ball_xt', 'off_ball_xt_results') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by player_id, match_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(player_id as string)          as player_id,
        cast(match_id as string)           as match_id,
        cast(total_off_ball_xt as double)  as total_off_ball_xt,
        cast(avg_off_ball_xt as double)    as avg_off_ball_xt,
        cast(frames_sampled as int)        as frames_sampled

    from deduplicated
    where _row_num = 1

)

select * from cleaned
