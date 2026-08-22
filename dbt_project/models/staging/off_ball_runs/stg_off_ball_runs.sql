-- stg_off_ball_runs.sql
-- Staging view for off-ball run detections + valuations (spec §7.5; ADR-013 writer-fed).
-- Source: bronze.off_ball_runs, written by ingestion.off_ball_runs_writer. Deduplicates by
-- (data_source, match_id, action_id, player_id), latest _ingested_at wins; renames match_id ->
-- native_match_id for the Kimball-side resolution in fct_off_ball_runs.

with source as (

    select * from {{ source('off_ball_runs', 'off_ball_runs') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by data_source, match_id, action_id, player_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(data_source as string)         as data_source,
        cast(match_id as string)            as native_match_id,
        cast(game_id as bigint)             as game_id,
        cast(period_id as bigint)           as period_id,
        cast(action_id as bigint)           as action_id,
        cast(player_id as string)           as player_id_native,
        cast(run_start_x as double)         as run_start_x,
        cast(run_start_y as double)         as run_start_y,
        cast(run_end_x as double)           as run_end_x,
        cast(run_end_y as double)           as run_end_y,
        cast(displacement_m as double)      as displacement_m,
        cast(duration_s as double)          as duration_s,
        cast(mean_speed_ms as double)       as mean_speed_ms,
        cast(peak_speed_ms as double)       as peak_speed_ms,
        cast(peak_speed_source as string)   as peak_speed_source,
        cast(toward_goal as boolean)        as toward_goal,
        cast(role as string)                as role,
        cast(is_receiver as boolean)        as is_receiver,
        cast(run_value as double)           as run_value,
        cast(enabled_pass_credit as double) as enabled_pass_credit

    from deduplicated
    where _row_num = 1

)

select * from cleaned
