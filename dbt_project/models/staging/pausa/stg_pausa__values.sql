-- stg_pausa__values.sql
-- Clean and deduplicate PAUSA values from the bronze layer.
--
-- Dedup: ROW_NUMBER partitioned by (pass_id), latest _ingested_at wins.
-- Enabled by pausa_enabled toggle.
--
-- PR 7 (ADR-013 second application): source repointed from
-- pausa_gold.fct_pausa_values (Python writer direct-write) to bronze.pausa_values
-- (Python writer raw output). The gold mart fct_pausa_values is now built by
-- dbt with contract: enforced: true and inherits Kimball FKs via INNER JOIN
-- to fct_passes on pass_id.

{{ config(enabled=var('pausa_enabled', false)) }}

with source as (

    select * from {{ source('pausa', 'pausa_values') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by pass_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(pass_id as string)              as pass_id,
        cast(match_id as string)             as match_id,
        cast(player_id as string)            as player_id,
        cast(team as string)                 as team,
        cast(period as int)                  as period,
        cast(timestamp_seconds as double)    as timestamp_seconds,
        cast(frame_id as int)                as frame_id,
        cast(temporal_judgment as double)     as temporal_judgment,
        cast(spatial_selection as double)     as spatial_selection,
        cast(pausa_score as double)          as pausa_score,
        cast(actual_obso as double)          as actual_obso,
        cast(peak_obso as double)            as peak_obso,
        cast(optimal_obso as double)         as optimal_obso,
        cast(receiver_x as double)           as receiver_x,
        cast(receiver_y as double)           as receiver_y

    from deduplicated
    where _row_num = 1

)

select * from cleaned
