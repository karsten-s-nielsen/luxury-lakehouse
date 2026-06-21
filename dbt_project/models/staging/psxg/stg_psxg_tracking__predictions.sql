-- stg_psxg_tracking__predictions.sql
-- Clean + dedup tracking-modality PSxG predictions (latest _ingested_at wins per shot).
-- Source: bronze.psxg_tracking_predictions (written by ingestion.compute_psxg_tracking).
-- Grain: one row per (match_key, action_id) — the SPADL shot universe.

{{ config(enabled=var('goalkeeper_enabled', false)) }}

with source as (

    select * from {{ source('psxg', 'psxg_tracking_predictions') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by match_key, action_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(match_key as bigint)              as match_key,
        cast(action_id as bigint)              as action_id,
        cast(data_source as string)            as data_source,
        -- NaN guard so SUM()/AVG() ignore gated (NULL) rows
        case when psxg is null or isnan(cast(psxg as double)) then null
             else cast(psxg as double) end     as psxg,
        case when psxg_recalibrated is null or isnan(cast(psxg_recalibrated as double)) then null
             else cast(psxg_recalibrated as double) end as psxg_recalibrated,
        cast(psxg_gated as boolean)            as psxg_gated,
        cast(psxg_calibration as string)       as psxg_calibration,
        cast(model_version as string)          as model_version,
        cast(platt_version as string)          as platt_version,
        cast(normalization_version as string)  as normalization_version,
        cast(_ingested_at as timestamp)        as _ingested_at

    from deduplicated
    where _row_num = 1

)

select * from cleaned
