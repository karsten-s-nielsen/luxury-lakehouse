-- stg_xt_gk_v2.sql
-- Staging view for the xT-GK v2 predictions (spec §7.4; ADR-013 writer-join).
-- Source: bronze.xt_gk_v2_predictions, written by ingestion.xt_gk_v2_writer (the fitted
-- MarkovPossessionValue + EmpiricalTurnoverValue surfaces score xt_gk_v2 per GK-distribution action).
-- Deduplicates by (match_id, action_id), latest _ingested_at wins; renames match_id -> native_match_id
-- for the Kimball-side LEFT JOIN in fct_action_context. Always enabled (fct_action_context refs it).

with source as (

    select * from {{ source('xt_gk_v2', 'xt_gk_v2_predictions') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by match_id, action_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(data_source as string)             as data_source,
        cast(match_id as string)                as native_match_id,
        cast(action_id as bigint)               as action_id,
        cast(xt_gk_v2_position as double)       as xt_gk_v2_position,
        cast(xt_gk_v2_pev as double)            as xt_gk_v2_pev,
        cast(xt_gk_v2_retention_loss as double) as xt_gk_v2_retention_loss,
        cast(xt_gk_v2_dzv as double)            as xt_gk_v2_dzv,
        cast(xt_gk_v2 as double)                as xt_gk_v2,
        cast(gk_geometry_source as string)      as gk_geometry_source

    from deduplicated
    where _row_num = 1

)

select * from cleaned
