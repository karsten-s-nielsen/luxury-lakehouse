-- stg_xg__predictions.sql
-- Staging view for custom xG model predictions from the bronze layer.
--
-- Dedup: ROW_NUMBER partitioned by shot_id, latest _ingested_at wins.

{{ config(enabled=var('xg_model_enabled', false)) }}

with source as (

    select
        shot_id,
        match_id,
        competition_id,
        xg_logistic,
        xg_gradient_boosted,
        _ingested_at,
        row_number() over (
            partition by shot_id
            order by _ingested_at desc
        ) as _row_num
    from {{ source('xg', 'xg_predictions') }}

)

select
    cast(shot_id as string)            as shot_id,
    cast(match_id as bigint)           as match_id,
    cast(competition_id as int)        as competition_id,
    cast(xg_logistic as double)        as xg_logistic,
    cast(xg_gradient_boosted as double) as xg_gradient_boosted
from source
where _row_num = 1
