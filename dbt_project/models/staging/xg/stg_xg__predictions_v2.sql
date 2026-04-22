-- stg_xg__predictions_v2.sql
-- Staging view for custom v2 xG model predictions from the bronze layer.
-- Emits only shot_id + v2 prediction columns per ADR-013; Kimball keys
-- (match_key, competition_key) are resolved at the mart layer via INNER
-- JOIN fct_shots ON shot_id.
--
-- Dedup: ROW_NUMBER partitioned by shot_id, latest _ingested_at wins.

{{ config(enabled=var('xg_v2_enabled', false)) }}

with source as (

    select
        shot_id,
        xg_set_encoder,
        xg_ci_lower,
        xg_ci_upper,
        _ingested_at,
        row_number() over (
            partition by shot_id
            order by _ingested_at desc
        ) as _row_num
    from {{ source('xg', 'xg_predictions_v2') }}

)

select
    cast(shot_id as string)          as shot_id,
    cast(xg_set_encoder as double)   as xg_set_encoder,
    cast(xg_ci_lower as double)      as xg_ci_lower,
    cast(xg_ci_upper as double)      as xg_ci_upper
from source
where _row_num = 1
