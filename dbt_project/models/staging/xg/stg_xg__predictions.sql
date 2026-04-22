-- stg_xg__predictions.sql
-- Staging view for custom v1 xG model predictions from the bronze layer.
--
-- Post-PR 3 (ADR-013): emits only shot_id + v1 prediction columns. Kimball
-- keys (match_key, competition_key) are resolved at the mart layer via INNER
-- JOIN fct_shots ON shot_id; legacy native competition_id also comes from
-- fct_shots. Bronze.xg_predictions still carries match_id and competition_id
-- for bronze back-compat (Chesterton's Fence) — staging deliberately drops them.
--
-- Dedup: ROW_NUMBER partitioned by shot_id, latest _ingested_at wins.

{{ config(enabled=var('xg_model_enabled', false)) }}

with source as (

    select
        shot_id,
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
    cast(shot_id as string)             as shot_id,
    cast(xg_logistic as double)         as xg_logistic,
    cast(xg_gradient_boosted as double) as xg_gradient_boosted
from source
where _row_num = 1
