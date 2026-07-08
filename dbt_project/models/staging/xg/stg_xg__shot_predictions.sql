-- stg_xg__shot_predictions.sql
-- Staging view for canonical-SPADL pre-shot xG v3 predictions (xg_model_v3,
-- two-mode gate) from the bronze layer. Emits the shot key (match_key,
-- action_id) + v3 prediction columns per ADR-013; Kimball surrogates
-- (competition_key / team_key / player_key) are resolved at the mart layer
-- (fct_shot_xg) via INNER JOIN fct_action_values on (match_key, action_id).
--
-- The shot key is (match_key, action_id) — action_id is per-match, NOT global.
-- Dedup: ROW_NUMBER partitioned by (match_key, action_id), latest _ingested_at wins.

{{ config(enabled=var('xg_v3_enabled', false)) }}

with source as (

    select
        match_key,
        action_id,
        data_source,
        xg,
        xg_ci_low,
        xg_ci_high,
        scoring_mode,
        ood_flag,
        _ingested_at,
        row_number() over (
            partition by match_key, action_id
            order by _ingested_at desc
        ) as _row_num
    from {{ source('xg', 'xg_shot_predictions') }}

)

select
    cast(match_key as bigint)     as match_key,
    cast(action_id as bigint)     as action_id,
    cast(data_source as string)   as data_source,
    cast(xg as double)            as xg,
    cast(xg_ci_low as double)     as xg_ci_low,
    cast(xg_ci_high as double)    as xg_ci_high,
    cast(scoring_mode as string)  as scoring_mode,
    cast(ood_flag as boolean)     as ood_flag
from source
where _row_num = 1
