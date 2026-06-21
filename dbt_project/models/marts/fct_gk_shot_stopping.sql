{{ config(
    materialized='table',
    enabled=var('goalkeeper_enabled', false),
    liquid_clustered_by=['match_key'],
    tags=['marts', 'output_mart']
) }}
-- fct_gk_shot_stopping.sql
-- GK x match shot-stopping aggregate — a DERIVED additive rollup of fct_shot_psxg
-- over shots FACED (the GK is the defending_gk_player_key). Replaces the inlined
-- psxg_agg CTE in fct_goalkeeper_stats (spec D-B / §10).
--
-- B2 HONESTY: this GK x match grain is a DRILL-DOWN DETAIL, NOT an evaluative number
-- (avg ~1 on-target shot faced per match — noise). The evaluative surface is the
-- pooled layer (fct_gk_shot_stopping_pooled).
-- B3 PAIRING: goals_conceded_on_shots counts goals among GATE-PASSED shots only
-- (the same set psxg_faced sums), so goals_prevented never mixes denominators; a
-- gated-out conceded goal is excluded from both but counted in shots_faced_total.
-- All measures are fully additive (pure SUM rollup → pooled layer).

with shots as (

    select * from {{ ref('fct_shot_psxg') }}
    where defending_gk_player_key is not null

),

agg as (

    select
        defending_gk_player_key                                                as player_key,
        match_key,
        max(competition_key)                                                   as competition_key,
        max(season_id)                                                         as season_id,
        max(data_source)                                                       as data_source,
        count(*)                                                               as shots_faced_total,
        sum(case when not psxg_gated then 1 else 0 end)                        as shots_faced,
        sum(case when (not psxg_gated) and is_goal then 1 else 0 end)          as goals_conceded_on_shots,
        sum(case when not psxg_gated then psxg_recalibrated else 0 end)        as psxg_faced,
        sum(case when not psxg_gated
                 then psxg_recalibrated * (1 - psxg_recalibrated) else 0 end)  as psxg_variance_sum,
        max(psxg_calibration)                                                  as psxg_calibration,
        max(model_version)                                                     as model_version,
        max(platt_version)                                                     as platt_version,
        max(normalization_version)                                            as normalization_version
    from shots
    group by defending_gk_player_key, match_key

)

select
    {{ dbt_utils.generate_surrogate_key(['player_key', 'match_key']) }} as gk_shot_stopping_id,
    cast(player_key as bigint)                                          as player_key,
    cast(match_key as bigint)                                          as match_key,
    cast(competition_key as bigint)                                    as competition_key,
    cast(season_id as int)                                             as season_id,
    cast(data_source as string)                                        as data_source,
    cast(shots_faced as int)                                           as shots_faced,
    cast(shots_faced_total as int)                                     as shots_faced_total,
    cast(goals_conceded_on_shots as int)                               as goals_conceded_on_shots,
    cast(psxg_faced as double)                                         as psxg_faced,
    cast(psxg_faced - goals_conceded_on_shots as double)               as goals_prevented,
    cast(psxg_variance_sum as double)                                  as psxg_variance_sum,
    cast(shots_faced_total < 5 as boolean)                             as low_sample,
    cast(psxg_calibration as string)                                   as psxg_calibration,
    cast(model_version as string)                                      as model_version,
    cast(platt_version as string)                                      as platt_version,
    cast(normalization_version as string)                              as normalization_version,
    current_timestamp()                                                as _loaded_at
from agg
