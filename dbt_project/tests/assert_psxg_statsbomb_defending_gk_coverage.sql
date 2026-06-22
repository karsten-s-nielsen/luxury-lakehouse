-- A2 attribution regression guard. The StatsBomb defending-GK in fct_shot_psxg is
-- resolved from the match LINEUP (sb_gk_per_match: dim_players GKs + their actions,
-- attributed to the opposing team), NOT the ~97%-NULL per-shot
-- fct_action_values.defending_gk_player_id. If a change reverts to the sparse per-shot
-- id, defending_gk_player_key NULL-rate spikes and fct_gk_shot_stopping silently drops
-- most shots from goals_prevented (the 2026-06-21 incident: 96.9% NULL collapsed
-- psxg_faced — Valdes 254 -> 10.8). Guard the coverage: the StatsBomb NULL-rate must
-- stay low. Currently ~2.8% (the residual is matches with no recorded GK actions); the
-- 0.10 threshold sits well above that and far below the regression. Returns a row
-- (fails) when the rate exceeds the threshold.
{{ config(enabled=var('goalkeeper_enabled', false), severity='error') }}

with cov as (

    select
        count(*)                                                          as n,
        sum(case when defending_gk_player_key is null then 1 else 0 end)  as n_null
    from {{ ref('fct_shot_psxg') }}
    where data_source = 'statsbomb'

)

select
    n,
    n_null,
    n_null / nullif(n, 0) as null_frac
from cov
where n > 0
  and n_null / nullif(n, 0) > 0.10
