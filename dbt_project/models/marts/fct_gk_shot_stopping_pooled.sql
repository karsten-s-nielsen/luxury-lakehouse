{{ config(
    materialized='table',
    enabled=var('goalkeeper_enabled', false),
    liquid_clustered_by=['competition_key'],
    tags=['marts', 'output_mart']
) }}
-- fct_gk_shot_stopping_pooled.sql
-- Pooled GK x competition x season shot-stopping — the page's evaluative surface
-- (spec C3 / B1 / §10). A pure additive SUM rollup of fct_gk_shot_stopping.
--
-- Data-shaped by the verified volume (max GK faced 14 on-target shots): the
-- percentile LEADERBOARD is DEFERRED. ranking_enabled is true only when >=20 GKs
-- in the (competition, season) cohort clear the >=20 shots-faced floor — currently
-- FALSE everywhere, so goals_prevented_pctile is NULL. The primary surface is the
-- raw goals_prevented + a closed-form POISSON-BINOMIAL band (NOT bootstrap, which
-- is degenerate at n<15): Var(goals_prevented) = sum psxg_i*(1-psxg_i), rolled up
-- additively as psxg_variance_sum.

{% set z_score = 1.96 %}
{% set show_value_floor = 5 %}
{% set ranking_min_shots = 20 %}
{% set ranking_min_gks = 20 %}

with gms as (

    select * from {{ ref('fct_gk_shot_stopping') }}

),

pooled as (

    select
        player_key,
        competition_key,
        season_id,
        max(data_source)                  as data_source,
        sum(shots_faced)                  as shots_faced,
        sum(shots_faced_total)            as shots_faced_total,
        sum(goals_conceded_on_shots)      as goals_conceded_on_shots,
        sum(psxg_faced)                   as psxg_faced,
        sum(psxg_variance_sum)            as psxg_variance_sum
    from gms
    group by player_key, competition_key, season_id

),

cohort as (

    -- ranking is enabled per (competition, season) only when the cohort is deep enough
    select
        competition_key,
        season_id,
        sum(case when shots_faced_total >= {{ ranking_min_shots }} then 1 else 0 end) as n_above_floor
    from pooled
    group by competition_key, season_id

),

banded as (

    select
        p.*,
        (p.psxg_faced - p.goals_conceded_on_shots)        as goals_prevented,
        sqrt(p.psxg_variance_sum)                         as gp_sd,
        (c.n_above_floor >= {{ ranking_min_gks }})        as ranking_enabled
    from pooled p
    inner join cohort c
        on p.competition_key = c.competition_key and p.season_id = c.season_id

)

select
    {{ dbt_utils.generate_surrogate_key(['player_key', 'competition_key', 'season_id']) }} as gk_pooled_id,
    cast(player_key as bigint)                                  as player_key,
    cast(competition_key as bigint)                             as competition_key,
    cast(season_id as int)                                      as season_id,
    cast(data_source as string)                                 as data_source,
    cast(shots_faced as int)                                    as shots_faced,
    cast(shots_faced_total as int)                              as shots_faced_total,
    cast(goals_conceded_on_shots as int)                        as goals_conceded_on_shots,
    cast(psxg_faced as double)                                  as psxg_faced,
    cast(goals_prevented as double)                             as goals_prevented,
    cast(goals_prevented - {{ z_score }} * gp_sd as double)     as goals_prevented_ci_low,
    cast(goals_prevented + {{ z_score }} * gp_sd as double)     as goals_prevented_ci_high,
    cast(shots_faced_total < {{ show_value_floor }} as boolean) as low_sample,
    cast(ranking_enabled as boolean)                            as ranking_enabled,
    -- Percentile is computed ONLY where ranking is enabled; NULL otherwise (deferred).
    case when ranking_enabled
         then percent_rank() over (
                  partition by competition_key, season_id
                  order by goals_prevented
              )
         else cast(null as double)
    end                                                         as goals_prevented_pctile,
    current_timestamp()                                         as _loaded_at
from banded
