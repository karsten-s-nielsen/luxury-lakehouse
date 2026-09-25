{{ config(
    materialized='table',
    enabled=var('goalkeeper_enabled', false),
    liquid_clustered_by=['match_key'],
    tags=['marts', 'output_mart']
) }}
-- fct_gk_shot_stopping.sql
-- GK x match shot-stopping aggregate. sk4118 P1 Task B1 SUPERSESSION: the GSAA counts
-- (shots_faced / goals_conceded_on_shots / psxg_faced / goals_prevented) are RE-SOURCED
-- from the silly-kicks compute_shot_stopping writer (stg_shot_stopping) — canonical-id
-- keeper grouping fixes cross-provider keeper-id fragmentation, and the metric now ships
-- the 4 _excl_penalties companions (in-play penalties split out). The pre-gate volume
-- (shots_faced_total), the Poisson-binomial band support (psxg_variance_sum), and the
-- calibration/version provenance are RETAINED from fct_shot_psxg (sk emits none of these) —
-- they feed the pooled layer's closed-form band. The two are joined on (player_key, match_key):
-- stg_shot_stopping resolves native keeper->player_key via dim_players and native match->match_key
-- via dim_matches.
--
-- B2 HONESTY: this GK x match grain is a DRILL-DOWN DETAIL, NOT an evaluative number
-- (avg ~1 on-target shot faced per match — noise). The evaluative surface is the
-- pooled layer (fct_gk_shot_stopping_pooled).
-- B3 PAIRING: goals_conceded_on_shots counts goals among the on-target attributed shots
-- (the same set psxg_faced sums), so goals_prevented never mixes denominators.
-- HYRUM: pre-existing column names/shape are preserved byte-for-byte (a re-source, not a
-- rename); the 4 _excl_penalties columns are ADDED (additive contract extension).

with ss as (

    -- sk compute_shot_stopping output, native ids resolved to Kimball surrogates.
    -- INNER JOIN dim_matches (identity fact); LEFT JOIN dim_players (a keeper without a
    -- dim_players row lands NULL player_key — filtered out below to preserve not_null(player_key)).
    select
        dp.player_key                                          as player_key,
        dm.match_key                                           as match_key,
        s.data_source                                          as data_source,
        s.shots_faced                                          as shots_faced,
        s.goals_conceded                                       as goals_conceded_on_shots,
        s.psxg_faced                                           as psxg_faced,
        s.goals_prevented                                      as goals_prevented,
        s.shots_faced_excl_penalties                           as shots_faced_excl_penalties,
        s.goals_conceded_excl_penalties                        as goals_conceded_excl_penalties,
        s.psxg_faced_excl_penalties                            as psxg_faced_excl_penalties,
        s.goals_prevented_excl_penalties                       as goals_prevented_excl_penalties
    from {{ ref('stg_shot_stopping') }} s
    inner join {{ ref('dim_matches') }} dm
        on  dm.provider = s.data_source
       and dm.native_match_id = s.native_match_id
    left join {{ ref('dim_players') }} dp
        on  dp.provider = s.data_source
       and dp.native_player_id = s.native_player_id

),

-- Band support + pre-gate volume + calibration/version provenance from the per-shot PSxG fact
-- (sk's compute_shot_stopping emits none of these). Rolled up additively per (keeper, match).
psxg_support as (

    select
        defending_gk_player_key                                        as player_key,
        match_key,
        max(competition_key)                                           as competition_key,
        max(season_id)                                                 as season_id,
        count(*)                                                       as shots_faced_total,
        sum(case when not psxg_gated
                 then psxg_recalibrated * (1 - psxg_recalibrated) else 0 end)  as psxg_variance_sum,
        max(psxg_calibration)                                         as psxg_calibration,
        max(model_version)                                            as model_version,
        max(platt_version)                                            as platt_version,
        max(normalization_version)                                   as normalization_version
    from {{ ref('fct_shot_psxg') }}
    where defending_gk_player_key is not null
    group by defending_gk_player_key, match_key

)

select
    {{ dbt_utils.generate_surrogate_key(['ss.player_key', 'ss.match_key']) }} as gk_shot_stopping_id,
    cast(ss.player_key as bigint)                                      as player_key,
    cast(ss.match_key as bigint)                                       as match_key,
    cast(sup.competition_key as bigint)                               as competition_key,
    cast(sup.season_id as int)                                        as season_id,
    cast(ss.data_source as string)                                    as data_source,
    cast(ss.shots_faced as int)                                       as shots_faced,
    cast(coalesce(sup.shots_faced_total, ss.shots_faced) as int)      as shots_faced_total,
    cast(ss.goals_conceded_on_shots as int)                          as goals_conceded_on_shots,
    cast(ss.psxg_faced as double)                                     as psxg_faced,
    cast(ss.goals_prevented as double)                               as goals_prevented,
    cast(ss.shots_faced_excl_penalties as int)                       as shots_faced_excl_penalties,
    cast(ss.goals_conceded_excl_penalties as int)                    as goals_conceded_excl_penalties,
    cast(ss.psxg_faced_excl_penalties as double)                     as psxg_faced_excl_penalties,
    cast(ss.goals_prevented_excl_penalties as double)                as goals_prevented_excl_penalties,
    cast(coalesce(sup.psxg_variance_sum, 0.0) as double)             as psxg_variance_sum,
    cast(coalesce(sup.shots_faced_total, ss.shots_faced) < 5 as boolean) as low_sample,
    cast(sup.psxg_calibration as string)                             as psxg_calibration,
    cast(sup.model_version as string)                                as model_version,
    cast(sup.platt_version as string)                                as platt_version,
    cast(sup.normalization_version as string)                        as normalization_version,
    current_timestamp()                                              as _loaded_at
from ss
left join psxg_support sup
    on  sup.player_key = ss.player_key
   and sup.match_key = ss.match_key
where ss.player_key is not null
