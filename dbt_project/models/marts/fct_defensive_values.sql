{{ config(
    materialized='incremental',
    unique_key='defensive_value_id',
    liquid_clustered_by=['match_id'],
    incremental_strategy='merge'
) }}
-- fct_defensive_values.sql
-- Per-defender per-match defensive valuation summary.
--
-- Aggregates DEFCON-lite credits into per-match totals and breakdowns
-- by credit type. Enables defender ranking and comparison.
--
-- Coordinate system: SPADL 105x68 meters.
-- One row per defender per match.

{% if var('defcon_enabled', false) %}

with defcon as (

    select * from {{ ref('stg_defcon__results') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}

),

credit_agg as (

    select
        defender_player_id,
        match_id,
        competition_id,
        season_id,
        defender_team_id,
        data_source,

        sum(defcon_value)                                           as total_defcon_value,
        count(*)                                                    as total_credits,

        sum(case when credit_type = 'intercept' then defcon_value else 0 end) as intercept_value,
        sum(case when credit_type = 'concede' then defcon_value else 0 end)   as concede_value,
        sum(case when credit_type = 'disturb' then defcon_value else 0 end)   as disturb_value,
        sum(case when credit_type = 'deter' then defcon_value else 0 end)     as deter_value,

        sum(case when credit_type = 'intercept' then 1 else 0 end)           as intercept_count,
        sum(case when credit_type = 'concede' then 1 else 0 end)             as concede_count,
        sum(case when credit_type = 'disturb' then 1 else 0 end)             as disturb_count,
        sum(case when credit_type = 'deter' then 1 else 0 end)               as deter_count,

        sum(case when confidence = 'high' then 1 else 0 end)                 as high_confidence_count,
        sum(case when confidence = 'approximate' then 1 else 0 end)           as approx_confidence_count

    from defcon
    group by defender_player_id, match_id, competition_id, season_id,
             defender_team_id, data_source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'defender_player_id',
            'match_id'
        ]) }}                                                       as defensive_value_id,

        defender_player_id                                          as player_id,
        match_id,
        competition_id,
        season_id,
        defender_team_id                                            as team_id,
        data_source,

        total_defcon_value,
        total_credits,

        intercept_value,
        concede_value,
        disturb_value,
        deter_value,

        intercept_count,
        concede_count,
        disturb_count,
        deter_count,

        high_confidence_count,
        approx_confidence_count,

        current_timestamp()                                         as _loaded_at

    from credit_agg

)

select * from final

{% else %}

-- DEFCON-lite not enabled — produce empty table with correct schema
select
    cast(null as string)    as defensive_value_id,
    cast(null as int)       as player_id,
    cast(null as string)    as match_id,
    cast(null as int)       as competition_id,
    cast(null as int)       as season_id,
    cast(null as int)       as team_id,
    cast(null as string)    as data_source,
    cast(null as double)    as total_defcon_value,
    cast(null as int)       as total_credits,
    cast(null as double)    as intercept_value,
    cast(null as double)    as concede_value,
    cast(null as double)    as disturb_value,
    cast(null as double)    as deter_value,
    cast(null as int)       as intercept_count,
    cast(null as int)       as concede_count,
    cast(null as int)       as disturb_count,
    cast(null as int)       as deter_count,
    cast(null as int)       as high_confidence_count,
    cast(null as int)       as approx_confidence_count,
    current_timestamp()     as _loaded_at
where 1 = 0

{% endif %}
