{{ config(
    materialized='incremental',
    unique_key='pressure_id',
    cluster_by=['match_id'],
    incremental_strategy='merge'
) }}
-- fct_defcon_pressure.sql
-- Per-attacker per-match defensive pressure summary.
--
-- Aggregates DEFCON-lite credits by action_player_id (the real player
-- who performed the action) rather than defender_player_id (synthetic
-- for 360 freeze-frame data). This provides a "pressure received" view:
-- how much defensive attention each attacker attracted.
--
-- Coordinate system: SPADL 105x68 meters.
-- One row per attacker per match.

{% if var('defcon_enabled', false) %}

with defcon as (

    select * from {{ ref('stg_defcon__results') }}
    where action_player_id is not null
    {% if is_incremental() %}
    and match_id not in (select distinct match_id from {{ this }})
    {% endif %}

),

pressure_agg as (

    select
        action_player_id,
        match_id,
        competition_id,
        season_id,
        data_source,

        sum(defcon_value)                                               as total_pressure,
        count(*)                                                        as total_defensive_actions,

        sum(case when credit_type = 'intercept' then defcon_value else 0 end) as intercept_pressure,
        sum(case when credit_type = 'concede' then defcon_value else 0 end)   as concede_pressure,
        sum(case when credit_type = 'disturb' then defcon_value else 0 end)   as disturb_pressure,
        sum(case when credit_type = 'deter' then defcon_value else 0 end)     as deter_pressure,

        sum(case when credit_type = 'intercept' then 1 else 0 end)           as intercept_count,
        sum(case when credit_type = 'concede' then 1 else 0 end)             as concede_count,
        sum(case when credit_type = 'disturb' then 1 else 0 end)             as disturb_count,
        sum(case when credit_type = 'deter' then 1 else 0 end)               as deter_count,

        sum(case when confidence = 'high' then 1 else 0 end)                 as high_confidence_count,
        sum(case when confidence = 'approximate' then 1 else 0 end)           as approx_confidence_count

    from defcon
    group by action_player_id, match_id, competition_id, season_id, data_source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'action_player_id',
            'match_id'
        ]) }}                                                           as pressure_id,

        action_player_id                                                as player_id,
        match_id,
        competition_id,
        season_id,
        data_source,

        total_pressure,
        total_defensive_actions,

        intercept_pressure,
        concede_pressure,
        disturb_pressure,
        deter_pressure,

        intercept_count,
        concede_count,
        disturb_count,
        deter_count,

        high_confidence_count,
        approx_confidence_count,

        current_timestamp()                                             as _loaded_at

    from pressure_agg

)

select * from final

{% else %}

-- DEFCON-lite not enabled — produce empty table with correct schema
select
    cast(null as string)    as pressure_id,
    cast(null as int)       as player_id,
    cast(null as string)    as match_id,
    cast(null as int)       as competition_id,
    cast(null as int)       as season_id,
    cast(null as string)    as data_source,
    cast(null as double)    as total_pressure,
    cast(null as int)       as total_defensive_actions,
    cast(null as double)    as intercept_pressure,
    cast(null as double)    as concede_pressure,
    cast(null as double)    as disturb_pressure,
    cast(null as double)    as deter_pressure,
    cast(null as int)       as intercept_count,
    cast(null as int)       as concede_count,
    cast(null as int)       as disturb_count,
    cast(null as int)       as deter_count,
    cast(null as int)       as high_confidence_count,
    cast(null as int)       as approx_confidence_count,
    current_timestamp()     as _loaded_at
where 1 = 0

{% endif %}
