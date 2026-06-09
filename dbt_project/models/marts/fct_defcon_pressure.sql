{{ config(
    materialized='incremental',
    unique_key='pressure_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    pre_hook="{{ reprocess_delete_hook('match_id') }}",
    tags=['marts', 'output_mart'],
    tblproperties={
        'delta.enableChangeDataFeed': 'true',
    }
) }}
-- fct_defcon_pressure.sql
-- Per-attacker per-match defensive pressure summary.
--
-- Aggregates DEFCON-lite credits by action_player_id (the real player
-- who performed the action) rather than defender_player_id (synthetic
-- for 360 freeze-frame data). Powers the Defensive Impact page pressure
-- view: how much defensive attention each attacker attracted.
--
-- Coordinate system: SPADL 105x68 meters.
-- One row per attacker per match per data_source.
--
-- PR 6 (ADR-011): Kimball surrogate FKs added.
--   - match_key + player_key (action attacker) LEFT JOIN-resolved
--     via provider CASE on data_source.
--   - data_source folded into surrogate hash (pressure_id) — existing IDs
--     CHANGE on first --full-refresh rebuild.
-- No team_key — action_team_id is not present on the source grain.
-- LEFT JOIN with relationships severity:warn during 2026-07-22 dual-column window.

{% if var('defcon_enabled', false) %}

with defcon as (

    select
        match_id,
        competition_id,
        season_id,
        action_player_id,
        credit_type,
        confidence,
        defcon_value,
        data_source
    from {{ ref('stg_defcon__results') }}
    where action_player_id is not null
    {% if is_incremental() %}
    and (match_id not in (select distinct match_id from {{ this }}) {{ reprocess_predicate('match_id') }})
    {% endif %}

),

defcon_with_provider as (

    select
        *,
        case data_source
            when 'statsbomb_360'    then 'statsbomb'
            when 'metrica_tracking' then 'metrica'
        end as _provider
    from defcon

),

pressure_agg as (

    select
        action_player_id,
        match_id,
        competition_id,
        season_id,
        data_source,
        _provider,

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

    from defcon_with_provider
    group by action_player_id, match_id, competition_id, season_id, data_source, _provider

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'pa.action_player_id',
            'pa.match_id',
            'pa.data_source'
        ]) }}                                                           as pressure_id,

        pa.action_player_id                                             as player_id,
        pa.match_id,
        pa.competition_id,
        pa.season_id,
        pa.data_source,

        -- PR 6 (ADR-011) Kimball surrogate FKs.
        dm.match_key,
        dp.player_key,

        pa.total_pressure,
        pa.total_defensive_actions,

        pa.intercept_pressure,
        pa.concede_pressure,
        pa.disturb_pressure,
        pa.deter_pressure,

        pa.intercept_count,
        pa.concede_count,
        pa.disturb_count,
        pa.deter_count,

        pa.high_confidence_count,
        pa.approx_confidence_count,

        current_timestamp()                                             as _loaded_at

    from pressure_agg pa
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = pa._provider
       and dm.native_match_id = pa.match_id
    left join {{ ref('dim_players') }} dp
        on  dp.provider = pa._provider
       and dp.native_player_id = cast(pa.action_player_id as string)

)

select * from final

{% else %}

select
    cast(null as string)    as pressure_id,
    cast(null as bigint)    as player_id,
    cast(null as string)    as match_id,
    cast(null as bigint)    as competition_id,
    cast(null as bigint)    as season_id,
    cast(null as string)    as data_source,
    cast(null as bigint)    as match_key,
    cast(null as bigint)    as player_key,
    cast(null as double)    as total_pressure,
    cast(null as bigint)    as total_defensive_actions,
    cast(null as double)    as intercept_pressure,
    cast(null as double)    as concede_pressure,
    cast(null as double)    as disturb_pressure,
    cast(null as double)    as deter_pressure,
    cast(null as bigint)    as intercept_count,
    cast(null as bigint)    as concede_count,
    cast(null as bigint)    as disturb_count,
    cast(null as bigint)    as deter_count,
    cast(null as bigint)    as high_confidence_count,
    cast(null as bigint)    as approx_confidence_count,
    current_timestamp()     as _loaded_at
where 1 = 0

{% endif %}
