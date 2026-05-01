{{ config(
    materialized='incremental',
    unique_key='defensive_value_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['marts', 'output_mart']
) }}
-- fct_defensive_values.sql
-- Per-defender per-match defensive valuation summary.
--
-- Aggregates DEFCON-lite credits into per-match totals and breakdowns
-- by credit type. Enables defender ranking and comparison.
--
-- Coordinate system: SPADL 105x68 meters.
-- One row per defender per match per data_source.
--
-- PR 6 (ADR-011): Kimball surrogate FKs added.
--   - match_key BIGINT FK → dim_matches.match_key. Resolved via provider CASE
--     on data_source ('statsbomb_360' → 'statsbomb', 'metrica_tracking' → 'metrica').
--   - team_key BIGINT FK → dim_teams.team_key (defender's team).
--   - player_key BIGINT FK → dim_players.player_key (defender). Resolution
--     rate is structurally low (~16% on dev_gold) due to 360-synthetic
--     defenders that don't have real player_id. action_player_key (on
--     fct_defcon_actions) resolves cleanly. Tracked under
--     `relationships severity:warn` during 2026-07-22 dual-column window.
--   - data_source folded into surrogate hash (defensive_value_id) — multi-provider
--     correctness fix; existing IDs CHANGE on first --full-refresh rebuild.

{% if var('defcon_enabled', false) %}

with defcon as (

    select * from {{ ref('stg_defcon__results') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
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

credit_agg as (

    select
        defender_player_id,
        match_id,
        competition_id,
        season_id,
        defender_team_id,
        data_source,
        _provider,

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

    from defcon_with_provider
    group by defender_player_id, match_id, competition_id, season_id,
             defender_team_id, data_source, _provider

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'ca.defender_player_id',
            'ca.match_id',
            'ca.data_source'
        ]) }}                                                       as defensive_value_id,

        ca.defender_player_id                                       as player_id,
        ca.match_id,
        ca.competition_id,
        ca.season_id,
        ca.defender_team_id                                         as team_id,
        ca.data_source,

        -- PR 6 (ADR-011) Kimball surrogate FKs.
        dm.match_key,
        dt.team_key,
        dp.player_key,

        ca.total_defcon_value,
        ca.total_credits,

        ca.intercept_value,
        ca.concede_value,
        ca.disturb_value,
        ca.deter_value,

        ca.intercept_count,
        ca.concede_count,
        ca.disturb_count,
        ca.deter_count,

        ca.high_confidence_count,
        ca.approx_confidence_count,

        current_timestamp()                                         as _loaded_at

    from credit_agg ca
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = ca._provider
       and dm.native_match_id = ca.match_id
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = ca._provider
       and dt.native_team_id = cast(ca.defender_team_id as string)
    left join {{ ref('dim_players') }} dp
        on  dp.provider = ca._provider
       and dp.native_player_id = cast(ca.defender_player_id as string)

)

select * from final

{% else %}

-- DEFCON-lite not enabled — produce empty table with correct schema
select
    cast(null as string)    as defensive_value_id,
    cast(null as bigint)    as player_id,
    cast(null as string)    as match_id,
    cast(null as bigint)    as competition_id,
    cast(null as bigint)    as season_id,
    cast(null as bigint)    as team_id,
    cast(null as string)    as data_source,
    cast(null as bigint)    as match_key,
    cast(null as bigint)    as team_key,
    cast(null as bigint)    as player_key,
    cast(null as double)    as total_defcon_value,
    cast(null as bigint)    as total_credits,
    cast(null as double)    as intercept_value,
    cast(null as double)    as concede_value,
    cast(null as double)    as disturb_value,
    cast(null as double)    as deter_value,
    cast(null as bigint)    as intercept_count,
    cast(null as bigint)    as concede_count,
    cast(null as bigint)    as disturb_count,
    cast(null as bigint)    as deter_count,
    cast(null as bigint)    as high_confidence_count,
    cast(null as bigint)    as approx_confidence_count,
    current_timestamp()     as _loaded_at
where 1 = 0

{% endif %}
