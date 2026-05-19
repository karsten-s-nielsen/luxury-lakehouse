{{ config(
    materialized='incremental',
    unique_key='defcon_action_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['marts', 'output_mart']
) }}
-- fct_defcon_actions.sql
-- Per-defender per-action defensive credits for timeline visualization.
--
-- Contains the full granularity of DEFCON-lite results: one row per
-- defender per credited action. Powers the Match Timeline view in
-- the Defensive Impact page.
--
-- Coordinate system: SPADL 105x68 meters.
-- One row per defender per action.
--
-- PR 6 (ADR-011): Kimball surrogate FKs added.
--   - match_key, team_key (defender), player_key (defender),
--     action_player_key (action-performing player) all LEFT JOIN-resolved
--     via provider CASE on data_source.
--   - defcon_action_id surrogate UNCHANGED — already includes data_source.
-- LEFT JOIN with relationships severity:warn during 2026-07-22 dual-column window.
-- 360-synthetic defenders: defender_player_key may be NULL (synthetic IDs
-- don't resolve in dim_players); action_player_key resolves cleanly.

{% if var('defcon_enabled', false) %}

with defcon as (

    select
        event_id,
        match_id,
        competition_id,
        season_id,
        defender_player_id,
        defender_team_id,
        defender_x,
        defender_y,
        action_player_id,
        action_type,
        action_x,
        action_y,
        credit_type,
        confidence,
        defcon_value,
        dist_to_ball,
        pitch_control_at_action,
        data_source
    from {{ ref('stg_defcon__results') }}
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

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'd.event_id',
            'd.defender_player_id',
            'd.data_source'
        ]) }}                                                       as defcon_action_id,

        d.event_id,
        d.match_id,
        d.competition_id,
        d.season_id,
        d.defender_player_id                                        as player_id,
        d.defender_team_id                                          as team_id,
        d.defender_x,
        d.defender_y,
        d.action_player_id,
        d.action_type,
        d.action_x,
        d.action_y,
        d.credit_type,
        d.confidence,
        d.defcon_value,
        d.dist_to_ball,
        d.pitch_control_at_action,
        d.data_source,

        -- PR 6 (ADR-011) Kimball surrogate FKs.
        dm.match_key,
        dt.team_key,
        dp_def.player_key,
        dp_act.player_key                                           as action_player_key,

        current_timestamp()                                         as _loaded_at

    from defcon_with_provider d
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = d._provider
       and dm.native_match_id = d.match_id
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = d._provider
       and dt.native_team_id = cast(d.defender_team_id as string)
    left join {{ ref('dim_players') }} dp_def
        on  dp_def.provider = d._provider
       and dp_def.native_player_id = cast(d.defender_player_id as string)
    left join {{ ref('dim_players') }} dp_act
        on  dp_act.provider = d._provider
       and dp_act.native_player_id = cast(d.action_player_id as string)

)

select * from final

{% else %}

select
    cast(null as string)    as defcon_action_id,
    cast(null as string)    as event_id,
    cast(null as string)    as match_id,
    cast(null as bigint)    as competition_id,
    cast(null as bigint)    as season_id,
    cast(null as bigint)    as player_id,
    cast(null as bigint)    as team_id,
    cast(null as double)    as defender_x,
    cast(null as double)    as defender_y,
    cast(null as bigint)    as action_player_id,
    cast(null as string)    as action_type,
    cast(null as double)    as action_x,
    cast(null as double)    as action_y,
    cast(null as string)    as credit_type,
    cast(null as string)    as confidence,
    cast(null as double)    as defcon_value,
    cast(null as double)    as dist_to_ball,
    cast(null as double)    as pitch_control_at_action,
    cast(null as string)    as data_source,
    cast(null as bigint)    as match_key,
    cast(null as bigint)    as team_key,
    cast(null as bigint)    as player_key,
    cast(null as bigint)    as action_player_key,
    current_timestamp()     as _loaded_at
where 1 = 0

{% endif %}
