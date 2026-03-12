{{ config(
    materialized='incremental',
    unique_key='defcon_action_id',
    liquid_clustered_by=['match_id'],
    incremental_strategy='merge'
) }}
-- fct_defcon_actions.sql
-- Per-defender per-action defensive credits for timeline visualization.
--
-- Contains the full granularity of DEFCON-lite results: one row per
-- defender per credited action. Powers the Match Timeline view in
-- the Streamlit Defensive Valuation page.
--
-- Coordinate system: SPADL 105x68 meters.
-- One row per defender per action.

{% if var('defcon_enabled', false) %}

with defcon as (

    select * from {{ ref('stg_defcon__results') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'event_id',
            'defender_player_id',
            'data_source'
        ]) }}                                                       as defcon_action_id,

        event_id,
        match_id,
        competition_id,
        season_id,
        defender_player_id                                          as player_id,
        defender_team_id                                            as team_id,
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
        data_source,
        current_timestamp()                                         as _loaded_at

    from defcon

)

select * from final

{% else %}

select
    cast(null as string)    as defcon_action_id,
    cast(null as string)    as event_id,
    cast(null as string)    as match_id,
    cast(null as int)       as competition_id,
    cast(null as int)       as season_id,
    cast(null as int)       as player_id,
    cast(null as int)       as team_id,
    cast(null as double)    as defender_x,
    cast(null as double)    as defender_y,
    cast(null as int)       as action_player_id,
    cast(null as string)    as action_type,
    cast(null as double)    as action_x,
    cast(null as double)    as action_y,
    cast(null as string)    as credit_type,
    cast(null as string)    as confidence,
    cast(null as double)    as defcon_value,
    cast(null as double)    as dist_to_ball,
    cast(null as double)    as pitch_control_at_action,
    cast(null as string)    as data_source,
    current_timestamp()     as _loaded_at
where 1 = 0

{% endif %}
