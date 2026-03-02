-- fct_action_values.sql
-- Gold-layer SPADL action values with VAEP scores.
--
-- Contains every on-ball action from all data sources converted to the
-- SPADL unified format, scored with offensive, defensive, and net VAEP
-- values. Enables player ranking by total contribution beyond goals/assists.
--
-- Coordinate system: 105x68 meters (SPADL academic standard).
-- One row per action.

with action_values as (

    select * from {{ ref('stg_spadl__action_values') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'action_values.match_id',
            'action_values.period',
            'action_values.time_seconds',
            'action_values.player_id',
            'action_values.type_id',
            'action_values.data_source'
        ]) }}                                       as action_value_id,

        action_values.match_id,
        action_values.player_id,
        action_values.team_id,
        action_values.competition_id,
        action_values.season_id,
        action_values.period,
        action_values.time_seconds,
        action_values.minute,
        action_values.second,

        -- SPADL coordinates (105x68 meters)
        action_values.start_x,
        action_values.start_y,
        action_values.end_x,
        action_values.end_y,

        -- Action classification
        action_values.action_type,
        action_values.action_result,
        action_values.bodypart,

        -- VAEP scores
        action_values.offensive_value,
        action_values.defensive_value,
        action_values.vaep_value,

        -- Provenance
        action_values.data_source,
        action_values.original_event_id,
        current_timestamp()                         as _loaded_at

    from action_values

)

select * from final
