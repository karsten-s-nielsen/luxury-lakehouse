{{ config(
    materialized='incremental',
    unique_key='action_value_id',
    liquid_clustered_by=['match_id'],
    incremental_strategy='merge'
) }}
-- fct_action_values.sql
-- Gold-layer SPADL action values with VAEP scores, possession context,
-- and per-action game state.
--
-- Contains every on-ball action from all data sources converted to the
-- SPADL unified format, scored with offensive, defensive, and net VAEP
-- values. Enables player ranking by total contribution beyond goals/assists.
--
-- Coordinate system: 105x68 meters (SPADL academic standard).
-- One row per action.

with action_values as (

    select * from {{ ref('stg_spadl__action_values') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}

),

sb_events as (

    select
        event_id,
        possession,
        possession_team_id
    from {{ ref('stg_statsbomb__events') }}

),

running_score as (

    select * from {{ ref('int_running_score') }}

),

-- Join each action to its most recent score milestone.
-- The kickoff row (period=1, minute=0, second=0) ensures every action
-- in period >= 1 has at least one matching score row.
actions_with_score as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'av.match_id',
            'av.period',
            'av.time_seconds',
            'av.player_id',
            'av.type_id',
            'av.data_source'
        ]) }}                                       as action_value_id,

        av.match_id,
        av.player_id,
        av.team_id,
        av.competition_id,
        av.season_id,
        av.period,
        av.time_seconds,
        av.minute,
        av.second,

        -- SPADL coordinates (105x68 meters)
        av.start_x,
        av.start_y,
        av.end_x,
        av.end_y,

        -- Action classification
        av.action_type,
        av.action_result,
        av.bodypart,

        -- VAEP scores
        av.offensive_value,
        av.defensive_value,
        av.vaep_value,

        -- Possession context (StatsBomb only; NULL for Wyscout)
        sbe.possession                              as possession_id,
        sbe.possession_team_id,

        -- Running score for game state derivation
        rs.home_score_after,
        rs.away_score_after,
        rs.home_team_id                             as _rs_home_team_id,

        -- Rank to pick the most recent score milestone
        row_number() over (
            partition by
                av.match_id, av.period, av.time_seconds,
                av.player_id, av.type_id, av.data_source
            order by rs.period desc, rs.minute desc, rs.second desc
        )                                           as _score_rn,

        -- Provenance
        av.data_source,
        av.original_event_id

    from action_values av
    left join sb_events sbe
        on av.original_event_id = sbe.event_id
        and av.data_source = 'statsbomb'
    left join running_score rs
        on rs.match_id = av.match_id
        and (
            rs.period < av.period
            or (rs.period = av.period
                and (rs.minute * 60 + rs.second) <= (av.minute * 60 + av.second))
        )

),

final as (

    select
        action_value_id,
        match_id,
        player_id,
        team_id,
        competition_id,
        season_id,
        period,
        time_seconds,
        minute,
        second,
        start_x,
        start_y,
        end_x,
        end_y,
        action_type,
        action_result,
        bodypart,
        offensive_value,
        defensive_value,
        vaep_value,
        possession_id,
        possession_team_id,
        case
            when coalesce(home_score_after, 0) = coalesce(away_score_after, 0)
                then 'drawing'
            when (team_id = _rs_home_team_id
                      and home_score_after > away_score_after)
                 or (team_id != _rs_home_team_id
                      and away_score_after > home_score_after)
                then 'winning'
            else 'losing'
        end                                         as game_state,
        data_source,
        original_event_id,
        current_timestamp()                         as _loaded_at

    from actions_with_score
    where _score_rn = 1

)

select * from final
