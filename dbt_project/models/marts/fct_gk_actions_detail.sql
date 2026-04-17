{{ config(
    materialized='table',
    liquid_clustered_by=['match_id']
) }}
-- fct_gk_actions_detail.sql
-- Pre-filtered goalkeeper pass and goalkick actions for the Taipy Goalkeeper
-- Analytics "Distribution" sub-view.
--
-- Motivation (2026-04-16 optimization audit):
-- fetch_gk_passes(comp, player=None, team=None, match=None) scans the 9.53M
-- row fct_action_values_synced with filters:
--   action_type IN ('goalkick', 'pass')
--   AND dim_players.position_group = 'Goalkeeper'
--   AND competition_id = %s
-- then joins to dim_players. Measured 13,247 ms (Parallel Seq Scan) for
-- comp-only.  The position_group filter only eliminates ~95% of rows and
-- the planner prefers Seq Scan at that selectivity.  Pre-filtering at dbt
-- time to GK + pass/goalkick produces a narrow mart where comp-only queries
-- become an index scan.
--
-- Grain: one row per (match, player, action) where player is a Goalkeeper
--        and action is a pass or goalkick.
-- Row estimate: ~4,000 matches * ~2 GKs/match * ~40 GK distributions/match
--               = ~320K rows.
--
-- This is a pre-filtered narrow projection — NOT an aggregation.  The Taipy
-- page renders individual passes as arrows on a pitch, so per-action detail
-- must be preserved.

with gk_players as (

    select distinct player_id
    from {{ ref('dim_players') }}
    where position_group = 'Goalkeeper'

),

gk_actions as (

    select
        av.action_value_id,
        av.match_id,
        av.competition_id,
        av.season_id,
        av.team_id,
        av.player_id,
        av.period,
        av.time_seconds,
        av.minute,
        av.second,
        av.start_x,
        av.start_y,
        av.end_x,
        av.end_y,
        av.action_type,
        av.action_result,
        av.data_source
    from {{ ref('fct_action_values') }} av
    inner join gk_players gk on av.player_id = gk.player_id
    where av.action_type in ('goalkick', 'pass')

),

final as (

    select
        cast(action_value_id as string)               as gk_action_id,
        cast(match_id as bigint)                      as match_id,
        cast(competition_id as int)                   as competition_id,
        cast(season_id as int)                        as season_id,
        cast(team_id as int)                          as team_id,
        cast(player_id as int)                        as player_id,
        cast(period as int)                           as period,
        cast(time_seconds as double)                  as time_seconds,
        cast(minute as int)                           as minute,
        cast(second as int)                           as second,
        cast(start_x as double)                       as start_x,
        cast(start_y as double)                       as start_y,
        cast(end_x as double)                         as end_x,
        cast(end_y as double)                         as end_y,
        cast(action_type as string)                   as action_type,
        cast(action_result as string)                 as action_result,
        cast(data_source as string)                   as data_source,
        current_timestamp()                           as _loaded_at

    from gk_actions

)

select * from final
