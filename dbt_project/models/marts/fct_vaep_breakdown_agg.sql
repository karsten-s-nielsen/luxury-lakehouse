{{ config(
    materialized='table',
    liquid_clustered_by=['competition_id']
) }}
-- fct_vaep_breakdown_agg.sql
-- Pre-aggregated VAEP breakdown by action_type for Taipy Action Values
-- and Defensive Impact pages.
--
-- Motivation (2026-04-16 optimization audit):
-- fetch_vaep_breakdown(comp_id, team_id=None, player_id=None) aggregates
-- fct_action_values (9.53M rows) by action_type with sum(vaep_value),
-- sum(offensive_value), sum(defensive_value), count(*).  Measured:
--   comp-only:            2,800 ms  (Parallel Seq Scan)
--   comp+team+player:        16 ms  (indexed)
-- Pre-aggregating to (competition_id, team_id, player_id, action_type)
-- lets us serve comp-only, comp+team, and comp+team+player by SUM
-- rollups on a small table.
--
-- Grain: (competition_id, team_id, player_id, action_type)
-- Row estimate:  ~21 comps * ~25 teams * ~25 players * ~10 action types
--                * sparsity ~50% = ~65K rows.

with action_values as (

    select * from {{ ref('fct_action_values') }}
    where competition_id is not null
      and team_id is not null
      and player_id is not null
      and action_type is not null

),

aggregated as (

    select
        competition_id,
        team_id,
        player_id,
        action_type,
        sum(vaep_value)                               as total_vaep,
        sum(offensive_value)                          as total_offensive,
        sum(defensive_value)                          as total_defensive,
        count(*)                                      as action_count
    from action_values
    group by competition_id, team_id, player_id, action_type

),

final as (

    select
        cast(competition_id as int)                   as competition_id,
        cast(team_id as int)                          as team_id,
        cast(player_id as int)                        as player_id,
        cast(action_type as string)                   as action_type,
        cast(total_vaep as double)                    as total_vaep,
        cast(total_offensive as double)               as total_offensive,
        cast(total_defensive as double)               as total_defensive,
        cast(action_count as bigint)                  as action_count,
        current_timestamp()                           as _loaded_at

    from aggregated

)

select * from final
