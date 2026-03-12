{{ config(
    materialized='incremental',
    unique_key='player_stats_id',
    liquid_clustered_by=['competition_id', 'season_id'],
    incremental_strategy='merge'
) }}
-- fct_player_stats.sql
-- Per-90 minute player aggregation table for player comparison.
--
-- All counting stats are normalized to per-90-minute rates to enable
-- fair comparison across players with different total minutes played.
-- Formula: stat_per_90 = (raw_count / minutes_played) * 90
--
-- Aggregation grain: one row per player per competition per season.
-- Incremental: merge strategy upserts on player_stats_id surrogate key.
-- Since this is a cross-match aggregation, source CTEs are not filtered —
-- the merge handles deduplication and updates changed rows.

with shots as (

    select * from {{ ref('fct_shots') }}

),

passes as (

    select * from {{ ref('fct_passes') }}

),

minutes as (

    select * from {{ ref('int_minutes_played') }}

),

player_shots_agg as (

    select
        player_id,
        competition_id,
        season_id,

        -- Shooting aggregates
        count(*)                                        as total_shots,
        sum(is_goal)                                    as total_goals,
        sum(case when shot_outcome in ('Goal', 'Saved') then 1 else 0 end) as shots_on_target,
        sum(coalesce(statsbomb_xg, 0))                  as total_xg,
        avg(distance_to_goal)                           as avg_shot_distance,
        avg(shot_angle)                                 as avg_shot_angle

    from shots
    group by player_id, competition_id, season_id

),

player_passes_agg as (

    select
        player_id,
        competition_id,
        season_id,

        -- Passing aggregates
        count(*)                                        as total_passes,
        sum(case when is_complete then 1 else 0 end)    as completed_passes,
        sum(case when is_progressive then 1 else 0 end) as progressive_passes,
        sum(case when is_cross then 1 else 0 end)       as total_crosses,
        sum(case when is_through_ball then 1 else 0 end) as total_through_balls,
        sum(case when is_switch then 1 else 0 end)      as total_switches,
        sum(case when is_line_breaking then 1 else 0 end) as line_breaking_passes

    from passes
    group by player_id, competition_id, season_id

),

action_values as (

    select * from {{ ref('fct_action_values') }}

),

vaep_agg as (

    select
        player_id,
        competition_id,
        season_id,
        sum(offensive_value)                            as total_offensive_vaep,
        sum(defensive_value)                            as total_defensive_vaep,
        sum(vaep_value)                                 as total_vaep,
        count(*)                                        as total_actions

    from action_values
    group by player_id, competition_id, season_id

),

{% if var('defcon_enabled', false) %}
defcon_agg as (

    select
        player_id,
        competition_id,
        season_id,
        sum(total_defcon_value)                             as total_defcon,
        sum(intercept_value)                                as total_intercept,
        sum(deter_value)                                    as total_deter,
        sum(total_credits)                                  as defcon_credits

    from {{ ref('fct_defensive_values') }}
    group by player_id, competition_id, season_id

),
{% endif %}

final as (

    select
        -- Surrogate key (use -1 sentinel for NULL comp/season from Wyscout data)
        {{ dbt_utils.generate_surrogate_key([
            'coalesce(s.player_id, p.player_id)',
            'coalesce(coalesce(s.competition_id, p.competition_id), -1)',
            'coalesce(coalesce(s.season_id, p.season_id), -1)'
        ]) }} as player_stats_id,

        coalesce(s.player_id, p.player_id)              as player_id,
        coalesce(s.competition_id, p.competition_id)    as competition_id,
        coalesce(s.season_id, p.season_id)              as season_id,

        -- Minutes played from int_minutes_played
        m.total_minutes_played                          as minutes_played,

        -- Raw shooting stats
        coalesce(s.total_shots, 0)                      as total_shots,
        coalesce(s.total_goals, 0)                      as total_goals,
        coalesce(s.shots_on_target, 0)                  as shots_on_target,
        coalesce(s.total_xg, 0)                         as total_xg,

        -- Raw passing stats
        coalesce(p.total_passes, 0)                     as total_passes,
        coalesce(p.completed_passes, 0)                 as completed_passes,
        coalesce(p.progressive_passes, 0)               as progressive_passes,

        -- Pass completion percentage
        case
            when coalesce(p.total_passes, 0) > 0
            then round(p.completed_passes * 100.0 / p.total_passes, 1)
            else 0
        end                                             as pass_completion_pct,

        -- Per-90 rates (NULL when minutes_played is not available)
        case
            when m.total_minutes_played > 0
            then round((coalesce(s.total_goals, 0) * 1.0 / m.total_minutes_played) * {{ var('minutes_per_match') }}, 2)
        end                                             as goals_per_90,
        cast(null as double)                            as assists_per_90,
        case
            when m.total_minutes_played > 0
            then round((coalesce(s.total_xg, 0) / m.total_minutes_played) * {{ var('minutes_per_match') }}, 2)
        end                                             as xg_per_90,
        case
            when m.total_minutes_played > 0
            then round((coalesce(p.total_passes, 0) * 1.0 / m.total_minutes_played) * {{ var('minutes_per_match') }}, 2)
        end                                             as passes_per_90,
        case
            when m.total_minutes_played > 0
            then round((coalesce(p.progressive_passes, 0) * 1.0 / m.total_minutes_played) * {{ var('minutes_per_match') }}, 2)
        end                                             as progressive_passes_per_90,

        -- xG overperformance (goals - xG, positive = clinical finisher)
        coalesce(s.total_goals, 0) - coalesce(s.total_xg, 0) as xg_overperformance,

        -- Line-breaking passes
        coalesce(p.line_breaking_passes, 0)              as line_breaking_passes,
        case
            when m.total_minutes_played > 0
            then round((coalesce(p.line_breaking_passes, 0) * 1.0 / m.total_minutes_played) * {{ var('minutes_per_match') }}, 2)
        end                                             as line_breaking_per_90,

        -- VAEP action valuation
        coalesce(v.total_vaep, 0)                       as total_vaep,
        coalesce(v.total_offensive_vaep, 0)              as total_offensive_vaep,
        coalesce(v.total_defensive_vaep, 0)              as total_defensive_vaep,
        coalesce(v.total_actions, 0)                     as total_actions,
        case
            when m.total_minutes_played > 0
            then round((coalesce(v.total_vaep, 0) / m.total_minutes_played) * {{ var('minutes_per_match') }}, 3)
        end                                             as vaep_per_90,
        case
            when m.total_minutes_played > 0
            then round((coalesce(v.total_offensive_vaep, 0) / m.total_minutes_played) * {{ var('minutes_per_match') }}, 3)
        end                                             as offensive_vaep_per_90,
        case
            when m.total_minutes_played > 0
            then round((coalesce(v.total_defensive_vaep, 0) / m.total_minutes_played) * {{ var('minutes_per_match') }}, 3)
        end                                             as defensive_vaep_per_90

        {% if var('defcon_enabled', false) %}
        ,
        coalesce(d.total_defcon, 0)                         as total_defcon,
        coalesce(d.defcon_credits, 0)                       as defcon_credits,
        case
            when m.total_minutes_played > 0
            then round((coalesce(d.total_defcon, 0) / m.total_minutes_played) * {{ var('minutes_per_match') }}, 3)
        end                                                 as defcon_per_90,
        case
            when m.total_minutes_played > 0
            then round((coalesce(d.total_intercept, 0) / m.total_minutes_played) * {{ var('minutes_per_match') }}, 3)
        end                                                 as intercept_per_90,
        case
            when m.total_minutes_played > 0
            then round((coalesce(d.total_deter, 0) / m.total_minutes_played) * {{ var('minutes_per_match') }}, 3)
        end                                                 as deter_per_90
        {% else %}
        ,
        cast(null as double)                                as total_defcon,
        cast(null as int)                                   as defcon_credits,
        cast(null as double)                                as defcon_per_90,
        cast(null as double)                                as intercept_per_90,
        cast(null as double)                                as deter_per_90
        {% endif %}

    from player_shots_agg s
    full outer join player_passes_agg p
        on s.player_id = p.player_id
        and coalesce(s.competition_id, -1) = coalesce(p.competition_id, -1)
        and coalesce(s.season_id, -1) = coalesce(p.season_id, -1)
    left join minutes m
        on coalesce(s.player_id, p.player_id) = m.player_id
        and coalesce(s.competition_id, p.competition_id) = m.competition_id
        and coalesce(s.season_id, p.season_id) = m.season_id
    left join vaep_agg v
        on coalesce(s.player_id, p.player_id) = v.player_id
        and coalesce(s.competition_id, p.competition_id) = v.competition_id
        and coalesce(s.season_id, p.season_id) = v.season_id
    {% if var('defcon_enabled', false) %}
    left join defcon_agg d
        on coalesce(s.player_id, p.player_id) = d.player_id
        and coalesce(s.competition_id, p.competition_id) = d.competition_id
        and coalesce(s.season_id, p.season_id) = d.season_id
    {% endif %}

)

select * from final
