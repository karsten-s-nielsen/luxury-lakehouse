{{ config(
    materialized='incremental',
    unique_key='player_stats_id',
    liquid_clustered_by=['competition_id', 'season_id'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}
-- fct_player_stats.sql
-- Per-90 minute player aggregation table for player comparison.
--
-- All counting stats are normalized to per-90-minute rates to enable
-- fair comparison across players with different total minutes played.
-- Formula: stat_per_90 = (raw_count / minutes_played) * 90
--
-- Aggregation grain: one row per (player_id, competition_id, season_id, data_source).
-- PR 5a (ADR-011): added data_source to the grain so dim_players can resolve
-- player_key uniquely by (provider, native_player_id) without cross-provider
-- integer collisions. Incremental: merge strategy upserts on player_stats_id.
--
-- Kimball keys (PR 5a):
--   - player_key BIGINT FK → dim_players.player_key. Populated via INNER JOIN
--     on (provider=data_source, native_player_id=cast(player_id as string)).
--     The INNER JOIN drops the 1 NULL player_id outlier previously warn-suppressed.
--   - team_key BIGINT FK → dim_teams.team_key. Nullable at this grain: career
--     aggregates span teams, so team_key is left NULL and resolved at the
--     display layer or via a future team-aware aggregate.

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
        data_source,

        -- Shooting aggregates
        count(*)                                        as total_shots,
        sum(is_goal)                                    as total_goals,
        sum(case when shot_outcome in ('Goal', 'Saved') then 1 else 0 end) as shots_on_target,
        sum(coalesce(statsbomb_xg, 0))                  as total_xg,
        avg(distance_to_goal)                           as avg_shot_distance,
        avg(shot_angle)                                 as avg_shot_angle

    from shots
    group by player_id, competition_id, season_id, data_source

),

player_passes_agg as (

    select
        player_id,
        competition_id,
        season_id,
        data_source,

        -- Passing aggregates
        count(*)                                        as total_passes,
        sum(case when is_complete then 1 else 0 end)    as completed_passes,
        sum(case when is_progressive then 1 else 0 end) as progressive_passes,
        sum(case when is_cross then 1 else 0 end)       as total_crosses,
        sum(case when is_through_ball then 1 else 0 end) as total_through_balls,
        sum(case when is_switch then 1 else 0 end)      as total_switches,
        sum(case when is_line_breaking then 1 else 0 end) as line_breaking_passes

    from passes
    group by player_id, competition_id, season_id, data_source

),

action_values as (

    select * from {{ ref('fct_action_values') }}

),

vaep_agg as (

    select
        player_id,
        competition_id,
        season_id,
        data_source,
        sum(offensive_value)                            as total_offensive_vaep,
        sum(defensive_value)                            as total_defensive_vaep,
        sum(vaep_value)                                 as total_vaep,
        count(*)                                        as total_actions

    from action_values
    group by player_id, competition_id, season_id, data_source

),

{% if var('defcon_enabled', false) %}
defcon_agg as (

    select
        player_id,
        competition_id,
        season_id,
        data_source,
        sum(total_defcon_value)                             as total_defcon,
        sum(intercept_value)                                as total_intercept,
        sum(deter_value)                                    as total_deter,
        sum(total_credits)                                  as defcon_credits

    from {{ ref('fct_defensive_values') }}
    group by player_id, competition_id, season_id, data_source

),
{% endif %}

base as (

    select
        -- Grain: (player_id, competition_id, season_id, data_source)
        coalesce(s.player_id, p.player_id)              as player_id,
        coalesce(s.competition_id, p.competition_id)    as competition_id,
        coalesce(s.season_id, p.season_id)              as season_id,
        coalesce(s.data_source, p.data_source)          as data_source,

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
        and coalesce(s.data_source, '') = coalesce(p.data_source, '')
    left join minutes m
        on coalesce(s.player_id, p.player_id) = m.player_id
        and coalesce(s.competition_id, p.competition_id) = m.competition_id
        and coalesce(s.season_id, p.season_id) = m.season_id
    left join vaep_agg v
        on coalesce(s.player_id, p.player_id) = v.player_id
        and coalesce(s.competition_id, p.competition_id) = v.competition_id
        and coalesce(s.season_id, p.season_id) = v.season_id
        and coalesce(s.data_source, p.data_source) = v.data_source
    {% if var('defcon_enabled', false) %}
    left join defcon_agg d
        on coalesce(s.player_id, p.player_id) = d.player_id
        and coalesce(s.competition_id, p.competition_id) = d.competition_id
        and coalesce(s.season_id, p.season_id) = d.season_id
        and coalesce(s.data_source, p.data_source) = d.data_source
    {% endif %}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'b.player_id',
            'coalesce(b.competition_id, -1)',
            'coalesce(b.season_id, -1)',
            "coalesce(b.data_source, '')"
        ]) }} as player_stats_id,

        -- PR 5a (ADR-011) Kimball surrogate keys.
        dp.player_key,
        cast(null as bigint)                            as team_key,

        b.player_id,
        b.competition_id,
        b.season_id,
        b.data_source,
        b.minutes_played,
        b.total_shots,
        b.total_goals,
        b.shots_on_target,
        b.total_xg,
        b.total_passes,
        b.completed_passes,
        b.progressive_passes,
        b.pass_completion_pct,
        b.goals_per_90,
        b.assists_per_90,
        b.xg_per_90,
        b.passes_per_90,
        b.progressive_passes_per_90,
        b.xg_overperformance,
        b.line_breaking_passes,
        b.line_breaking_per_90,
        b.total_vaep,
        b.total_offensive_vaep,
        b.total_defensive_vaep,
        b.total_actions,
        b.vaep_per_90,
        b.offensive_vaep_per_90,
        b.defensive_vaep_per_90,
        b.total_defcon,
        b.defcon_credits,
        b.defcon_per_90,
        b.intercept_per_90,
        b.deter_per_90

    from base b
    -- INNER JOIN drops NULL player_id outliers (previously warn-suppressed).
    inner join {{ ref('dim_players') }} dp
        on dp.provider = b.data_source
       and dp.native_player_id = cast(b.player_id as string)

)

select * from final
