-- fct_player_stats.sql
-- Per-90 minute player aggregation table for player comparison.
--
-- All counting stats are normalized to per-90-minute rates to enable
-- fair comparison across players with different total minutes played.
-- Formula: stat_per_90 = (raw_count / minutes_played) * 90
--
-- Aggregation grain: one row per player per competition per season.

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
        sum(case when is_switch then 1 else 0 end)      as total_switches

    from passes
    group by player_id, competition_id, season_id

),

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
            then round((coalesce(s.total_goals, 0) * 1.0 / m.total_minutes_played) * 90, 2)
        end                                             as goals_per_90,
        cast(null as double)                            as assists_per_90,
        case
            when m.total_minutes_played > 0
            then round((coalesce(s.total_xg, 0) / m.total_minutes_played) * 90, 2)
        end                                             as xg_per_90,
        case
            when m.total_minutes_played > 0
            then round((coalesce(p.total_passes, 0) * 1.0 / m.total_minutes_played) * 90, 2)
        end                                             as passes_per_90,
        case
            when m.total_minutes_played > 0
            then round((coalesce(p.progressive_passes, 0) * 1.0 / m.total_minutes_played) * 90, 2)
        end                                             as progressive_passes_per_90,

        -- xG overperformance (goals - xG, positive = clinical finisher)
        coalesce(s.total_goals, 0) - coalesce(s.total_xg, 0) as xg_overperformance

    from player_shots_agg s
    full outer join player_passes_agg p
        on s.player_id = p.player_id
        and coalesce(s.competition_id, -1) = coalesce(p.competition_id, -1)
        and coalesce(s.season_id, -1) = coalesce(p.season_id, -1)
    left join minutes m
        on coalesce(s.player_id, p.player_id) = m.player_id
        and coalesce(s.competition_id, p.competition_id) = m.competition_id
        and coalesce(s.season_id, p.season_id) = m.season_id

)

select * from final
