-- fct_match_summary.sql
-- Match-level aggregate metrics for dashboards and analysis.
--
-- Combines data from shots, passes, and match metadata to produce
-- a single row per match with team-level summary statistics.
--
-- Key metrics per team:
--   - xG (total and by half)
--   - Possession percentage (derived from event counts)
--   - Pass completion percentage
--   - Shot counts and shot accuracy
--   - Progressive pass counts
--
-- Downstream consumers:
--   - Match result dashboards
--   - xG timeline charts
--   - Season summary tables
--   - Team performance comparison

with matches as (

    select * from {{ ref('stg_statsbomb__matches') }}

),

shots as (

    select * from {{ ref('fct_shots') }}

),

passes as (

    select * from {{ ref('fct_passes') }}

),

-- Aggregate shots per team per match
match_shots as (

    select
        match_id,
        team_id,
        count(*)                                        as total_shots,
        sum(is_goal)                                    as total_goals,
        sum(coalesce(statsbomb_xg, 0))                  as total_xg,
        sum(case when shot_outcome in ('Goal', 'Saved') then 1 else 0 end) as shots_on_target

    from shots
    group by match_id, team_id

),

-- Aggregate passes per team per match
match_passes as (

    select
        match_id,
        team_id,
        count(*)                                        as total_passes,
        sum(case when is_complete then 1 else 0 end)    as completed_passes,
        sum(case when is_progressive then 1 else 0 end) as progressive_passes

    from passes
    group by match_id, team_id

),

-- Pivot to home/away perspective
final as (

    select
        m.match_id,
        m.competition_id,
        m.season_id,
        m.match_date,
        m.home_team_id,
        m.away_team_id,
        m.home_score,
        m.away_score,

        -- Home team shot metrics
        coalesce(hs.total_shots, 0)                     as home_shots,
        coalesce(hs.total_goals, 0)                     as home_goals,
        coalesce(hs.total_xg, 0)                        as home_xg,
        coalesce(hs.shots_on_target, 0)                 as home_shots_on_target,

        -- Away team shot metrics
        coalesce(aws.total_shots, 0)                    as away_shots,
        coalesce(aws.total_goals, 0)                    as away_goals,
        coalesce(aws.total_xg, 0)                       as away_xg,
        coalesce(aws.shots_on_target, 0)                as away_shots_on_target,

        -- Home team pass metrics
        coalesce(hp.total_passes, 0)                    as home_total_passes,
        coalesce(hp.completed_passes, 0)                as home_completed_passes,
        coalesce(hp.progressive_passes, 0)              as home_progressive_passes,

        -- Away team pass metrics
        coalesce(ap.total_passes, 0)                    as away_total_passes,
        coalesce(ap.completed_passes, 0)                as away_completed_passes,
        coalesce(ap.progressive_passes, 0)              as away_progressive_passes,

        -- Derived: pass completion percentages
        case
            when coalesce(hp.total_passes, 0) > 0
            then round(hp.completed_passes * 100.0 / hp.total_passes, 1)
            else 0
        end                                             as home_pass_completion_pct,
        case
            when coalesce(ap.total_passes, 0) > 0
            then round(ap.completed_passes * 100.0 / ap.total_passes, 1)
            else 0
        end                                             as away_pass_completion_pct,

        -- Derived: possession estimate (based on pass share)
        -- This is a rough proxy; true possession requires tracking data
        case
            when coalesce(hp.total_passes, 0) + coalesce(ap.total_passes, 0) > 0
            then round(
                hp.total_passes * 100.0 / (hp.total_passes + ap.total_passes), 1
            )
            else 50.0
        end                                             as home_possession_pct,

        -- xG difference (positive = home advantage)
        coalesce(hs.total_xg, 0) - coalesce(aws.total_xg, 0) as xg_difference,

        -- Match result
        case
            when m.home_score > m.away_score then 'home_win'
            when m.home_score < m.away_score then 'away_win'
            else 'draw'
        end                                             as match_result

    from matches m
    left join match_shots hs on m.match_id = hs.match_id and m.home_team_id = hs.team_id
    left join match_shots aws on m.match_id = aws.match_id and m.away_team_id = aws.team_id
    left join match_passes hp on m.match_id = hp.match_id and m.home_team_id = hp.team_id
    left join match_passes ap on m.match_id = ap.match_id and m.away_team_id = ap.team_id

)

select * from final
