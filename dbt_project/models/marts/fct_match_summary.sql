{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='match_id',
    on_schema_change='fail',
    liquid_clustered_by=['match_id']
) }}

-- fct_match_summary.sql
-- Match-level aggregate metrics for dashboards and analysis.
--
-- Combines data from shots, passes, and match metadata to produce
-- a single row per match with team-level summary statistics.
--
-- The matches table has team names but no team IDs (statsbombpy flattening).
-- We derive the team_name → team_id mapping from events per match.
--
-- PPDA (Passes Per Defensive Action): opponent passes allowed in the defending
-- team's defensive 40% of pitch, divided by team defensive actions in that zone.
-- StatsBomb coordinates are from the acting team's perspective (x=0 own goal).

with

{% if is_incremental() %}
existing_matches as (
    select distinct match_id from {{ this }}
),
{% endif %}

matches as (

    select * from {{ ref('stg_statsbomb__matches') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}

),

-- Derive team_id from events for each match's home/away team
match_team_ids as (

    select distinct
        match_id,
        team_id,
        team_name
    from {{ ref('stg_statsbomb__events') }}
    where team_id is not null
    {% if is_incremental() %}
      and match_id not in (select match_id from existing_matches)
    {% endif %}

),

shots as (

    select * from {{ ref('fct_shots') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}

),

passes as (

    select * from {{ ref('fct_passes') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}

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

-- PPDA: defensive actions per team pressing HIGH in the opponent's 60% of pitch.
-- StatsBomb coords: x=0 own goal, x=120 opponent goal.
-- Pressing zone = x > pitch_length * 0.4 (opponent's 60%, where team presses)
defensive_actions as (

    select
        match_id,
        team_id,
        count(*) as def_action_count
    from {{ ref('stg_statsbomb__events') }}
    where event_type in ('Duel', 'Interception', 'Foul Committed', 'Block')
      and location_x > {{ var('pitch_length') }} * 0.4
    {% if is_incremental() %}
      and match_id not in (select match_id from existing_matches)
    {% endif %}
    group by match_id, team_id

),

-- Opponent passes under pressure: passes made in the passer's own 60%.
-- These are passes the pressing team "allowed" in the pressing zone.
-- The 60% threshold matches the defensive action zone (other team's x > 40%).
opponent_passes_in_def_zone as (

    select
        match_id,
        team_id,
        count(*) as opp_pass_count
    from {{ ref('stg_statsbomb__events') }}
    where event_type = 'Pass'
      and location_x < {{ var('pitch_length') }} * 0.6
    {% if is_incremental() %}
      and match_id not in (select match_id from existing_matches)
    {% endif %}
    group by match_id, team_id

),

-- Pivot to home/away perspective
final as (

    select
        m.match_id,
        m.competition_id,
        m.season_id,
        m.match_date,
        m.home_team_name,
        m.away_team_name,
        htm.team_id                                     as home_team_id,
        atm.team_id                                     as away_team_id,
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
        case
            when coalesce(hp.total_passes, 0) + coalesce(ap.total_passes, 0) > 0
            then round(
                hp.total_passes * 100.0 / (hp.total_passes + ap.total_passes), 1
            )
            else 50.0
        end                                             as home_possession_pct,

        -- xG difference (positive = home advantage)
        coalesce(hs.total_xg, 0) - coalesce(aws.total_xg, 0) as xg_difference,

        -- PPDA: Passes Per Defensive Action
        -- Home PPDA = away passes in home zone / home defensive actions
        case
            when coalesce(hda.def_action_count, 0) > 0
            then round(
                coalesce(aop.opp_pass_count, 0) * 1.0 / hda.def_action_count, 2
            )
            else null
        end                                             as home_ppda,
        -- Away PPDA = home passes in away zone / away defensive actions
        case
            when coalesce(ada.def_action_count, 0) > 0
            then round(
                coalesce(hop.opp_pass_count, 0) * 1.0 / ada.def_action_count, 2
            )
            else null
        end                                             as away_ppda,

        -- Match result
        case
            when m.home_score > m.away_score then 'home_win'
            when m.home_score < m.away_score then 'away_win'
            else 'draw'
        end                                             as match_result

    from matches m
    -- Map team names to IDs via events
    left join match_team_ids htm
        on m.match_id = htm.match_id and m.home_team_name = htm.team_name
    left join match_team_ids atm
        on m.match_id = atm.match_id and m.away_team_name = atm.team_name
    -- Join shot/pass aggregates on resolved team_id
    left join match_shots hs on m.match_id = hs.match_id and htm.team_id = hs.team_id
    left join match_shots aws on m.match_id = aws.match_id and atm.team_id = aws.team_id
    left join match_passes hp on m.match_id = hp.match_id and htm.team_id = hp.team_id
    left join match_passes ap on m.match_id = ap.match_id and atm.team_id = ap.team_id
    -- PPDA joins: home def actions + away passes in home zone (and vice versa)
    left join defensive_actions hda on m.match_id = hda.match_id and htm.team_id = hda.team_id
    left join defensive_actions ada on m.match_id = ada.match_id and atm.team_id = ada.team_id
    left join opponent_passes_in_def_zone aop on m.match_id = aop.match_id and atm.team_id = aop.team_id
    left join opponent_passes_in_def_zone hop on m.match_id = hop.match_id and htm.team_id = hop.team_id

)

select * from final
