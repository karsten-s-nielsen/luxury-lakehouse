{{ config(materialized='ephemeral') }}
-- int_running_score.sql
-- Running scoreline per match — one kickoff row (0-0) plus one row per goal
-- with cumulative home/away scores. Ephemeral: inlined as CTE into consumers.
--
-- Used by fct_action_values, fct_shots, fct_passes to derive per-action
-- game_state (winning/losing/drawing from the acting team's perspective).
--
-- PR 2 (ADR-011) adds the `match_key` column (BIGINT surrogate FK to
-- dim_matches). Native `match_id` is retained for consumers still on the
-- native ID (fct_shots, fct_action_values) until their own migration PRs.
--
-- Known limitation: own goals are not tracked. An own goal does not appear
-- in int_unified_shots with shot_outcome='Goal', so the running score may
-- be inaccurate in matches with own goals (~3-5% of all goals).
--
-- Team ID resolution: StatsBomb matches have team names but no IDs; we
-- resolve via a team_name→team_id lookup from events. Wyscout matches
-- lack team names entirely; we assign the lower team_id as "home"
-- (arbitrary but deterministic).

with sb_match_team_ids as (

    select distinct
        match_id,
        team_id,
        team_name
    from {{ ref('stg_statsbomb__events') }}
    where team_id is not null

),

sb_matches as (

    select
        m.match_id,
        'statsbomb'    as provider,
        htm.team_id    as home_team_id,
        atm.team_id    as away_team_id
    from {{ ref('stg_statsbomb__matches') }} m
    left join sb_match_team_ids htm
        on m.match_id = htm.match_id and m.home_team_name = htm.team_name
    left join sb_match_team_ids atm
        on m.match_id = atm.match_id and m.away_team_name = atm.team_name
    where htm.team_id is not null
      and atm.team_id is not null

),

ws_match_team_ids as (

    select distinct
        match_id,
        team_id
    from {{ ref('stg_wyscout__events') }}
    where team_id is not null

),

ws_matches as (

    select
        match_id,
        'wyscout'    as provider,
        min(team_id) as home_team_id,
        max(team_id) as away_team_id
    from ws_match_team_ids
    group by match_id
    having count(distinct team_id) = 2

),

match_teams as (

    select match_id, provider, home_team_id, away_team_id from sb_matches
    union all
    select match_id, provider, home_team_id, away_team_id from ws_matches

),

match_teams_keyed as (

    select
        mt.match_id,
        dm.match_key,
        mt.home_team_id,
        mt.away_team_id
    from match_teams mt
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = mt.provider
       and dm.native_match_id = cast(mt.match_id as string)

),

goals as (

    -- int_unified_shots emits match_key (not match_id) since PR 3. We pull
    -- match_key here and recover match_id downstream via the match_teams_keyed
    -- join so int_running_score's output schema stays backward compatible
    -- (fct_action_values and fct_shots both consume it; fct_shots uses
    -- match_key post-PR 3, fct_action_values still uses match_id until PR 4).
    select
        s.match_key,
        s.team_id    as scoring_team_id,
        s.period,
        s.minute,
        s.second
    from {{ ref('int_unified_shots') }} s
    where s.shot_outcome = 'Goal'

),

goals_with_scores as (

    select
        mt.match_id,          -- recovered from match_teams_keyed; legacy consumer FK
        g.match_key,
        mt.home_team_id,
        mt.away_team_id,
        g.period,
        g.minute,
        g.second,
        sum(case when g.scoring_team_id = mt.home_team_id then 1 else 0 end)
            over (partition by g.match_key
                  order by g.period, g.minute, g.second
                  rows between unbounded preceding and current row)
            as home_score_after,
        sum(case when g.scoring_team_id = mt.away_team_id then 1 else 0 end)
            over (partition by g.match_key
                  order by g.period, g.minute, g.second
                  rows between unbounded preceding and current row)
            as away_score_after
    from goals g
    inner join match_teams_keyed mt on g.match_key = mt.match_key

),

kickoffs as (

    select
        match_id,
        match_key,
        home_team_id,
        away_team_id,
        1    as period,
        0    as minute,
        0    as second,
        0    as home_score_after,
        0    as away_score_after
    from match_teams_keyed

)

select * from kickoffs
union all
select * from goals_with_scores
