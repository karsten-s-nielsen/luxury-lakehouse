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
-- Match teams sourced from dim_matches (home_team_id_native / away_team_id_native).
-- Provider-agnostic: any new provider added to dim_matches automatically
-- gets game_state resolution without modifying this file.

with match_teams as (

    select
        native_match_id,
        match_key,
        home_team_id_native,
        away_team_id_native
    from {{ ref('dim_matches') }}
    where home_team_id_native is not null
      and away_team_id_native is not null

),

goals as (

    -- SB + WS goals from int_unified_shots.
    select
        s.match_key,
        s.team_id    as scoring_team_id,
        s.period,
        s.minute,
        s.second
    from {{ ref('int_unified_shots') }} s
    where s.shot_outcome = 'Goal'

),

spadl_goals as (

    -- Goals from all SPADL-sourced providers (IDSSE, Metrica, GradientSports, etc.).
    -- int_unified_shots only covers StatsBomb + Wyscout; this CTE fills
    -- the gap for every other provider via stg_spadl__action_values.
    -- Note: like int_unified_shots, own goals are not tracked here
    -- (SPADL codes them as action_type='shot' + action_result='success'
    -- for the SCORING team, not the conceding team — consistent with
    -- the existing "own goals are not tracked" limitation in the header).
    select
        dm.match_key,
        av.team_id_native   as scoring_team_id_native,
        av.period,
        av.minute,
        av.second
    from {{ ref('stg_spadl__action_values') }} av
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = av.data_source
       and dm.native_match_id = av.match_id_native
    where av.action_type = 'shot'
      and av.action_result = 'success'
      and av.data_source not in ('statsbomb', 'wyscout')

),

all_goals as (

    -- SB + WS goals from int_unified_shots
    select
        g.match_key,
        cast(g.scoring_team_id as string) as scoring_team_id_native,
        g.period,
        g.minute,
        g.second
    from goals g
    union all
    -- All other SPADL-sourced provider goals
    select match_key, scoring_team_id_native, period, minute, second
    from spadl_goals

),

goals_with_scores as (

    select
        mt.native_match_id,
        g.match_key,
        mt.home_team_id_native,
        mt.away_team_id_native,
        g.period,
        g.minute,
        g.second,
        sum(case when g.scoring_team_id_native = mt.home_team_id_native then 1 else 0 end)
            over (partition by g.match_key
                  order by g.period, g.minute, g.second
                  rows between unbounded preceding and current row)
            as home_score_after,
        sum(case when g.scoring_team_id_native = mt.away_team_id_native then 1 else 0 end)
            over (partition by g.match_key
                  order by g.period, g.minute, g.second
                  rows between unbounded preceding and current row)
            as away_score_after
    from all_goals g
    inner join match_teams mt on g.match_key = mt.match_key

),

kickoffs as (

    select
        native_match_id,
        match_key,
        home_team_id_native,
        away_team_id_native,
        1    as period,
        0    as minute,
        0    as second,
        0    as home_score_after,
        0    as away_score_after
    from match_teams

)

select * from kickoffs
union all
select * from goals_with_scores
