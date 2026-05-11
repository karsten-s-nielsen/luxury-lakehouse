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

idsse_matches as (

    select
        native_match_id   as match_id,
        'idsse'           as provider,
        home_team_id      as home_team_id_native,
        away_team_id      as away_team_id_native
    from {{ ref('stg_idsse__matches') }}

),

metrica_matches as (

    select
        native_match_id   as match_id,
        'metrica'         as provider,
        concat('metrica_', native_match_id, '_home') as home_team_id_native,
        concat('metrica_', native_match_id, '_away') as away_team_id_native
    from {{ ref('stg_metrica__matches') }}

),

match_teams as (

    select cast(match_id as string) as match_id, provider, cast(home_team_id as string) as home_team_id_native, cast(away_team_id as string) as away_team_id_native from sb_matches
    union all
    select cast(match_id as string) as match_id, provider, cast(home_team_id as string) as home_team_id_native, cast(away_team_id as string) as away_team_id_native from ws_matches
    union all
    select match_id, provider, home_team_id_native, away_team_id_native from idsse_matches
    union all
    select match_id, provider, home_team_id_native, away_team_id_native from metrica_matches

),

match_teams_keyed as (

    select
        mt.match_id  as native_match_id,
        dm.match_key,
        mt.home_team_id_native,
        mt.away_team_id_native
    from match_teams mt
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = mt.provider
       and dm.native_match_id = mt.match_id

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

    -- IDSSE + Metrica goals extracted from SPADL actions.
    -- int_unified_shots only covers StatsBomb + Wyscout; this CTE fills
    -- the gap for sources that lack dedicated shot staging models.
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
      and av.data_source in ('idsse', 'metrica')

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
    -- IDSSE + Metrica goals from SPADL actions
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
    inner join match_teams_keyed mt on g.match_key = mt.match_key

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
    from match_teams_keyed

)

select * from kickoffs
union all
select * from goals_with_scores
