{{ config(
    materialized='table',
    liquid_clustered_by=['competition_id']
) }}
-- fct_funnel_stages_agg.sql
-- Pre-aggregated conversion funnel stages for Taipy Conversion Funnel page.
--
-- Motivation (2026-04-17 D58 perf + correctness audit):
-- The live funnel query scans fct_action_values (9.5 M rows). Season mode hits
-- a Parallel Seq Scan (6,305 ms no-gs / 37,800 ms with gs) — exceeding the
-- app's 30 s statement_timeout for prolific teams. Simultaneously, the old
-- Taipy query truncated results at LIMIT 500000, silently dropping 57 % of
-- actions for (comp=11, team=217) — under-reporting A3 entries, shots and
-- goals by >50 %. Pre-aggregating to (match_id, team_id, game_state) grain
-- yields ~12,145 rows total and closes both bugs in one change.
--
-- Grain: (match_id, team_id, game_state)
--   — dbt_utils.unique_combination_of_columns asserts this.
--   — opponent_team_id is derivable from match_summary home/away.
--
-- Straddler handling (V01 Phase 0 verification):
--   168,298 (match_id, possession_id) pairs span >1 game_state within a match.
--   pos_in_gs  — COUNT(DISTINCT possession_id) within this (match, team, gs);
--                a straddler is counted ONCE per game_state it touches.
--   pos_in_match — COUNT(DISTINCT possession_id) across the full (match, team);
--                  replicated on every gs row for that match+team so the app
--                  can dedup via groupby((match,team)).first().sum() at gs=All.
--
-- Wyscout handling:
--   Wyscout actions have possession_id = NULL. Current Python treats those
--   as 1 synthetic possession per match (at gs=All) or per (match, gs) at
--   gs-filter. wy_match_flag=1 if a team had any NULL-possession row during
--   THIS specific (match, team, gs); flag is per-gs (not match-level). The
--   app dedups at the driver via
--   COUNT(DISTINCT CASE WHEN wy_match_flag=1 THEN match_id END), which works
--   correctly for both gs-filtered (sees only that gs's flags) and gs=All
--   (unions all gs's flags and dedups on match_id via nunique).
--
--   Earlier design had wy_match_flag at match-level (max across gs rows),
--   but that over-counted: a match with Wyscout rows only during drawing
--   would register flag=1 on ALL gs rows, so a winning-gs query would
--   erroneously count it as a Wyscout match during winning.
--
-- Other invariants (V04, V09):
--   INNER JOIN to fct_match_summary drops any orphaned action rows (V04: 0 in current data).
--   opponent_team_id derivation assumes home_team_id != away_team_id (V09: 0 violations).

with base as (

    select
        av.match_id,
        av.competition_id,
        av.team_id,
        av.game_state,
        av.possession_id,
        av.possession_team_id,
        av.start_x,
        av.end_x,
        av.action_type,
        av.action_result,
        ms.home_team_id,
        ms.away_team_id
    from {{ ref('fct_action_values') }} av
    -- fct_match_summary was migrated to match_key in PR 2 (ADR-011) and no
    -- longer has match_id. fct_action_values gained match_key in PR 4b; join
    -- on the Kimball surrogate. Downstream CTEs still grain on match_id,
    -- which remains on the base rows via av.match_id (selected above).
    inner join {{ ref('fct_match_summary') }} ms using (match_key)
    where av.team_id is not null
      and av.game_state is not null

),

own_possession as (

    select
        *,
        case
            when team_id = home_team_id then away_team_id
            else home_team_id
        end as opponent_team_id
    from base
    where possession_team_id is null or possession_team_id = team_id

),

per_gs as (

    select
        match_id,
        competition_id,
        team_id,
        opponent_team_id,
        game_state,
        count(distinct case when possession_id is not null then possession_id end)      as pos_in_gs,
        max(case when possession_id is null then 1 else 0 end)                           as wy_match_flag,
        sum(case when start_x <= 70 and end_x > 70 then 1 else 0 end)                    as a3_entries,
        sum(case when action_type in ('shot','shot_penalty','shot_freekick') then 1 else 0 end) as shots,
        sum(case
                when action_type in ('shot','shot_penalty','shot_freekick')
                 and action_result = 'success'
                then 1 else 0
            end)                                                                         as goals
    from own_possession
    group by match_id, competition_id, team_id, opponent_team_id, game_state

),

per_match as (

    select
        match_id,
        team_id,
        count(distinct case when possession_id is not null then possession_id end) as pos_in_match
    from own_possession
    group by match_id, team_id

),

final as (

    select
        cast(g.match_id as bigint)             as match_id,
        cast(g.competition_id as int)          as competition_id,
        cast(g.team_id as int)                 as team_id,
        cast(g.opponent_team_id as int)        as opponent_team_id,
        cast(g.game_state as string)           as game_state,
        cast(g.pos_in_gs as bigint)            as pos_in_gs,
        cast(m.pos_in_match as bigint)         as pos_in_match,
        cast(g.a3_entries as bigint)           as a3_entries,
        cast(g.shots as bigint)                as shots,
        cast(g.goals as bigint)                as goals,
        cast(g.wy_match_flag as smallint)      as wy_match_flag,
        current_timestamp()                    as _loaded_at
    from per_gs g
    inner join per_match m using (match_id, team_id)

)

select * from final
