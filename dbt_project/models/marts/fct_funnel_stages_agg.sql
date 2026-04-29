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
-- PR 5a (ADR-011): Kimball surrogate keys added.
--   - match_key BIGINT: propagated from fct_match_summary via the existing
--     JOIN (using match_key).
--   - team_key BIGINT: resolved via dim_teams on (provider, native_team_id).
--     StatsBomb + Wyscout providers only (funnel is SB+WS sourced).
--   - opponent_team_key BIGINT: same resolution on opponent_team_id.
--   Wyscout home/away team_ids now populate via stg_wyscout__home_away_teams
--   (fct_match_summary patched in PR 5a); the opponent_team_id warn-suppression
--   flips to error severity.
--
-- Straddler handling (V01 Phase 0 verification):
--   168,298 (match_id, possession_id) pairs span >1 game_state within a match.
--   pos_in_gs  — COUNT(DISTINCT possession_id) within this (match, team, gs);
--                a straddler is counted ONCE per game_state it touches.
--   pos_in_match — COUNT(DISTINCT possession_id) across the full (match, team);
--                  replicated on every gs row for that match+team so the app
--                  can dedup via groupby((match,team)).first().sum() at gs=All.
--
-- LL2 Path B (2026-04-29): Canonical possession_id semantics.
--   The pre-LL2 Wyscout-synthetic-possession workaround retired. Possession
--   IDs are now sourced from silly-kicks's heuristic ``add_possessions``
--   (sourced into ``av.possession_id_heuristic``, exposed as canonical
--   ``possession_id`` on the mart) and populated for ALL 4 sources
--   (StatsBomb / Wyscout / IDSSE / Metrica). The previous COUNT(DISTINCT
--   CASE WHEN possession_id IS NOT NULL ...) wrapping is no longer needed
--   — every row has a possession_id. ``wy_match_flag`` removed (always 0
--   post-LL2 since possession_id is non-null for every row).
--
--   The own-possession filter uses ``statsbomb_possession_team_id`` (the
--   β-consistent renamed StatsBomb-native passthrough). It remains
--   meaningful only for StatsBomb rows; other sources fall through the
--   NULL branch and treat every row as own-possession. PR-LL3 may
--   extend the heuristic to also emit possession_team via
--   ``add_possessions_with_team`` and let this filter narrow further.
--
-- Other invariants (V04, V09):
--   INNER JOIN to fct_match_summary drops any orphaned action rows (V04: 0 in current data).
--   opponent_team_id derivation assumes home_team_id != away_team_id (V09: 0 violations).

with base as (

    select
        ms.match_key,
        av.match_id,
        av.competition_id,
        av.team_id,
        av.game_state,
        av.possession_id,
        av.statsbomb_possession_team_id,
        av.start_x,
        av.end_x,
        av.action_type,
        av.action_result,
        av.data_source,
        av.team_id_native,
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
    -- LL2 Path B: own-possession filter uses the β-consistent
    -- statsbomb_possession_team_id (was possession_team_id pre-LL2).
    -- For non-StatsBomb sources this column is NULL, falling through to
    -- the "treat every row as own-possession" branch.
    where statsbomb_possession_team_id is null or statsbomb_possession_team_id = team_id

),

per_gs as (

    select
        match_key,
        match_id,
        competition_id,
        team_id,
        opponent_team_id,
        data_source,
        game_state,
        -- LL2 Path B: possession_id is canonical heuristic, populated for
        -- ALL sources. No more NULL handling needed.
        count(distinct possession_id)                                                    as pos_in_gs,
        sum(case when start_x <= 70 and end_x > 70 then 1 else 0 end)                    as a3_entries,
        sum(case when action_type in ('shot','shot_penalty','shot_freekick') then 1 else 0 end) as shots,
        sum(case
                when action_type in ('shot','shot_penalty','shot_freekick')
                 and action_result = 'success'
                then 1 else 0
            end)                                                                         as goals
    from own_possession
    group by match_key, match_id, competition_id, team_id, opponent_team_id, data_source, game_state

),

per_match as (

    select
        match_id,
        team_id,
        count(distinct possession_id) as pos_in_match
    from own_possession
    group by match_id, team_id

),

final as (

    select
        g.match_key                                     as match_key,
        cast(g.match_id as bigint)                      as match_id,
        cast(g.competition_id as int)                   as competition_id,
        cast(g.team_id as int)                          as team_id,
        cast(g.opponent_team_id as int)                 as opponent_team_id,
        dt_own.team_key                                 as team_key,
        dt_opp.team_key                                 as opponent_team_key,
        cast(g.game_state as string)                    as game_state,
        cast(g.pos_in_gs as bigint)                     as pos_in_gs,
        cast(m.pos_in_match as bigint)                  as pos_in_match,
        cast(g.a3_entries as bigint)                    as a3_entries,
        cast(g.shots as bigint)                         as shots,
        cast(g.goals as bigint)                         as goals,
        current_timestamp()                             as _loaded_at
    from per_gs g
    inner join per_match m using (match_id, team_id)
    -- PR 5a: Kimball team keys via dim_teams (SB + WS providers only for funnel).
    left join {{ ref('dim_teams') }} dt_own
        on  dt_own.provider = g.data_source
       and dt_own.native_team_id = cast(g.team_id as string)
    left join {{ ref('dim_teams') }} dt_opp
        on  dt_opp.provider = g.data_source
       and dt_opp.native_team_id = cast(g.opponent_team_id as string)

)

select * from final
