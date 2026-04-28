{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='match_key',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key']
) }}
-- fct_match_summary.sql
-- Match-level aggregate metrics. One row per match, keyed by `match_key`
-- (Kimball surrogate FK to dim_matches per ADR-011).
--
-- PR 2 extends coverage from StatsBomb-only to all four providers
-- (StatsBomb, Wyscout, IDSSE, Metrica). Non-StatsBomb rows have NULL
-- metric columns because:
--
--   * Wyscout: open-data events carry team_id but no home/away side
--     designation and no match-level score. Team-name pivots aren't
--     resolvable without the side info.
--   * IDSSE / Metrica: tracking-only data; no shot/pass event
--     provenance in the shape this mart expects.
--
-- Their rows carry the minimum metadata (match_key + team names from
-- dim_matches) so the Taipy match cascade can present them alongside
-- StatsBomb matches.
--
-- PPDA (Passes Per Defensive Action): opponent passes allowed in the
-- defending team's defensive 40% of pitch, divided by that team's
-- defensive actions in the same zone. StatsBomb only.
--
-- PR 7 (ADR-011 close-out): adds Kimball surrogate FKs `home_team_key` and
-- `away_team_key` for ALL providers. Resolution sources differ by provider:
--   * StatsBomb: existing match_team_ids (from stg_statsbomb__events) →
--     dim_teams JOIN.
--   * Wyscout: existing stg_wyscout__home_away_teams (PR 5a bridge) →
--     dim_teams JOIN.
--   * IDSSE / Metrica / SkillCorner: NEW tracking_home_away CTE derives
--     home/away team_id per match from fct_tracking_frames (one row per
--     match-team-frame; DISTINCT collapses to ~2 rows per match) → dim_teams
--     JOIN. SkillCorner onboarded into dim_matches/dim_teams/dim_players in
--     PR 7 alongside this mart.
-- Legacy `home_team_id` / `away_team_id` BIGINT columns remain populated
-- where they were before (StatsBomb + Wyscout); NULL for tracking-only
-- providers as before. PR 8 drops the legacy INT columns post-2026-07-22.
--
-- Downstream tracking-derivative marts (fct_player_positions,
-- fct_position_maps, fct_formation_labels) resolve their own `team_key`
-- by JOINing this mart on `match_key` and CASE-ing on the row's
-- `team='home'|'away'` role string.

with dim as (

    select * from {{ ref('dim_matches') }}

),

sb_matches as (

    select * from {{ ref('stg_statsbomb__matches') }}

),

match_team_ids as (

    select distinct
        match_id,
        team_id,
        team_name
    from {{ ref('stg_statsbomb__events') }}
    where team_id is not null

),

shots as (

    -- PR 3 (ADR-011): fct_shots was migrated from match_id to match_key.
    -- Downstream CTEs here still group/join on native match_id (shared with
    -- stg_statsbomb__events which retains it). Recover match_id via
    -- dim_matches JOIN to preserve existing semantics.
    select
        s.*,
        try_cast(dm.native_match_id as bigint) as match_id
    from {{ ref('fct_shots') }} s
    left join {{ ref('dim_matches') }} dm on s.match_key = dm.match_key

),

passes_sb_ws as (

    -- fct_passes is keyed on match_key post PR 2. We aggregate SB+WS passes
    -- (only those two providers produce usable per-team pass metrics;
    -- IDSSE/Metrica have NULL team_id and so cannot be pivoted home/away).
    select
        match_key,
        team_id,
        count(*)                                        as total_passes,
        sum(case when is_complete then 1 else 0 end)    as completed_passes,
        sum(case when is_progressive then 1 else 0 end) as progressive_passes
    from {{ ref('fct_passes') }}
    where data_source in ('statsbomb', 'wyscout')
      and team_id is not null
    group by match_key, team_id

),

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

defensive_actions as (

    select
        match_id,
        team_id,
        count(*) as def_action_count
    from {{ ref('stg_statsbomb__events') }}
    where event_type in ('Duel', 'Interception', 'Foul Committed', 'Block')
      and location_x > {{ var('pitch_length') }} * 0.4
    group by match_id, team_id

),

opponent_passes_in_def_zone as (

    select
        match_id,
        team_id,
        count(*) as opp_pass_count
    from {{ ref('stg_statsbomb__events') }}
    where event_type = 'Pass'
      and location_x < {{ var('pitch_length') }} * 0.6
    group by match_id, team_id

),

sb_keyed as (

    select
        dm.match_key,
        dm.competition_key,
        m.match_id                                      as sb_match_id,
        cast(m.competition_id as int)                   as competition_id,
        cast(m.season_id as int)                        as season_id,
        m.match_date,
        m.home_team_name,
        m.away_team_name,
        m.home_score,
        m.away_score
    from sb_matches m
    inner join dim dm
        on dm.provider = 'statsbomb'
       and dm.native_match_id = cast(m.match_id as string)

),

tracking_home_away as (

    -- PR 7 hotfix #3: replaced the prior inline fct_tracking_frames-derived CTE
    -- with the shared int_tracking__match_side_team_bridge. The bridge generalises
    -- the per-(match, side)→team_id mapping across all 3 tracking providers and
    -- eliminates the dependency on fct_tracking_frames being rebuilt before this
    -- mart resolves correctly. JOIN dim_matches to recover match_key from the
    -- bridge's native match_id (post-staging-canonicalization).
    select
        dm.match_key,
        mstb.source_provider,
        max(case when mstb.side = 'home' then mstb.team_id end)   as home_team_id_str,
        max(case when mstb.side = 'away' then mstb.team_id end)   as away_team_id_str
    from {{ ref('int_tracking__match_side_team_bridge') }} mstb
    inner join {{ ref('dim_matches') }} dm
        on  dm.provider = mstb.source_provider
       and dm.native_match_id = mstb.match_id
    where mstb.source_provider in ('idsse', 'metrica', 'skillcorner')
    group by dm.match_key, mstb.source_provider

),

sb_summary as (

    select
        sb.match_key,
        sb.competition_key,
        sb.competition_id,
        sb.season_id,
        sb.match_date,
        sb.home_team_name,
        sb.away_team_name,
        htm.team_id                                     as home_team_id,
        atm.team_id                                     as away_team_id,
        dt_h.team_key                                   as home_team_key,
        dt_a.team_key                                   as away_team_key,
        sb.home_score,
        sb.away_score,
        coalesce(hs.total_shots, 0)                     as home_shots,
        coalesce(hs.total_goals, 0)                     as home_goals,
        coalesce(hs.total_xg, 0)                        as home_xg,
        coalesce(hs.shots_on_target, 0)                 as home_shots_on_target,
        coalesce(aws.total_shots, 0)                    as away_shots,
        coalesce(aws.total_goals, 0)                    as away_goals,
        coalesce(aws.total_xg, 0)                       as away_xg,
        coalesce(aws.shots_on_target, 0)                as away_shots_on_target,
        coalesce(hp.total_passes, 0)                    as home_total_passes,
        coalesce(hp.completed_passes, 0)                as home_completed_passes,
        coalesce(hp.progressive_passes, 0)              as home_progressive_passes,
        coalesce(ap.total_passes, 0)                    as away_total_passes,
        coalesce(ap.completed_passes, 0)                as away_completed_passes,
        coalesce(ap.progressive_passes, 0)              as away_progressive_passes,
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
        case
            when coalesce(hp.total_passes, 0) + coalesce(ap.total_passes, 0) > 0
            then round(hp.total_passes * 100.0 / (hp.total_passes + ap.total_passes), 1)
            else 50.0
        end                                             as home_possession_pct,
        coalesce(hs.total_xg, 0) - coalesce(aws.total_xg, 0) as xg_difference,
        case
            when coalesce(hda.def_action_count, 0) > 0
            then round(coalesce(aop.opp_pass_count, 0) * 1.0 / hda.def_action_count, 2)
            else null
        end                                             as home_ppda,
        case
            when coalesce(ada.def_action_count, 0) > 0
            then round(coalesce(hop.opp_pass_count, 0) * 1.0 / ada.def_action_count, 2)
            else null
        end                                             as away_ppda,
        case
            when sb.home_score > sb.away_score then 'home_win'
            when sb.home_score < sb.away_score then 'away_win'
            else 'draw'
        end                                             as match_result
    from sb_keyed sb
    left join match_team_ids htm
        on sb.sb_match_id = htm.match_id and sb.home_team_name = htm.team_name
    left join match_team_ids atm
        on sb.sb_match_id = atm.match_id and sb.away_team_name = atm.team_name
    left join {{ ref('dim_teams') }} dt_h
        on  dt_h.provider = 'statsbomb'
       and dt_h.native_team_id = cast(htm.team_id as string)
    left join {{ ref('dim_teams') }} dt_a
        on  dt_a.provider = 'statsbomb'
       and dt_a.native_team_id = cast(atm.team_id as string)
    left join match_shots hs on sb.sb_match_id = hs.match_id and htm.team_id = hs.team_id
    left join match_shots aws on sb.sb_match_id = aws.match_id and atm.team_id = aws.team_id
    left join passes_sb_ws hp on sb.match_key = hp.match_key and htm.team_id = hp.team_id
    left join passes_sb_ws ap on sb.match_key = ap.match_key and atm.team_id = ap.team_id
    left join defensive_actions hda on sb.sb_match_id = hda.match_id and htm.team_id = hda.team_id
    left join defensive_actions ada on sb.sb_match_id = ada.match_id and atm.team_id = ada.team_id
    left join opponent_passes_in_def_zone aop on sb.sb_match_id = aop.match_id and atm.team_id = aop.team_id
    left join opponent_passes_in_def_zone hop on sb.sb_match_id = hop.match_id and htm.team_id = hop.team_id

),

non_sb_summary as (

    -- Wyscout, IDSSE, Metrica rows: minimum metadata from dim_matches.
    -- All metric columns NULL (see file-level header for rationale).
    --
    -- PR 5a (ADR-011): Wyscout home/away team_id populated via new
    -- stg_wyscout__home_away_teams bridge (parses teams_data_parsed MAP).
    -- Previously NULL for ~36% of rows; feeds fct_funnel_stages_agg.
    -- opponent_team_id warn→error flip. IDSSE + Metrica stay NULL in the
    -- legacy INT column; their native team IDs are STRING (resolved via
    -- team_key at the dim_teams layer when facts migrate).
    select
        dm.match_key,
        dm.competition_key,
        try_cast(dm.competition_id as int)              as competition_id,
        try_cast(dm.season_id as int)                   as season_id,
        dm.match_date,
        dm.home_team_name,
        dm.away_team_name,
        case when dm.provider = 'wyscout' then hab.team_id end as home_team_id,
        case when dm.provider = 'wyscout' then aab.team_id end as away_team_id,
        coalesce(dt_h_ws.team_key, dt_h_track.team_key) as home_team_key,
        coalesce(dt_a_ws.team_key, dt_a_track.team_key) as away_team_key,
        cast(null as int)                               as home_score,
        cast(null as int)                               as away_score,
        cast(null as bigint)                            as home_shots,
        cast(null as bigint)                            as home_goals,
        cast(null as double)                            as home_xg,
        cast(null as bigint)                            as home_shots_on_target,
        cast(null as bigint)                            as away_shots,
        cast(null as bigint)                            as away_goals,
        cast(null as double)                            as away_xg,
        cast(null as bigint)                            as away_shots_on_target,
        cast(null as bigint)                            as home_total_passes,
        cast(null as bigint)                            as home_completed_passes,
        cast(null as bigint)                            as home_progressive_passes,
        cast(null as bigint)                            as away_total_passes,
        cast(null as bigint)                            as away_completed_passes,
        cast(null as bigint)                            as away_progressive_passes,
        cast(null as decimal(26,1))                     as home_pass_completion_pct,
        cast(null as decimal(26,1))                     as away_pass_completion_pct,
        cast(null as decimal(26,1))                     as home_possession_pct,
        cast(null as double)                            as xg_difference,
        cast(null as decimal(25,2))                     as home_ppda,
        cast(null as decimal(25,2))                     as away_ppda,
        cast(null as string)                            as match_result
    from dim dm
    left join {{ ref('stg_wyscout__home_away_teams') }} hab
        on  dm.provider = 'wyscout'
       and try_cast(dm.native_match_id as bigint) = hab.match_id
       and hab.side = 'home'
    left join {{ ref('stg_wyscout__home_away_teams') }} aab
        on  dm.provider = 'wyscout'
       and try_cast(dm.native_match_id as bigint) = aab.match_id
       and aab.side = 'away'
    -- Wyscout team_key resolution via dim_teams (real BIGINT team_id).
    left join {{ ref('dim_teams') }} dt_h_ws
        on  dt_h_ws.provider = 'wyscout'
       and dt_h_ws.native_team_id = cast(hab.team_id as string)
    left join {{ ref('dim_teams') }} dt_a_ws
        on  dt_a_ws.provider = 'wyscout'
       and dt_a_ws.native_team_id = cast(aab.team_id as string)
    -- Tracking-provider home/away derivation + dim_teams JOINs (PR 7).
    left join tracking_home_away tho
        on dm.match_key = tho.match_key
    left join {{ ref('dim_teams') }} dt_h_track
        on  dt_h_track.provider = tho.source_provider
       and dt_h_track.native_team_id = tho.home_team_id_str
    left join {{ ref('dim_teams') }} dt_a_track
        on  dt_a_track.provider = tho.source_provider
       and dt_a_track.native_team_id = tho.away_team_id_str
    where dm.provider != 'statsbomb'

)

select * from sb_summary
union all
select * from non_sb_summary
