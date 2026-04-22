{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='match_key',
    on_schema_change='fail',
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
    select
        dm.match_key,
        dm.competition_key,
        try_cast(dm.competition_id as int)              as competition_id,
        try_cast(dm.season_id as int)                   as season_id,
        dm.match_date,
        dm.home_team_name,
        dm.away_team_name,
        cast(null as int)                               as home_team_id,
        cast(null as int)                               as away_team_id,
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
    where dm.provider != 'statsbomb'

)

select * from sb_summary
union all
select * from non_sb_summary
