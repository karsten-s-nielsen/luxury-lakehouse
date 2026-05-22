{{ config(
    materialized='table',
    tags=['marts', 'dimension']
) }}
-- dim_matches.sql
-- Conformed match dimension unifying StatsBomb, Wyscout, IDSSE, Metrica, SkillCorner, and Gradient Sports.
--
-- PRIMARY KEY: match_key (BIGINT surrogate, deterministic hash).
-- UNIQUE: (provider, native_match_id).
--
-- Kimball conformed dimension per ADR-011. PR 1 establishes this dim; no
-- facts reference it yet. PR 2 migrates fct_passes + fct_line_breaking_results
-- + fct_match_summary to match_key FKs. Subsequent PRs migrate remaining facts.
--
-- Cardinality at the time of PR 1:
--   - statsbomb: ~3500 matches (open data)
--   - wyscout:   ~1900 matches (open data)
--   - idsse:     7 matches
--   - metrica:   3 matches
--
-- Note on coverage: StatsBomb staging exposes home_team_name / away_team_name
-- (from flattened statsbombpy output) but NO team IDs. Wyscout staging exposes
-- neither team names nor team IDs at this layer. IDSSE has names from
-- stg_tracking__player_metadata. Metrica is anonymised. team_id columns are
-- therefore omitted from this dim; downstream consumers that need team IDs
-- should join the per-provider dimension (e.g., dim_players → teams).

with statsbomb_matches as (

    select
        cast(match_id as string)       as native_match_id,
        'statsbomb'                    as provider,
        cast(competition_id as string) as competition_id,
        cast(season_id as string)      as season_id,
        cast(match_date as date)       as match_date,
        home_team_name,
        away_team_name
    from {{ ref('stg_statsbomb__matches') }}

),

wyscout_matches as (

    select
        cast(match_id as string)       as native_match_id,
        'wyscout'                      as provider,
        cast(competition_id as string) as competition_id,
        cast(season_id as string)      as season_id,
        cast(match_date as date)       as match_date,
        cast(null as string)           as home_team_name,
        cast(null as string)           as away_team_name
    from {{ ref('stg_wyscout__matches') }}

),

idsse_matches as (

    select
        native_match_id,
        provider,
        competition_id,
        cast(null as string)           as season_id,
        cast(null as date)             as match_date,
        home_team_name,
        away_team_name
    from {{ ref('stg_idsse__matches') }}

),

metrica_matches as (

    -- PR-LL2 Path B close-out (2026-04-29, ADR-018 + Bug #4):
    -- pass competition_id through from staging instead of hardcoding NULL.
    -- stg_metrica__matches emits 'metrica-sample' (PR 5a, ADR-011) and
    -- dim_competitions has the matching row — without this passthrough,
    -- generate_competition_key returns NULL for all Metrica rows, breaking
    -- fct_action_values.competition_key resolution.
    select
        native_match_id,
        provider,
        competition_id,
        cast(null as string)           as season_id,
        cast(null as date)             as match_date,
        home_team_name,
        away_team_name
    from {{ ref('stg_metrica__matches') }}

),

skillcorner_matches as (

    -- SkillCorner matches sourced from stg_skillcorner__matches (roster format).
    -- Real competition/season/date metadata from match.json via pining-for-the-data API.
    -- Aggregate across roster rows to get one row per match, resolving team names
    -- by matching team_id to home_team_id / away_team_id.
    select
        cast(match_id as string)                                            as native_match_id,
        'skillcorner'                                                       as provider,
        cast(max(competition_id) as string)                                 as competition_id,
        cast(max(season_id) as string)                                      as season_id,
        cast(max(match_date) as date)                                       as match_date,
        max(case when team_id = home_team_id then team_name end)            as home_team_name,
        max(case when team_id = away_team_id then team_name end)            as away_team_name
    from {{ ref('stg_skillcorner__matches') }}
    group by match_id

),

gradientsports_matches as (

    select
        cast(match_id as string)       as native_match_id,
        'gradientsports'               as provider,
        competition_id,
        season_id,
        cast(match_date as date)       as match_date,
        home_team_name,
        away_team_name
    from {{ ref('stg_gradientsports__metadata') }}

),

unioned as (

    select * from statsbomb_matches
    union all
    select * from wyscout_matches
    union all
    select * from idsse_matches
    union all
    select * from metrica_matches
    union all
    select * from skillcorner_matches
    union all
    select * from gradientsports_matches

),

final as (

    select
        {{ generate_match_key('provider', 'native_match_id') }} as match_key,
        -- Kimball surrogate FK to dim_competitions (added PR 2, ADR-011).
        -- NULL for Metrica (no competition metadata in open-data).
        {{ generate_competition_key('provider', 'competition_id') }} as competition_key,
        provider,
        native_match_id,
        competition_id,
        season_id,
        match_date,
        home_team_name,
        away_team_name

    from unioned

)

select * from final
