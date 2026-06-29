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
-- (from flattened statsbombpy output) but NO team IDs in the staging model.
-- Wyscout staging exposes neither team names nor team IDs at this layer.
-- For StatsBomb, home/away team IDs are resolved from stg_statsbomb__events
-- via team_name→team_id lookup. For Wyscout, min/max team_id per match
-- gives a deterministic (though arbitrary home/away) assignment.
-- IDSSE, Metrica, SkillCorner, and GradientSports all have team IDs in
-- their staging match models.

-- StatsBomb: resolve team IDs from events (matches staging has names only).
with sb_event_team_ids as (

    select distinct
        match_id,
        team_id,
        team_name
    from {{ ref('stg_statsbomb__events') }}
    where team_id is not null

),

statsbomb_matches as (

    select
        cast(m.match_id as string)     as native_match_id,
        'statsbomb'                    as provider,
        cast(m.competition_id as string) as competition_id,
        cast(m.season_id as string)    as season_id,
        cast(m.match_date as date)     as match_date,
        m.home_team_name,
        m.away_team_name,
        cast(htm.team_id as string)    as home_team_id_native,
        cast(atm.team_id as string)    as away_team_id_native,
        -- Per-match HF redistribution tier (spec 2026-06-29 §6.4). StatsBomb has
        -- no per-match `visibility` feed → no signal → provider-default PUBLIC
        -- (classify_access_tier('statsbomb', None) == 'public').
        cast(null as string)           as visibility,
        'public'                       as access_tier
    from {{ ref('stg_statsbomb__matches') }} m
    left join sb_event_team_ids htm
        on m.match_id = htm.match_id and m.home_team_name = htm.team_name
    left join sb_event_team_ids atm
        on m.match_id = atm.match_id and m.away_team_name = atm.team_name

),

-- Wyscout: resolve team IDs from events (matches staging has neither
-- names nor IDs). min/max gives a deterministic but arbitrary assignment.
ws_event_team_ids as (

    select distinct
        match_id,
        team_id
    from {{ ref('stg_wyscout__events') }}
    where team_id is not null

),

wyscout_matches as (

    select
        cast(m.match_id as string)     as native_match_id,
        'wyscout'                      as provider,
        cast(m.competition_id as string) as competition_id,
        cast(m.season_id as string)    as season_id,
        cast(m.match_date as date)     as match_date,
        cast(null as string)           as home_team_name,
        cast(null as string)           as away_team_name,
        cast(ws.home_team_id as string) as home_team_id_native,
        cast(ws.away_team_id as string) as away_team_id_native,
        -- No per-match visibility feed → provider-default PUBLIC.
        cast(null as string)           as visibility,
        'public'                       as access_tier
    from {{ ref('stg_wyscout__matches') }} m
    left join (
        select
            match_id,
            min(team_id) as home_team_id,
            max(team_id) as away_team_id
        from ws_event_team_ids
        group by match_id
        having count(distinct team_id) = 2
    ) ws on m.match_id = ws.match_id

),

idsse_matches as (

    select
        native_match_id,
        provider,
        competition_id,
        cast(null as string)           as season_id,
        cast(null as date)             as match_date,
        home_team_name,
        away_team_name,
        home_team_id                   as home_team_id_native,
        away_team_id                   as away_team_id_native,
        -- No per-match visibility feed → provider-default PUBLIC.
        cast(null as string)           as visibility,
        'public'                       as access_tier
    from {{ ref('stg_idsse__matches') }}

),

metrica_matches as (

    -- PR-LL2 Path B close-out (2026-04-29, ADR-018 + Bug #4):
    -- pass competition_id through from staging instead of hardcoding NULL.
    -- stg_metrica__matches emits 'metrica-sample' (PR 5a, ADR-011) and
    -- dim_competitions has the matching row — without this passthrough,
    -- generate_competition_key returns NULL for all Metrica rows, breaking
    -- fct_action_values.competition_key resolution.
    -- Metrica is anonymized — synthesize home/away team IDs to match the
    -- 'metrica_{match_id}_{home|away}' convention from SPADL conversion.
    select
        native_match_id,
        provider,
        competition_id,
        cast(null as string)           as season_id,
        cast(null as date)             as match_date,
        home_team_name,
        away_team_name,
        concat('metrica_', native_match_id, '_home') as home_team_id_native,
        concat('metrica_', native_match_id, '_away') as away_team_id_native,
        -- No per-match visibility feed → provider-default PUBLIC.
        cast(null as string)           as visibility,
        'public'                       as access_tier
    from {{ ref('stg_metrica__matches') }}

),

skillcorner_matches as (

    -- SkillCorner matches sourced from stg_skillcorner__matches (roster format).
    -- Real competition/season/date metadata from match.json via pining-for-the-data API.
    -- Aggregate across roster rows to get one row per match, resolving team names
    -- and team IDs by matching team_id to home_team_id / away_team_id.
    select
        cast(match_id as string)                                            as native_match_id,
        'skillcorner'                                                       as provider,
        cast(max(competition_id) as string)                                 as competition_id,
        cast(max(season_id) as string)                                      as season_id,
        cast(max(match_date) as date)                                       as match_date,
        max(case when team_id = home_team_id then team_name end)            as home_team_name,
        max(case when team_id = away_team_id then team_name end)            as away_team_name,
        cast(max(home_team_id) as string)                                   as home_team_id_native,
        cast(max(away_team_id) as string)                                   as away_team_id_native,
        -- Per-match HF redistribution tier (spec 2026-06-29 §6.4). SkillCorner
        -- carries a real pining `visibility` feed → raw value + the derived
        -- access_tier ride through bronze.skillcorner_matches. Aggregate across
        -- roster rows (all share one match's visibility). NULL until the row is
        -- (re-)ingested with the visibility signal → fail-safe restricted.
        max(visibility)                                                     as visibility,
        max(access_tier)                                                    as access_tier
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
        away_team_name,
        home_team_id                   as home_team_id_native,
        away_team_id                   as away_team_id_native,
        -- Per-match HF redistribution tier (spec 2026-06-29 §6.4). GradientSports
        -- carries a real pining `visibility` feed (provider-default RESTRICTED
        -- when absent); raw value + derived access_tier ride through bronze.
        visibility,
        access_tier
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
        away_team_name,
        home_team_id_native,
        away_team_id_native,
        -- Per-match HF redistribution attributes (spec 2026-06-29 §6.4):
        -- raw provider `visibility` (NULL when no feed) + derived `access_tier`
        -- ('public'/'restricted'). Per-match source of truth for the publish split.
        visibility,
        access_tier

    from unioned

)

select * from final
