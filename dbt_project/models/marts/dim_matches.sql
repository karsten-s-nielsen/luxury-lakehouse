-- dim_matches.sql
-- Conformed match dimension unifying StatsBomb, Wyscout, IDSSE, and Metrica.
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

    select
        native_match_id,
        provider,
        cast(null as string)           as competition_id,
        cast(null as string)           as season_id,
        cast(null as date)             as match_date,
        home_team_name,
        away_team_name
    from {{ ref('stg_metrica__matches') }}

),

unioned as (

    select * from statsbomb_matches
    union all
    select * from wyscout_matches
    union all
    select * from idsse_matches
    union all
    select * from metrica_matches

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
