-- stg_statsbomb__matches.sql
-- Clean and flatten match metadata from StatsBomb bronze data.
--
-- The ingestion layer (statsbombpy) already flattened most nested fields:
--   competition_id, season_id → top-level BIGINT columns
--   competition, season, home_team, away_team → plain string columns (names only)
--   stadium, referee, home_managers, away_managers → plain strings
-- There are NO home_team_id / away_team_id columns in bronze.

with source as (

    select * from {{ source('statsbomb', 'statsbomb_matches') }}

),

cleaned as (

    select
        -- Primary key
        match_id,

        -- Competition and season (already flat columns)
        cast(competition_id as int)                   as competition_id,
        competition                                   as competition_name,
        cast(season_id as int)                        as season_id,
        season                                        as season_name,

        -- Match date
        cast(match_date as date)                      as match_date,

        -- Team names (no team_id available from statsbombpy flattened output)
        home_team                                     as home_team_name,
        away_team                                     as away_team_name,

        -- Managers
        home_managers                                 as home_manager,
        away_managers                                 as away_manager,

        -- Score (already flat BIGINT)
        cast(home_score as int)                       as home_score,
        cast(away_score as int)                       as away_score,

        -- Match metadata
        referee                                       as referee_name,
        stadium                                       as stadium_name,
        match_status,
        cast(match_week as string)                    as match_week,
        competition_stage,

        -- Data provenance (already a top-level column)
        data_version,

        -- Bronze pass-through cols (PR 2 — Kimball migration, ADR-011).
        -- Surface remaining bronze cols with their bronze names so downstream
        -- models and analysts can reach them without re-reading bronze. The
        -- casts and renames above remain the preferred consumption path;
        -- these are the raw source-of-truth view. See
        -- src/tests/test_staging_coverage.py INITIAL_BRONZE_STAGING_GAPS.
        _ingested_at,
        away_managers,
        away_team,
        competition,
        home_managers,
        home_team,
        kick_off,
        last_updated,
        last_updated_360,
        match_status_360,
        referee,
        season,
        shot_fidelity_version,
        stadium,
        xy_fidelity_version

    from source

)

select * from cleaned
