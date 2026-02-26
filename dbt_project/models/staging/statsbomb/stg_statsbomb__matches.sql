-- stg_statsbomb__matches.sql
-- Clean and flatten match metadata from StatsBomb bronze data.
--
-- Key transformations needed:
--   1. Extract competition info from nested JSON: competition.competition_id, competition.competition_name
--   2. Extract season info: season.season_id, season.season_name
--   3. Extract home/away team objects: home_team.home_team_id, home_team.home_team_name
--   4. Extract manager info (may be null): home_team.managers[0].name
--   5. Extract referee info: referee.name
--   6. Parse match_date string to proper DATE type
--   7. Extract stadium and metadata fields

with source as (

    select * from {{ source('statsbomb', 'statsbomb_matches') }}

),

cleaned as (

    select
        -- Primary key
        match_id,

        -- Competition and season
        -- TODO: Extract from nested competition and season JSON objects
        cast(null as int)                               as competition_id,
        cast(null as string)                            as competition_name,
        cast(null as int)                               as season_id,
        cast(null as string)                            as season_name,

        -- Match date
        -- TODO: Cast match_date string to DATE type
        cast(null as date)                              as match_date,

        -- Home team
        -- TODO: Extract from nested home_team JSON object
        cast(null as int)                               as home_team_id,
        cast(null as string)                            as home_team_name,
        cast(null as string)                            as home_team_country,
        cast(null as string)                            as home_manager,

        -- Away team
        -- TODO: Extract from nested away_team JSON object
        cast(null as int)                               as away_team_id,
        cast(null as string)                            as away_team_name,
        cast(null as string)                            as away_team_country,
        cast(null as string)                            as away_manager,

        -- Score
        cast(null as int)                               as home_score,
        cast(null as int)                               as away_score,

        -- Match metadata
        -- TODO: Extract from top-level and nested fields
        cast(null as string)                            as referee_name,
        cast(null as string)                            as stadium_name,
        cast(null as string)                            as match_status,
        cast(null as string)                            as match_week,

        -- Data provenance
        cast(null as string)                            as data_version

    from source

)

select * from cleaned
