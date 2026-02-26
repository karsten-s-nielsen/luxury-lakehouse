-- dim_teams.sql
-- Team dimension table combining data from all sources.
--
-- Extracts unique teams from match metadata across all data providers.
-- Similar to dim_players, cross-source deduplication is deferred to a
-- future phase; for now each source's team_id is treated as distinct.
--
-- Grain: one row per unique team (per data source).

with statsbomb_home_teams as (

    select distinct
        home_team_id                                    as team_id,
        home_team_name                                  as team_name,
        home_team_country                               as country,
        'statsbomb'                                     as data_source

    from {{ ref('stg_statsbomb__matches') }}
    where home_team_id is not null

),

statsbomb_away_teams as (

    select distinct
        away_team_id                                    as team_id,
        away_team_name                                  as team_name,
        away_team_country                               as country,
        'statsbomb'                                     as data_source

    from {{ ref('stg_statsbomb__matches') }}
    where away_team_id is not null

),

all_teams as (

    select * from statsbomb_home_teams
    union
    select * from statsbomb_away_teams

)

-- TODO: Add Wyscout teams from wyscout_matches.teamsData JSON
-- TODO: Add Metrica teams (Home/Away anonymized teams)

select
    team_id,
    team_name,
    country,
    data_source

from all_teams
