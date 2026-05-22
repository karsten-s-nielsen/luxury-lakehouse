-- stg_gradientsports__metadata.sql
-- Staging view over bronze.gradientsports_metadata.
-- Grain: one row per match. Source: pining-for-the-data metadata artifact.

select
    match_id,
    cast(`homeTeam.id` as string)     as home_team_id,
    `homeTeam.name`                   as home_team_name,
    `homeTeam.shortName`              as home_team_short_name,
    cast(`awayTeam.id` as string)     as away_team_id,
    `awayTeam.name`                   as away_team_name,
    `awayTeam.shortName`              as away_team_short_name,
    cast(`competition.id` as string)  as competition_id,
    `competition.name`                as competition_name,
    `season`                          as season_id,
    cast(`date` as timestamp)         as match_date,
    cast(`stadium.id` as string)      as stadium_id,
    `stadium.name`                    as stadium_name,
    `homeTeamStartLeft`               as home_team_start_left,
    `homeTeamStartLeftExtraTime`      as home_team_start_left_extra_time,
    `fps`,
    cast(`week` as int)               as matchweek,
    _ingested_at
from {{ source('gradientsports', 'gradientsports_metadata') }}
