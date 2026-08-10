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
    -- Per-match HF redistribution signal (spec 2026-06-29 §6.2): raw pining
    -- `visibility` + the derived `access_tier`, stamped on bronze at ingestion.
    visibility,
    access_tier,

    -- Bronze pass-through cols (PR-2a). Surfaced with snake_case names so every bronze column
    -- is either preserved, renamed, or intentionally dropped — the invariant
    -- src/tests/test_staging_coverage.py enforces with INITIAL_BRONZE_STAGING_GAPS locked
    -- empty. These carry no analytical consumer today; they exist so the bronze contract is
    -- complete and a NEW provider field cannot slip in undocumented.
    `id`                              as gs_metadata_row_id,
    `videoUrl`                        as video_url,
    `startPeriod1`                    as start_period_1,
    `endPeriod1`                      as end_period_1,
    `startPeriod2`                    as start_period_2,
    `endPeriod2`                      as end_period_2,
    `period1`                         as period_1,
    `period2`                         as period_2,
    `halfPeriod`                      as half_period,
    `stadium.pitches`                 as stadium_pitches,
    `homeTeamKit.name`                as home_team_kit_name,
    `homeTeamKit.primaryColor`        as home_team_kit_primary_color,
    `homeTeamKit.primaryTextColor`    as home_team_kit_primary_text_color,
    `homeTeamKit.secondaryColor`      as home_team_kit_secondary_color,
    `homeTeamKit.secondaryTextColor`  as home_team_kit_secondary_text_color,
    `awayTeamKit.name`                as away_team_kit_name,
    `awayTeamKit.primaryColor`        as away_team_kit_primary_color,
    `awayTeamKit.primaryTextColor`    as away_team_kit_primary_text_color,
    `awayTeamKit.secondaryColor`      as away_team_kit_secondary_color,
    `awayTeamKit.secondaryTextColor`  as away_team_kit_secondary_text_color,

    _ingested_at
from {{ source('gradientsports', 'gradientsports_metadata') }}
