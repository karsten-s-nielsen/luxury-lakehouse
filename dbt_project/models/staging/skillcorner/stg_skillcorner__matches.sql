-- stg_skillcorner__matches.sql
-- SkillCorner match metadata staging (roster format: one row per player-match).
--
-- Sources from bronze.skillcorner_matches which denormalizes match.json into
-- a per-player roster. This is the authoritative source for SkillCorner
-- player names, team names, competition info, and pitch dimensions.

with source as (

    select * from {{ source('skillcorner', 'skillcorner_matches') }}

)

select
    match_id,
    player_id,
    team_id,
    player_name,
    first_name,
    last_name,
    jersey_number,
    position_name,
    position_acronym,
    team_name,
    team_short_name,
    home_team_id,
    away_team_id,
    competition_id,
    competition_name,
    season_id,
    season_name,
    match_date,
    stadium_name,
    pitch_length,
    pitch_width,
    period_boundaries,
    -- b.1 bronze-completeness: playing_time + player metadata
    start_time,
    end_time,
    minutes_played,
    start_frame,
    end_frame,
    minutes_tip,
    minutes_otip,
    yellow_card,
    red_card,
    injured,
    goal,
    own_goal,
    trackable_object,
    birthday,
    gender,
    team_player_id,
    _ingested_at
from source
