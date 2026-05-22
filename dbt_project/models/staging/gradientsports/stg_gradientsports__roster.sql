-- stg_gradientsports__roster.sql
-- Staging view over bronze.gradientsports_roster.
-- Grain: one row per player per match (~51 rows/match).

select
    match_id,
    cast(`player.id` as string)   as player_id,
    `player.nickname`             as player_nickname,
    cast(`team.id` as string)     as team_id,
    `team.name`                   as team_name,
    `positionGroupType`           as position_group,
    `shirtNumber`                 as shirt_number,
    `started`,
    _ingested_at
from {{ source('gradientsports', 'gradientsports_roster') }}
