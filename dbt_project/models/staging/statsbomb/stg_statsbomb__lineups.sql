-- stg_statsbomb__lineups.sql
-- Flatten lineup data from StatsBomb bronze layer.
--
-- Key transformations needed:
--   1. Explode the nested `lineup` JSON array so each player is a separate row
--      - In Databricks SQL: LATERAL VIEW EXPLODE(from_json(lineup, ...))
--      - Or: EXPLODE(lineup) with proper schema definition
--   2. Extract player-level fields from each array element:
--      - player.player_id, player.player_name, player.player_nickname
--      - jersey_number
--      - positions array (player can change position during match)
--      - cards array (yellow/red cards received)
--   3. For positions: take the first position as the starting position
--      - positions[0].position_id, positions[0].position
--      - positions[0].from (timestamp), positions[0].to (timestamp)
--   4. Generate a surrogate key from match_id + team_id + player_id
--
-- Note: A player appears once per team per match, but may have multiple
-- positions if they were moved during the match (e.g. midfielder → forward).

with source as (

    select * from {{ source('statsbomb', 'statsbomb_lineups') }}

),

-- TODO: Explode the lineup JSON array into individual player rows
-- Example Databricks approach:
--   select
--       match_id,
--       team_id,
--       team_name,
--       player_element.*
--   from source
--   lateral view explode(from_json(lineup, 'array<struct<...>>')) as player_element

flattened as (

    select
        -- Surrogate key
        -- TODO: Replace with actual surrogate key generation once columns are populated
        -- {{ dbt_utils.generate_surrogate_key(['match_id', 'team_id', 'player_id']) }} as lineup_id,
        cast(null as string)                            as lineup_id,

        -- Match and team context
        match_id,
        team_id,
        team_name,

        -- Player info (extracted from exploded lineup array element)
        -- TODO: Extract from the exploded player struct
        cast(null as int)                               as player_id,
        cast(null as string)                            as player_name,
        cast(null as string)                            as player_nickname,
        cast(null as int)                               as jersey_number,

        -- Starting position (first element of the positions array)
        -- TODO: Extract positions[0].position_id and positions[0].position
        cast(null as int)                               as position_id,
        cast(null as string)                            as position_name,

        -- Cards summary
        -- TODO: Count cards from the cards array
        cast(null as int)                               as yellow_cards,
        cast(null as int)                               as red_cards

    from source

)

select * from flattened
