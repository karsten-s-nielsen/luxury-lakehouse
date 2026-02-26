-- dim_players.sql
-- Player dimension table combining data from all sources.
--
-- Deduplication strategy:
--   Players may appear in multiple data sources (StatsBomb and Wyscout)
--   with different IDs. This model creates a canonical player record.
--   For Phase 1, we treat each source's player_id as distinct.
--   Future: implement fuzzy matching on name + team + position.
--
-- Grain: one row per unique player (per data source, until cross-source matching).

with statsbomb_players as (

    select distinct
        player_id,
        player_name,
        player_nickname,
        position_name                                   as primary_position,
        'statsbomb'                                     as data_source

    from {{ ref('stg_statsbomb__lineups') }}
    where player_id is not null

),

-- TODO: Add Wyscout player data once a player dimension source is available
-- Wyscout events have player_id but no player name in the events table.
-- Would need a separate player reference table from Wyscout.

-- TODO: Add Metrica player data (player IDs are anonymous: Player1, Player2, etc.)

final as (

    select
        player_id,
        player_name,
        -- Use nickname if available, otherwise full name
        coalesce(player_nickname, player_name)          as player_display_name,
        primary_position,
        data_source

    from statsbomb_players

)

select * from final
