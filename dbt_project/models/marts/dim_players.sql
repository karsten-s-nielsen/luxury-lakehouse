-- dim_players.sql
-- Player dimension table combining data from all sources.
--
-- Deduplication strategy:
--   Players may appear in multiple matches with different positions or nicknames.
--   We pick one canonical record per player_id using the most recent lineup entry
--   (by match_id as a proxy for recency).
--
-- Grain: one row per unique player_id (per data source, until cross-source matching).

with statsbomb_players as (

    select
        player_id,
        player_name,
        player_nickname,
        position_name                                   as primary_position,
        'statsbomb'                                     as data_source,
        row_number() over (
            partition by player_id
            order by match_id desc
        )                                               as rn

    from {{ ref('stg_statsbomb__lineups') }}
    where player_id is not null

),

-- TODO: Add Wyscout player data once a player dimension source is available
-- Wyscout events have player_id but no player name in the events table.

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
    where rn = 1

)

select * from final
