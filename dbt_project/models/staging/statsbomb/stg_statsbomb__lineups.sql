-- stg_statsbomb__lineups.sql
-- Clean lineup data from StatsBomb bronze layer.
--
-- The ingestion layer already exploded the lineup array, so each row
-- represents one player per team per match. This model extracts the
-- starting position from the positions JSON string and counts cards.

with source as (

    select * from {{ source('statsbomb', 'statsbomb_lineups') }}

),

flattened as (

    select
        -- Surrogate key (no team_id column; use team_name)
        {{ dbt_utils.generate_surrogate_key(['match_id', 'team_name', 'player_id']) }} as lineup_id,

        -- Match and team context
        match_id,
        competition_id,
        season_id,
        team_name,

        -- Player info (already flat columns)
        cast(player_id as int)                          as player_id,
        player_name,
        player_nickname,
        cast(jersey_number as int)                      as jersey_number,

        -- Starting position (first element of positions JSON array)
        -- Use get() to safely handle empty positions arrays
        get(
            from_json(
                positions,
                'ARRAY<STRUCT<position:STRING, position_id:INT, `from`:STRING, `to`:STRING>>'
            ),
            0
        ).position_id                                   as position_id,
        get(
            from_json(
                positions,
                'ARRAY<STRUCT<position:STRING, position_id:INT, `from`:STRING, `to`:STRING>>'
            ),
            0
        ).position                                      as position_name,

        -- Cards summary
        coalesce(
            size(filter(
                from_json(cards, 'ARRAY<STRUCT<card_type:STRING>>'),
                c -> c.card_type = 'Yellow Card'
            )),
            0
        )                                               as yellow_cards,
        coalesce(
            size(filter(
                from_json(cards, 'ARRAY<STRUCT<card_type:STRING>>'),
                c -> c.card_type IN ('Red Card', 'Second Yellow')
            )),
            0
        )                                               as red_cards

    from source

)

select * from flattened
