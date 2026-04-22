-- stg_statsbomb__lineups.sql
-- Clean lineup data from StatsBomb bronze layer.
--
-- The ingestion layer already exploded the lineup array, so each row
-- represents one player per team per match. This model extracts the
-- starting position from the positions JSON string and counts cards.

with source as (

    select * from {{ source('statsbomb', 'statsbomb_lineups') }}

),

-- Parse JSON columns once for reuse
parsed as (

    select
        *,
        from_json(
            positions,
            'ARRAY<STRUCT<position:STRING, position_id:INT, `from`:STRING, `to`:STRING>>'
        )                                                   as parsed_positions,
        from_json(cards, 'ARRAY<STRUCT<card_type:STRING>>')  as parsed_cards
    from source

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
        cast(player_id as int)                              as player_id,
        player_name,
        player_nickname,
        cast(jersey_number as int)                          as jersey_number,

        -- Starting position (first element of parsed positions array)
        get(parsed_positions, 0).position_id                as position_id,
        get(parsed_positions, 0).position                   as position_name,

        -- Cards summary
        coalesce(
            size(filter(parsed_cards, c -> c.card_type = 'Yellow Card')),
            0
        )                                                   as yellow_cards,
        coalesce(
            size(filter(parsed_cards, c -> c.card_type IN ('Red Card', 'Second Yellow'))),
            0
        )                                                   as red_cards,

        -- Bronze pass-through cols (PR 2 — Kimball migration, ADR-011).
        -- Surface remaining bronze cols with their bronze names so downstream
        -- models and analysts can reach them without re-reading bronze. The
        -- counts + casts above remain the preferred consumption path; these
        -- are the raw source-of-truth view. See
        -- src/tests/test_staging_coverage.py INITIAL_BRONZE_STAGING_GAPS.
        _ingested_at,
        cards,
        country,
        positions

    from parsed

)

select * from flattened
