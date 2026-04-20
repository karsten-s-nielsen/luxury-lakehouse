-- stg_statsbomb__events.sql
-- Flatten and clean raw StatsBomb event data from the bronze layer.
--
-- The ingestion layer (statsbombpy) already extracts most nested fields
-- into flat columns (team_id, player_id, shot_outcome, pass_type, etc.).
-- Main transformation: parse `location` JSON string to separate x/y.
--
-- StatsBomb coordinate system:
--   - Pitch is 120 x 80 yards
--   - Origin (0,0) is bottom-left when team attacks left to right
--   - x: 0 (own goal line) to 120 (opponent goal line)
--   - y: 0 (right touchline) to 80 (left touchline)

with source as (

    select * from {{ source('statsbomb', 'statsbomb_events') }}

),

flattened as (

    select
        -- Primary key
        id                                              as event_id,
        match_id,

        -- Competition / season context (from bronze; enables partitioned exports)
        cast(competition_id as int)                     as competition_id,
        cast(season_id as int)                          as season_id,

        -- Event classification (already flat strings from statsbombpy)
        type                                            as event_type,

        -- Temporal fields (already flat)
        period,
        minute,
        second,
        timestamp,

        -- Team and player (already extracted by ingestion)
        cast(team_id as int)                            as team_id,
        team                                            as team_name,
        cast(player_id as int)                          as player_id,
        player                                          as player_name,

        -- Location (parse JSON string "[x, y]" into separate columns)
        from_json(location, 'ARRAY<DOUBLE>')[0]         as location_x,
        from_json(location, 'ARRAY<DOUBLE>')[1]         as location_y,

        -- Possession context (already flat)
        possession,
        cast(possession_team_id as int)                 as possession_team_id,

        -- Play pattern (already a flat string)
        play_pattern,

        -- Duration (seconds the event lasted)
        duration,

        -- Index for ordering events within a possession sequence
        index,

        -- Shot-specific fields (pass-through for downstream shot/pass models)
        shot_end_location,
        shot_freeze_frame,
        shot_outcome,
        shot_technique,
        shot_body_part,
        shot_type,
        shot_statsbomb_xg,
        shot_first_time,
        shot_one_on_one,

        -- Pass-specific fields (pass-through for downstream pass models)
        pass_end_location,
        pass_type,
        pass_height,
        pass_body_part,
        pass_length,
        pass_angle,
        pass_outcome,
        pass_cross,
        pass_switch,
        pass_through_ball,

        -- Pass recipient (for pass network edges)
        cast(pass_recipient_id as int)                  as pass_recipient_id,
        pass_recipient                                  as pass_recipient_name,

        -- Substitution fields
        substitution_replacement_id,

        -- Discipline fields (extracted from _raw_extra_json, which statsbombpy
        -- does NOT flatten). Cards are issued on two event types:
        --   * Bad Behaviour  — direct misconduct (e.g. dissent, violent conduct)
        --   * Foul Committed — card issued as consequence of a foul
        -- Values observed in bronze: 'Yellow Card', 'Red Card', 'Second Yellow'.
        -- Downstream marts (e.g. fct_discipline_events) filter on card_name.
        case
            when type = 'Bad Behaviour' then
                from_json(_raw_extra_json,
                          'STRUCT<bad_behaviour: STRUCT<card: STRUCT<name: STRING>>>'
                         ).bad_behaviour.card.name
            when type = 'Foul Committed' then
                from_json(_raw_extra_json,
                          'STRUCT<foul_committed: STRUCT<card: STRUCT<name: STRING>>>'
                         ).foul_committed.card.name
        end                                             as card_name

    from source

)

select * from flattened
