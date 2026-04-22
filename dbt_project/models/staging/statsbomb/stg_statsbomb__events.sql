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

        -- Pass attribute flags (already flat booleans in bronze — PR 1.5 expansion).
        pass_technique,
        pass_aerial_won,
        pass_goal_assist,
        pass_shot_assist,
        pass_cut_back,
        pass_deflected,
        pass_inswinging,
        pass_outswinging,
        pass_miscommunication,
        pass_backheel,

        -- Shot attribute flags (PR 1.5 expansion — previously only pass-through core).
        shot_aerial_won,
        shot_open_goal,
        shot_redirect,
        shot_follows_dribble,
        shot_saved_off_target,
        shot_saved_to_post,
        shot_kick_off,

        -- Dribble outcome flags (PR 1.5 expansion).
        dribble_overrun,
        dribble_no_touch,
        dribble_nutmeg,

        -- Player positional role for this event (e.g. 'Right Wing', 'Center Forward').
        -- Matches the player's `positions` entry in the lineup for the event time.
        position                                        as player_position,

        -- Substitution fields
        substitution_replacement_id,

        -- Tactics JSON (only non-null on 'Starting XI' / 'Tactical Shift' events).
        -- Structure: { formation: int, lineup: [{ player: {id, name}, position: {id, name}, jersey_number: int }...] }.
        -- PR 1.5: extract formation as a scalar col; lineup JSON pass-through
        -- (downstream can LATERAL VIEW EXPLODE it into per-player-position rows).
        from_json(tactics, 'STRUCT<formation: BIGINT>').formation as tactics_formation,
        tactics                                         as tactics_json,

        -- 50/50 duel JSON (present only on '50/50' event type). bronze col name
        -- is quoted because `50_50` isn't a valid SQL identifier without backticks.
        -- Structure: { outcome: {id, name}, counterpress: bool }.
        from_json(`50_50`, 'STRUCT<outcome: STRUCT<name: STRING>, counterpress: BOOLEAN>').outcome.name
                                                         as fifty_fifty_outcome,
        from_json(`50_50`, 'STRUCT<counterpress: BOOLEAN>').counterpress
                                                         as fifty_fifty_counterpress,

        -- Generic counterpress flag (from statsbombpy — distinct from the 50/50-specific counterpress above).
        counterpress                                    as is_counterpress,

        -- under_pressure / off_camera / out — already flat booleans in bronze.
        under_pressure,
        off_camera,
        out                                             as is_out_of_play,

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
        end                                             as card_name,

        -- Bronze pass-through cols (PR 2 — Kimball migration, ADR-011).
        -- Surface remaining bronze cols with their bronze names so downstream
        -- models and analysts can reach them without re-reading bronze. The
        -- type casts and renames above remain the preferred consumption path;
        -- these are the raw source-of-truth view for provenance + ad hoc
        -- exploration. See src/tests/test_staging_coverage.py
        -- INITIAL_BRONZE_STAGING_GAPS for the enforcement contract.
        `50_50`                                         as `50_50`,
        _ingested_at,
        _raw_extra_json,
        bad_behaviour_card,
        ball_receipt_outcome,
        ball_recovery_offensive,
        ball_recovery_recovery_failure,
        block_deflection,
        block_offensive,
        block_save_block,
        carry_end_location,
        clearance_aerial_won,
        clearance_body_part,
        clearance_head,
        clearance_left_foot,
        clearance_other,
        clearance_right_foot,
        counterpress,
        dribble_outcome,
        duel_outcome,
        duel_type,
        foul_committed_advantage,
        foul_committed_card,
        foul_committed_offensive,
        foul_committed_penalty,
        foul_committed_type,
        foul_won_advantage,
        foul_won_defensive,
        foul_won_penalty,
        goalkeeper_body_part,
        goalkeeper_end_location,
        goalkeeper_lost_in_play,
        goalkeeper_lost_out,
        goalkeeper_outcome,
        goalkeeper_penalty_saved_to_post,
        goalkeeper_position,
        goalkeeper_punched_out,
        goalkeeper_saved_to_post,
        goalkeeper_shot_saved_off_target,
        goalkeeper_shot_saved_to_post,
        goalkeeper_success_in_play,
        goalkeeper_success_out,
        goalkeeper_technique,
        goalkeeper_type,
        half_end_early_video_end,
        half_start_late_video_start,
        injury_stoppage_in_chain,
        interception_outcome,
        location,
        miscontrol_aerial_won,
        out,
        pass_assisted_shot_id,
        pass_no_touch,
        pass_recipient,
        pass_straight,
        player,
        player_off_permanent,
        position,
        possession_team,
        related_events,
        shot_deflected,
        shot_key_pass_id,
        substitution_outcome,
        substitution_outcome_id,
        substitution_replacement,
        tactics,
        team

    from source

)

select * from flattened
