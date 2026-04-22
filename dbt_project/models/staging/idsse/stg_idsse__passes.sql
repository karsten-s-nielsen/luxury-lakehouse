-- stg_idsse__passes.sql
-- IDSSE Bundesliga pass events in SPADL-like shape, ready to union into
-- `int_unified_passes`.
--
-- Source: bronze.idsse_events WHERE event_type='Play'. DFL XML flattens
-- to a wide schema; every on-ball pass is carried by a <Play> outer
-- element with `play_evaluation` indicating outcome. Related event types
-- (FreeKick, ThrowIn, CornerKick, GoalKick, ShotAtGoal) are separate and
-- deliberately excluded here — they have their own event_type in bronze.
--
-- Coordinate system: DFL event XML records pitch-origin metres
-- (x ∈ 0-105, y ∈ 0-68). We scale to the shared 120x80 via `pitch_m`.
--
-- Bronze-completeness: every Play-event bronze column that carries
-- pass-relevant information is surfaced as a pass-through column so
-- downstream UI reviews can discover valuable additions without
-- re-ingesting or re-shaping the staging layer.
--
-- Known gaps (documented for follow-up PRs):
--
--   * team_id / player_id / pass_recipient_id are NULL in the canonical
--     INT form. Raw DFL string identifiers (team_id_native,
--     player_id_native, pass_recipient_id_native) are surfaced for
--     cross-provider reconciliation in a future PR (dim_teams /
--     dim_players surrogate keys akin to dim_matches.match_key).
--
--   * end_x / end_y NULL — the DFL <Play> row carries a start location
--     only. ELASTIC event-tracking sync (`stg_idsse__elastic_sync`,
--     pausa_enabled-gated) could enrich end coords; not in PR 2 scope.
--
--   * is_progressive = FALSE (requires end coordinates to evaluate).

with source as (

    select * from {{ source('idsse', 'idsse_events') }}
    where event_type = 'Play'

),

final as (

    select
        cast(event_id as string)                                as event_id,

        -- Strip the 'idsse_' prefix so native_match_id matches
        -- dim_matches.native_match_id (stg_idsse__matches already
        -- strips the same prefix).
        regexp_replace(cast(match_id as string), '^idsse_', '') as match_id,

        -- Canonical typed identity cols (INT FKs). NULL for IDSSE because
        -- DFL IDs are strings; int_unified_passes union needs INT.
        cast(null as int)                                       as player_id,
        cast(null as int)                                       as team_id,
        cast(null as int)                                       as pass_recipient_id,

        -- Raw DFL identity strings — surfaced for future cross-provider
        -- reconciliation + debugging. DO NOT drop these from staging.
        cast(play_player as string)                             as player_id_native,
        cast(play_team as string)                               as team_id_native,
        cast(play_recipient as string)                          as pass_recipient_id_native,
        cast(team as string)                                    as team_side,

        cast(period as int)                                     as period,
        cast(floor(timestamp_seconds / 60.0) as int)            as minute,
        cast(cast(timestamp_seconds as int) % 60 as int)        as second,

        -- Event coordinates: DFL pitch-origin metres → 120x80.
        {{ normalize_x('x', 'pitch_m') }}                       as start_x,
        {{ normalize_y('y', 'pitch_m') }}                       as start_y,
        cast(null as double)                                    as end_x,
        cast(null as double)                                    as end_y,

        -- Canonical pass-attribute cols (present on all providers in
        -- int_unified_passes).
        pass_direction                                          as pass_type,
        play_height                                             as pass_height,
        cast(null as string)                                    as body_part,
        cast(null as double)                                    as pass_length,
        radians(try_cast(play_play_angle as double))            as pass_angle_radians,

        case play_evaluation
            when 'successfullyCompleted' then 'Complete'
            when 'successful'            then 'Complete'
            when 'unsuccessful'          then 'Incomplete'
            else 'Unknown'
        end                                                     as pass_outcome,

        case
            when play_flat_cross is null then null
            when play_flat_cross = 'true' then true
            else false
        end                                                     as is_cross,
        cast(null as boolean)                                   as is_switch,
        pass_direction = 'throughBall'                          as is_through_ball,
        false                                                   as is_progressive,

        -- Bronze passthrough — Play-event prefixed attributes. Surfaced
        -- as-is so future UI reviews can spot uses without
        -- re-ingesting. Original DFL strings preserved.
        play_ball_possession_phase,
        play_distance,
        play_evaluation,
        play_flat_cross,
        play_from_open_play,
        play_goal_keeper_action,
        play_penalty_box,
        play_play_angle,
        play_play_origin,
        play_rotation,
        play_semi_field,

        -- Pass sub-attributes
        pass_direction,
        pass_free_kick_layup,
        pass_one_two,

        -- Cross sub-attributes (populated when the Play is a cross)
        cross_side,

        -- Source-timing / lineage cols (useful for event-tracking sync
        -- debugging and reconciliation against tracking frames)
        timestamp_seconds,
        start_frame,
        end_frame,
        calculated_frame,
        calculated_timestamp,
        event_time,
        match_id_raw,
        x_source_position,
        y_source_position,
        x_position_from_tracking,
        y_position_from_tracking,
        kickoff_team_left,
        kickoff_team_right,
        kickoff_game_section,

        'idsse'                                                 as data_source

    from source

)

select * from final
