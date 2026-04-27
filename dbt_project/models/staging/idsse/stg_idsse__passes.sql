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
--   * end_x / end_y derived via two-tier strategy:
--     (a) PREFERRED: frame-based ball lookup (LEFT JOIN stg_idsse__tracking
--         on (match_id, period, end_frame=frame) to recover ball position
--         at end-of-pass). Activates when end_frame IS NOT NULL — currently
--         0% on the figshare CC-BY 4.0 research-tier release (verified
--         2026-04-27 via raw DFL XML inspection: Play XML elements in
--         research-tier files do not carry StartFrame/EndFrame despite
--         the schema documenting them); becomes the dominant path on
--         commercial DFL subscription data tiers that include the
--         frame attributes per the DFL_03_02 schema.
--     (b) FALLBACK: next-event start position. LEAD-window over all events
--         in the same (match, period) ordered by timestamp + event_id —
--         the SPADL convention used by socceraction et al. for providers
--         without explicit pass-end coords. Always available (~99%
--         coverage; NULL only at end-of-period boundary).
--     Combined via COALESCE — frame wins when populated, fallback fills
--     the gap. No re-migration needed when subscription data lands.
--
--   * is_progressive evaluated via the standard cross-provider rule
--     (distance_to_goal end < progressive_pass_ratio * distance_to_goal start)
--     applied to the derived end coords. NULL-preserving when both
--     mechanisms produce NULL (only at end-of-period boundary).

with source as (

    select * from {{ source('idsse', 'idsse_events') }}
    where event_type = 'Play'

),

events_with_native_match_id as (

    select
        *,
        regexp_replace(cast(match_id as string), '^idsse_', '') as native_match_id
    from source

),

-- PR 5a: hydrate real DFL TeamId via the new home_away bridge. DFL event
-- XML carries play_team as 'home'/'away' string only; the bridge resolves
-- (match_id, side) → DFL-CLU-XXXXXX by joining to tracking's per-frame
-- TeamId. Gives stg_idsse__passes a real team_id_native instead of the
-- useless 'home'/'away' value it previously carried.
hydrated as (

    select
        e.*,
        bridge.team_id                                          as bridge_team_id
    from events_with_native_match_id e
    left join {{ ref('stg_idsse__home_away_teams') }} bridge
        on bridge.match_id = e.native_match_id
       and bridge.side = lower(e.play_team)

),

-- PR 6 / PR 6-followup: ball position at the pass-end frame.
-- PREFERRED end-coord source when end_frame is populated (commercial DFL
-- subscription tiers). Tracking replicates ball_x/ball_y across per-player
-- rows for the same frame; DISTINCT collapses to one row per (match, period,
-- frame). On the figshare CC-BY 4.0 research-tier release this CTE produces
-- a useful row but bef.ball_x/ball_y are joined as NULL because end_frame
-- is itself NULL across all 5,381 Play rows there.
ball_at_end_frame as (

    select distinct
        regexp_replace(cast(match_id as string), '^idsse_', '') as match_id,
        cast(period as int)                                     as period,
        cast(frame as int)                                      as frame,
        ball_x,
        ball_y
    from {{ ref('stg_idsse__tracking') }}
    where ball_x is not null
      and ball_y is not null

),

-- PR 6-followup: next-event end coords (FALLBACK).
-- Standard SPADL convention used by socceraction / Wyscout-derived
-- pipelines for providers without explicit pass-end coords: the pass
-- ends at the start position of the chronologically next event in the
-- same (match, period). LEAD-window over ALL events (Play + non-Play).
-- Always available except at end-of-period (last event has no successor
-- in the partition). Coordinates normalized to 120x80 inline so the
-- COALESCE downstream uses a single coordinate convention.
events_with_next as (

    select
        cast(event_id as string)                                as event_id,
        lead({{ normalize_x('x', 'pitch_m') }}) over (
            partition by match_id, period
            order by timestamp_seconds, event_id
        )                                                       as next_event_x,
        lead({{ normalize_y('y', 'pitch_m') }}) over (
            partition by match_id, period
            order by timestamp_seconds, event_id
        )                                                       as next_event_y
    from {{ source('idsse', 'idsse_events') }}

),

-- PR 6: precompute normalized start + end coordinates so the final SELECT
-- can reference them by alias in the is_progressive CASE expression
-- without repeating the normalize_x / normalize_y macro expansion.
-- PR 6-followup: end coords are now COALESCE(frame_lookup, next_event_start) —
-- frame-based wins when populated, next-event-start fills the gap.
with_end_coords as (

    select
        h.*,
        {{ normalize_x('h.x', 'pitch_m') }}                     as _start_x_normalized,
        {{ normalize_y('h.y', 'pitch_m') }}                     as _start_y_normalized,
        coalesce(bef.ball_x, ene.next_event_x)                  as _end_x_normalized,
        coalesce(bef.ball_y, ene.next_event_y)                  as _end_y_normalized
    from hydrated h
    left join ball_at_end_frame bef
        on  bef.match_id = h.native_match_id
       and bef.period   = cast(h.period as int)
       and bef.frame    = cast(h.end_frame as int)
    left join events_with_next ene
        on  ene.event_id = cast(h.event_id as string)

),

final as (

    select
        cast(event_id as string)                                as event_id,

        -- Match id with 'idsse_' prefix already stripped in upstream CTE.
        native_match_id                                          as match_id,

        -- Canonical typed identity cols (INT FKs). NULL for IDSSE because
        -- DFL IDs are strings; int_unified_passes union needs INT.
        cast(null as int)                                       as player_id,
        cast(null as int)                                       as team_id,
        cast(null as int)                                       as pass_recipient_id,

        -- Raw DFL identity strings — surfaced for dim_teams / dim_players
        -- surrogate resolution (PR 5a) + lineage. DO NOT drop these.
        cast(play_player as string)                             as player_id_native,
        -- PR 5a: team_id_native now carries the REAL DFL TeamId from the
        -- bridge (was previously 'home' / 'away' — useless for xref).
        cast(bridge_team_id as string)                          as team_id_native,
        cast(play_recipient as string)                          as pass_recipient_id_native,
        cast(team as string)                                    as team_side,

        cast(period as int)                                     as period,
        cast(floor(timestamp_seconds / 60.0) as int)            as minute,
        cast(cast(timestamp_seconds as int) % 60 as int)        as second,

        -- Event coordinates: DFL pitch-origin metres → 120x80.
        -- PR 6: precomputed in with_end_coords; reuse here.
        _start_x_normalized                                     as start_x,
        _start_y_normalized                                     as start_y,
        _end_x_normalized                                       as end_x,
        _end_y_normalized                                       as end_y,

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

        -- PR 6 (ADR-011): is_progressive derived via ball-frame tracking
        -- lookup. NULL when end_frame is null OR the tracking lookup misses
        -- (preserves "unknown" semantics rather than false-positive).
        case
            when _end_x_normalized is null or _end_y_normalized is null
                then cast(null as boolean)
            else {{ distance_to_goal('_end_x_normalized', '_end_y_normalized') }}
                 < {{ var('progressive_pass_ratio') }}
                   * {{ distance_to_goal('_start_x_normalized', '_start_y_normalized') }}
        end                                                     as is_progressive,

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

    from with_end_coords

)

select * from final
