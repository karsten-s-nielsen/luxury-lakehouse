-- stg_idsse__events.sql
-- Normalize IDSSE Bundesliga event data to the shared 120×80 coordinate system
-- AND surface every bronze column as a pass-through (bronze-completeness sweep,
-- PR 2 of the Kimball migration).
--
-- Coordinate system alignment:
--   IDSSE events (DFL): pitch-origin meters, x ∈ (0, 105), y ∈ (0, 68)
--   (Note: tracking uses center-origin; events use pitch-origin per DFL XML spec)
--   Target: (0,0) = bottom-left, (120,80) = top-right (StatsBomb system)
--   Transform: x_out = x / 105.0 * 120.0
--              y_out = y / 68.0 * 80.0
--
-- Reference: Bassek et al. (2025), Scientific Data, Nature. CC-BY 4.0.
-- Enabled by pausa_enabled toggle (events consumed by ELASTIC sync → PAUSA pipeline).
--
-- Column policy:
--   * The 9 "core" bronze cols (match_id, event_id, event_type, period,
--     timestamp_seconds, player_id, team, x, y) are typed/scaled as before.
--   * A synthetic `event_sk` surrogate + `source_provider = 'idsse'` label
--     are added for cross-provider conformance.
--   * Every other bronze col (first-child prefixed attrs, nested-child
--     prefixed attrs, tracking-context attrs, MatchId/EventTime audit attrs)
--     flows through verbatim, no cast or transform. Downstream marts pick
--     what they need; bronze-completeness is preserved.

{{ config(enabled=var('pausa_enabled', false)) }}

with source as (

    select * from {{ source('idsse', 'idsse_events') }}

),

normalized as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['match_id', 'event_id']) }} as event_sk,

        -- Match context
        match_id,

        -- Event identity
        event_id,
        event_type,

        -- Timing
        timestamp_seconds,
        cast(period as int)                             as period,

        -- Player identity
        player_id,
        team,

        -- Source provider
        'idsse'                                         as source_provider,

        -- Scaled event coordinates (120×80) — events use pitch-origin (0-105, 0-68)
        {{ normalize_x('x', 'pitch_m') }} as x,
        {{ normalize_y('y', 'pitch_m') }} as y,

        -- ------------------------------------------------------------------
        -- Bronze passthrough — tracking-context + audit attrs from <Event>
        -- ------------------------------------------------------------------
        calculated_frame,
        calculated_timestamp,
        end_frame,
        event_time,
        match_id_raw,
        start_frame,
        x_position_from_tracking,
        x_source_position,
        y_position_from_tracking,
        y_source_position,

        -- ------------------------------------------------------------------
        -- Bronze passthrough — first-child prefixed attrs (BallClaiming)
        -- ------------------------------------------------------------------
        claim_ball_possession_phase,
        claim_player,
        claim_team,
        claim_type,

        -- BallDeflection
        deflection_player,
        deflection_team,
        deflection_type,

        -- Caution
        caution_card_color,
        caution_card_rating,
        caution_other_reason,
        caution_player,
        caution_reason,
        caution_ref_decision_evaluation,
        caution_team,

        -- CautionTeamofficial
        caution_official_card_color,
        caution_official_person_sent_off,
        caution_official_team,

        -- ChanceWithoutShot
        chance_assist_action,
        chance_chance_assist,
        chance_chance_assist_type,
        chance_counter_attack,
        chance_player,
        chance_prevention_goalkeeper,
        chance_setup_origin,
        chance_sitter,
        chance_situation,
        chance_taker_setup,
        chance_team,

        -- CornerKick
        corner_decision_timestamp,
        corner_placing,
        corner_post_marking,
        corner_rotation,
        corner_side,
        corner_target_area,
        corner_team,

        -- Delete
        delete_reason,

        -- FairPlay
        fairplay_ball_possession_phase,
        fairplay_player,
        fairplay_team,

        -- FinalWhistle
        whistle_breaking_off,
        whistle_final_result,
        whistle_game_section,

        -- Foul
        foul_committing_player_action,
        foul_foul_type,
        foul_fouled,
        foul_fouler,
        foul_team_fouled,
        foul_team_fouler,

        -- FreeKick
        freekick_decision_timestamp,
        freekick_execution_mode,
        freekick_team,

        -- GoalDisallowed
        goaldis_player,
        goaldis_reason,
        goaldis_ref_decision_evaluation,
        goaldis_team,

        -- GoalKick
        goalkick_decision_timestamp,
        goalkick_team,

        -- KickOff
        kickoff_game_section,
        kickoff_team_left,
        kickoff_team_right,

        -- Nutmeg
        nutmeg_affected_player,
        nutmeg_affected_team,
        nutmeg_player,
        nutmeg_team,

        -- Offside
        offside_player,
        offside_team,

        -- OtherBallAction
        otherball_ball_possession_phase,
        otherball_defensive_clearance,
        otherball_player,
        otherball_team,

        -- OtherPlayerAction
        other_action_change_contingent_exhausted,
        other_action_change_of_captain,
        other_action_player,
        other_action_player_becomes_goalkeeper,
        other_action_team,

        -- Penalty
        penalty_causing_player,
        penalty_decision_timestamp,
        penalty_fouled_player,
        penalty_goalkeeper_behaviour,
        penalty_goalkeeper_movement,
        penalty_players_in_box,
        penalty_prospective_taker,
        penalty_ref_decision_evaluation,
        penalty_retaken_penalty,
        penalty_team,

        -- PenaltyNotAwarded
        penalty_not_causing_player,
        penalty_not_player_to_be_awarded,
        penalty_not_reason,
        penalty_not_ref_decision_evaluation,
        penalty_not_team,

        -- Play
        play_ball_possession_phase,
        play_distance,
        play_evaluation,
        play_flat_cross,
        play_from_open_play,
        play_goal_keeper_action,
        play_height,
        play_penalty_box,
        play_play_angle,
        play_play_origin,
        play_player,
        play_recipient,
        play_rotation,
        play_semi_field,
        play_team,

        -- PlayerNotSentOff
        not_sent_off_player,
        not_sent_off_reason,
        not_sent_off_ref_decision_evaluation,
        not_sent_off_team,
        not_sent_off_type,

        -- PossessionLossBeforeGoal
        possloss_player,
        possloss_possession_loss_origin,
        possloss_team,
        possloss_type_of_possession_loss,

        -- Run
        run_player,
        run_team,

        -- ShotAtGoal
        shot_after_free_kick,
        shot_amount_of_defenders,
        shot_angle_to_goal,
        shot_assist_action,
        shot_assist_shot_at_goal,
        shot_assist_type_shot_at_goal,
        shot_ball_possession_phase,
        shot_build_up,
        shot_chance_evaluation,
        shot_counter_attack,
        shot_distance_to_goal,
        shot_extended_type_of_shot,
        shot_goal_distance_goalkeeper,
        shot_inside_box,
        shot_player,
        shot_player_speed,
        shot_pressure,
        shot_setup_origin,
        shot_shot_condition,
        shot_shot_contribution,
        shot_shot_origin,
        shot_significance_evaluation,
        shot_sitter_contribution,
        shot_taker_ball_control,
        shot_taker_setup,
        shot_team,
        shot_type_of_shot,
        shot_x_g,

        -- SitterPrevented
        sitter_prev_player,
        sitter_prev_reason,
        sitter_prev_ref_decision_evaluation,
        sitter_prev_team,

        -- SpectacularPlay
        spectacular_player,
        spectacular_team,
        spectacular_type,

        -- Substitution
        sub_player_in,
        sub_player_out,
        sub_playing_position,
        sub_team,

        -- TacklingGame
        tackle_ball_possession_phase,
        tackle_dribble_evaluation,
        tackle_dribbling_side,
        tackle_dribbling_type,
        tackle_goal_keeper_involved,
        tackle_loser,
        tackle_loser_role,
        tackle_loser_team,
        tackle_possession_change,
        tackle_type,
        tackle_winner,
        tackle_winner_action,
        tackle_winner_result,
        tackle_winner_role,
        tackle_winner_team,

        -- ThrowIn
        throwin_decision_timestamp,
        throwin_side,
        throwin_team,

        -- VideoAssistantAction
        var_final_decision,
        var_linesman1,
        var_linesman2,
        var_opponent_team,
        var_proofed_event,
        var_ref_decision,
        var_ref_decision_evaluation,
        var_referee,
        var_refereein_rra,
        var_team_challenged,
        var_timestamp_end_action,
        var_timestamp_start_action,
        var_video_assistant,

        -- ------------------------------------------------------------------
        -- Bronze passthrough — nested-child prefixed attrs
        -- ------------------------------------------------------------------
        -- Cross (nested inside Play)
        cross_goal_keeper,
        cross_goal_keeper_interference,
        cross_side,

        -- Pass (nested inside Play)
        pass_direction,
        pass_free_kick_layup,
        pass_one_two,

        -- ShotAtGoal outcome disambiguator (synthetic bronze col — populated
        -- by the IDSSE parser from the mutually-exclusive outcome nested
        -- tags: SuccessfulShot / SavedShot / ShotWide / ShotWoodWork /
        -- BlockedShot / OtherShot).
        shot_outcome_type,
        -- ShotAtGoal outcome detail (nested under the outcome tags disambiguated by
        -- shot_outcome_type above). Bronze passthrough — surfaced so the contract is
        -- complete; no mart reads these yet.
        shot_outcome_assist,
        shot_outcome_assist_contribution,
        shot_outcome_assist_fouled_player,
        shot_outcome_assist_type,
        shot_outcome_blocked_by_own_team,
        shot_outcome_current_result,
        shot_outcome_deflection_keeper,
        shot_outcome_deflection_player,
        shot_outcome_error,
        shot_outcome_goal_keeper,
        shot_outcome_goal_prevented,
        shot_outcome_goal_zone,
        shot_outcome_location,
        shot_outcome_pitch_marking,
        shot_outcome_placing,
        shot_outcome_player,
        shot_outcome_ref_decision_evaluation,
        shot_outcome_save_evaluation,
        shot_outcome_save_result,
        shot_outcome_save_type,
        shot_outcome_solo,

        -- Shot execution / set-piece detail (nested under ShotAtGoal).
        shot_direct_free_kick_intention,
        shot_penalty_direction,
        shot_penalty_execution,
        shot_rotation,
        shot_shot_assist_fouled_player,

        -- FaultExecution (nested under the foul event).
        fault_execution_ball_possession_phase,
        fault_execution_player,
        fault_execution_team,

        -- Native provider identifiers (DFL CLU/competition/season strings). The Kimball
        -- surrogates are resolved downstream; these carry the raw ids per ADR-016.
        home_team_id_native,
        away_team_id_native,
        team_id_native,
        competition_native_id,
        season_native_id,

        -- Audit column.
        _ingested_at

    from source
    where x is not null
      and y is not null

)

select * from normalized
