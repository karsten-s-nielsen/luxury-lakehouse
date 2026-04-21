"""Cross-provider staging-coverage test.

Enforces the bronze-completeness-through-to-staging contract: every bronze
column documented in `_<provider>__sources.yml` must either:

  (a) appear in the staging model as-is,
  (b) appear in the staging model under a renamed identifier (``RENAMES``), or
  (c) be in ``INITIAL_BRONZE_STAGING_GAPS`` — the current-state snapshot of
      bronze cols the staging model deliberately or historically omits.

This is DOCUMENTATION-DRIFT detection, not SQL-level verification: it reads
the ``columns:`` entries in ``_<provider>__models.yml`` as the staging
contract. Staging SQL may surface additional cols; if they're not in
models.yml, they're invisible to this test. Future work: expand models.yml
entries to match actual staging output.

Drift detection:

  - New bronze col appears (DESCRIBE TABLE snapshot updated + sources.yml
    updated) but staging doesn't expose it → test FAILS. Remedy: either
    add the col to the staging model (and document in models.yml), or
    add it to ``INITIAL_BRONZE_STAGING_GAPS`` with a reason.
  - Bronze col removed from sources.yml → test still passes (not currently
    checked; add a "no-phantom-bronze-cols" check later).
"""

from __future__ import annotations

from pathlib import Path

import pytest

try:
    from coverage_utils import (
        load_bronze_cols_from_sources_yml,
        load_staging_cols_from_models_yml,
    )
except ImportError:  # pragma: no cover
    from tests.coverage_utils import (  # type: ignore[no-redef]
        load_bronze_cols_from_sources_yml,
        load_staging_cols_from_models_yml,
    )

_TEST_DIR = Path(__file__).parent
_DBT_STAGING = _TEST_DIR.parent.parent / "dbt_project" / "models" / "staging"


# Provider → list of (bronze_table, staging_model) pairs.
# Bronze tables without a staging model (e.g. ``statsbomb_competitions``,
# ``metrica_matches``) are omitted from this test; downstream marts read
# them directly.
PROVIDER_COVERAGE: dict[str, list[tuple[str, str]]] = {
    "idsse": [
        ("idsse_events", "stg_idsse__events"),
        ("idsse_tracking", "stg_idsse__tracking"),
    ],
    "skillcorner": [
        ("skillcorner_tracking", "stg_skillcorner__tracking"),
    ],
    "metrica": [
        ("metrica_events", "stg_metrica__events"),
        ("metrica_tracking", "stg_metrica__tracking"),
    ],
    "statsbomb": [
        ("statsbomb_events", "stg_statsbomb__events"),
        ("statsbomb_matches", "stg_statsbomb__matches"),
        ("statsbomb_lineups", "stg_statsbomb__lineups"),
        ("statsbomb_360", "stg_statsbomb__360"),
    ],
    "wyscout": [
        ("wyscout_events", "stg_wyscout__events"),
        # wyscout_matches: stg_wyscout__matches.sql exists but is not yet
        # documented in _wyscout__models.yml. Added to coverage when docs land.
        ("wyscout_players", "stg_wyscout__players"),
    ],
}


# Explicit bronze→staging column renames per table. Format:
#   (provider, bronze_table): {bronze_col: staging_col}
# Empty dicts indicate no renames — every bronze col keeps its name in staging.
RENAMES: dict[tuple[str, str], dict[str, str]] = {
    ("idsse", "idsse_events"): {},
    ("idsse", "idsse_tracking"): {"timestamp": "timestamp"},
    ("skillcorner", "skillcorner_tracking"): {},
    ("metrica", "metrica_events"): {},
    ("metrica", "metrica_tracking"): {},
    ("statsbomb", "statsbomb_events"): {"id": "event_id", "type": "event_type"},
    ("statsbomb", "statsbomb_matches"): {},
    ("statsbomb", "statsbomb_lineups"): {},
    ("statsbomb", "statsbomb_360"): {"id": "event_uuid"},
    ("wyscout", "wyscout_events"): {"eventId": "event_id", "subEventName": "sub_event_type"},
    ("wyscout", "wyscout_players"): {},
}


# Snapshot of the current bronze→staging gap (computed 2026-04-21 during PR 1.5).
# These cols are in bronze but not yet surfaced in the staging model's
# ``models.yml`` columns. Future PRs progressively move cols OUT of this set
# (by documenting them in models.yml after adding to staging SQL).
#
# Maintenance: when you intentionally leave a bronze col out of staging,
# add it here with a reason comment. When you add a bronze col to staging,
# remove it from this set AND add it to models.yml.
#
# NOTE: This set is large because many existing staging models.yml entries
# document only a subset of actual staging-SQL output cols. Progressive
# cleanup is the direction of travel.
INITIAL_BRONZE_STAGING_GAPS: dict[tuple[str, str], set[str]] = {
    # Many IDSSE bronze cols are produced by the PR 1.5 rewrite but aren't
    # yet surfaced by stg_idsse__events (which currently passes through
    # only the core 9 cols). PR 2 will expand stg_idsse__events to parse
    # Play/Pass/Cross into SPADL-like fields + shrink this set.
    ("idsse", "idsse_events"): {
        "calculated_frame",
        "calculated_timestamp",
        "end_frame",
        "event_time",
        "match_id_raw",
        "start_frame",
        "x_position_from_tracking",
        "x_source_position",
        "y_position_from_tracking",
        "y_source_position",
        # All first-child prefixed cols (Play/Shot/Tackle/etc.) — not yet
        # surfaced in staging. Listed compactly here; remove entries when
        # staging SQL starts surfacing them.
        *(
            f"{prefix}_{suffix}"
            for prefix, suffixes in {
                "caution": ("card_color card_rating other_reason player reason ref_decision_evaluation team").split(),
                "caution_official": ("card_color person_sent_off team").split(),
                "chance": (
                    "assist_action chance_assist chance_assist_type counter_attack player "
                    "prevention_goalkeeper setup_origin sitter situation taker_setup team"
                ).split(),
                "claim": "ball_possession_phase player team type".split(),
                "corner": "decision_timestamp placing post_marking rotation side target_area team".split(),
                "cross": "goal_keeper goal_keeper_interference side".split(),
                "deflection": "player team type".split(),
                "delete": ["reason"],
                "fairplay": "ball_possession_phase player team".split(),
                "foul": "committing_player_action foul_type fouled fouler team_fouled team_fouler".split(),
                "freekick": "decision_timestamp execution_mode team".split(),
                "goaldis": "player reason ref_decision_evaluation team".split(),
                "goalkick": "decision_timestamp team".split(),
                "kickoff": "game_section team_left team_right".split(),
                "not_sent_off": "player reason ref_decision_evaluation team type".split(),
                "nutmeg": "affected_player affected_team player team".split(),
                "offside": "player team".split(),
                "other_action": (
                    "change_contingent_exhausted change_of_captain player player_becomes_goalkeeper team"
                ).split(),
                "otherball": "ball_possession_phase defensive_clearance player team".split(),
                "pass": "direction free_kick_layup one_two".split(),
                "penalty": (
                    "causing_player decision_timestamp fouled_player goalkeeper_behaviour goalkeeper_movement "
                    "players_in_box prospective_taker ref_decision_evaluation retaken_penalty team"
                ).split(),
                "penalty_not": ("causing_player player_to_be_awarded reason ref_decision_evaluation team").split(),
                "play": (
                    "ball_possession_phase distance evaluation flat_cross from_open_play goal_keeper_action "
                    "height penalty_box play_angle play_origin player recipient rotation semi_field team"
                ).split(),
                "possloss": "player possession_loss_origin team type_of_possession_loss".split(),
                "run": "player team".split(),
                "shot": (
                    "after_free_kick amount_of_defenders angle_to_goal assist_action assist_shot_at_goal "
                    "assist_type_shot_at_goal ball_possession_phase build_up chance_evaluation counter_attack "
                    "distance_to_goal extended_type_of_shot goal_distance_goalkeeper inside_box outcome_type "
                    "player player_speed pressure setup_origin shot_condition shot_contribution shot_origin "
                    "significance_evaluation sitter_contribution taker_ball_control taker_setup team type_of_shot x_g"
                ).split(),
                "sitter_prev": "player reason ref_decision_evaluation team".split(),
                "spectacular": "player team type".split(),
                "sub": "player_in player_out playing_position team".split(),
                "tackle": (
                    "ball_possession_phase dribble_evaluation dribbling_side dribbling_type goal_keeper_involved "
                    "loser loser_role loser_team possession_change type winner winner_action winner_result "
                    "winner_role winner_team"
                ).split(),
                "throwin": "decision_timestamp side team".split(),
                "var": (
                    "final_decision linesman1 linesman2 opponent_team proofed_event ref_decision "
                    "ref_decision_evaluation referee refereein_rra team_challenged timestamp_end_action "
                    "timestamp_start_action video_assistant"
                ).split(),
                "whistle": "breaking_off final_result game_section".split(),
            }.items()
            for suffix in suffixes
        ),
    },
    # idsse_tracking: bronze has `timestamp`; staging models.yml doesn't
    # currently document it, though the SQL produces it. Documentation gap.
    ("idsse", "idsse_tracking"): {"timestamp"},
    ("skillcorner", "skillcorner_tracking"): {
        "away_team_id",
        "ball_owning_team_id",
        "ball_state",
        "ball_z",
        "home_team_id",
        "is_goalkeeper",
        "is_visible",
        "position_name",
        "timestamp",  # SQL produces it but models.yml doesn't list it
        # PR 1.5 added most of these to the bronze parser but staging
        # models.yml hasn't yet been expanded to document them.
    },
    ("metrica", "metrica_events"): {
        "end_time_s",
        "pitch_length_m",
        "pitch_width_m",
        "player",
        "start_time_s",
        "subtype",
        "subtypes_all_json",
        "to",
        "type",
    },
    ("metrica", "metrica_tracking"): {
        "away_players",
        "gk_jersey_numbers",
        "home_players",
        "pitch_length_m",
        "pitch_width_m",
        "timestamp",  # SQL produces it but models.yml doesn't list it
    },
    # StatsBomb: the expanded sources.yml (PR 1.5) documents 126 bronze
    # cols; models.yml for stg_statsbomb__events documents only the core
    # subset. Staging SQL output includes more than models.yml does —
    # progressive documentation cleanup is future work.
    ("statsbomb", "statsbomb_events"): {
        "50_50",
        "_ingested_at",
        "_raw_extra_json",
        "bad_behaviour_card",
        "ball_receipt_outcome",
        "ball_recovery_offensive",
        "ball_recovery_recovery_failure",
        "block_deflection",
        "block_offensive",
        "block_save_block",
        "carry_end_location",
        "clearance_aerial_won",
        "clearance_body_part",
        "clearance_head",
        "clearance_left_foot",
        "clearance_other",
        "clearance_right_foot",
        "counterpress",
        "dribble_no_touch",
        "dribble_nutmeg",
        "dribble_outcome",
        "dribble_overrun",
        "duel_outcome",
        "duel_type",
        "foul_committed_advantage",
        "foul_committed_card",
        "foul_committed_offensive",
        "foul_committed_penalty",
        "foul_committed_type",
        "foul_won_advantage",
        "foul_won_defensive",
        "foul_won_penalty",
        "goalkeeper_body_part",
        "goalkeeper_end_location",
        "goalkeeper_lost_in_play",
        "goalkeeper_lost_out",
        "goalkeeper_outcome",
        "goalkeeper_penalty_saved_to_post",
        "goalkeeper_position",
        "goalkeeper_punched_out",
        "goalkeeper_saved_to_post",
        "goalkeeper_shot_saved_off_target",
        "goalkeeper_shot_saved_to_post",
        "goalkeeper_success_in_play",
        "goalkeeper_success_out",
        "goalkeeper_technique",
        "goalkeeper_type",
        "half_end_early_video_end",
        "half_start_late_video_start",
        "injury_stoppage_in_chain",
        "interception_outcome",
        "location",
        "miscontrol_aerial_won",
        "off_camera",
        "out",
        "pass_aerial_won",
        "pass_assisted_shot_id",
        "pass_backheel",
        "pass_cut_back",
        "pass_deflected",
        "pass_goal_assist",
        "pass_inswinging",
        "pass_miscommunication",
        "pass_no_touch",
        "pass_outswinging",
        "pass_recipient",
        "pass_shot_assist",
        "pass_straight",
        "pass_technique",
        "player",
        "player_off_permanent",
        "position",
        "possession_team",
        "related_events",
        "shot_aerial_won",
        "shot_deflected",
        "shot_follows_dribble",
        "shot_key_pass_id",
        "shot_kick_off",
        "shot_open_goal",
        "shot_redirect",
        "shot_saved_off_target",
        "shot_saved_to_post",
        "substitution_outcome",
        "substitution_outcome_id",
        "substitution_replacement",
        "tactics",
        "team",
        "under_pressure",
    },
    ("statsbomb", "statsbomb_matches"): {
        "_ingested_at",
        "away_managers",
        "away_team",
        "competition",
        "home_managers",
        "home_team",
        "kick_off",
        "last_updated",
        "last_updated_360",
        "match_status_360",
        "referee",
        "season",
        "shot_fidelity_version",
        "stadium",
        "xy_fidelity_version",
    },
    ("statsbomb", "statsbomb_lineups"): {
        "_ingested_at",
        "cards",
        "country",
        "positions",
    },
    ("statsbomb", "statsbomb_360"): {
        "_ingested_at",
        "actor",
        "keeper",
        "location",
        "teammate",
        "visible_area",
    },
    ("wyscout", "wyscout_events"): {
        "_ingested_at",
        "competition_name",
        "eventName",
        "eventSec",
        "id",
        "matchId",
        "matchPeriod",
        "playerId",
        "positions",
        "subEventId",
        "tags",
        "teamId",
    },
    ("wyscout", "wyscout_players"): {
        "_ingested_at",
        "birthArea",
        "birthDate",
        "currentNationalTeamId",
        "currentTeamId",
        "firstName",
        "height",
        "lastName",
        "middleName",
        "passportArea",
        "role",
        "shortName",
        "weight",
        "wyId",
    },
}


def _all_params() -> list[tuple[str, str, str]]:
    """Flatten PROVIDER_COVERAGE into (provider, bronze_table, staging_model) tuples."""
    return [
        (provider, bronze_table, staging_model)
        for provider, pairs in PROVIDER_COVERAGE.items()
        for bronze_table, staging_model in pairs
    ]


class TestStagingCoverage:
    """Every bronze col is either preserved, renamed, or in INITIAL_BRONZE_STAGING_GAPS."""

    @pytest.mark.parametrize(
        ("provider", "bronze_table", "staging_model"),
        _all_params(),
    )
    def test_bronze_col_coverage(self, provider: str, bronze_table: str, staging_model: str) -> None:
        sources_yml = _DBT_STAGING / provider / f"_{provider}__sources.yml"
        models_yml = _DBT_STAGING / provider / f"_{provider}__models.yml"

        bronze_cols = load_bronze_cols_from_sources_yml(sources_yml, bronze_table)
        staging_cols = load_staging_cols_from_models_yml(models_yml, staging_model)

        renames = RENAMES.get((provider, bronze_table), {})
        gaps = INITIAL_BRONZE_STAGING_GAPS.get((provider, bronze_table), set())

        # Cols we expect to see downstream: bronze minus gaps, with renames applied.
        to_verify = bronze_cols - gaps
        expected_staging_names = {renames.get(c, c) for c in to_verify}
        missing = expected_staging_names - staging_cols

        assert not missing, (
            f"[{provider}.{bronze_table}] {len(missing)} bronze col(s) not "
            f"preserved, renamed, or in INITIAL_BRONZE_STAGING_GAPS:\n"
            f"  {sorted(missing)}\n"
            "Fix: either (a) carry through in staging SQL + document in "
            f"_{provider}__models.yml, (b) add to RENAMES if renamed, or\n"
            f"(c) add to INITIAL_BRONZE_STAGING_GAPS[('{provider}', "
            f"'{bronze_table}')] with a reason."
        )


class TestCoverageInvariants:
    """Invariants on the RENAMES + GAPS config itself."""

    def test_every_provider_covered(self) -> None:
        """Every provider in PROVIDER_COVERAGE has matching sources + models yml files."""
        for provider, pairs in PROVIDER_COVERAGE.items():
            sources_yml = _DBT_STAGING / provider / f"_{provider}__sources.yml"
            models_yml = _DBT_STAGING / provider / f"_{provider}__models.yml"
            assert sources_yml.exists(), f"missing {sources_yml}"
            assert models_yml.exists(), f"missing {models_yml}"
            assert pairs, f"PROVIDER_COVERAGE['{provider}'] is empty"

    def test_renames_keys_match_coverage_keys(self) -> None:
        """Every (provider, bronze_table) in PROVIDER_COVERAGE has a RENAMES entry."""
        coverage_keys = {(p, t) for p, pairs in PROVIDER_COVERAGE.items() for t, _ in pairs}
        extra = set(RENAMES.keys()) - coverage_keys
        assert not extra, f"RENAMES has keys not in PROVIDER_COVERAGE: {extra}"
