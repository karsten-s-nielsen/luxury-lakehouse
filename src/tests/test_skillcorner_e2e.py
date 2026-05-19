"""End-to-end integration test for SkillCorner ingestion pipeline (no Spark).

Tests the full flow: parse events/tracking/matches -> SPADL conversion.
Uses fixture subsets of match 1886347.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "skillcorner"


@pytest.fixture
def match_metadata() -> dict:
    with open(_FIXTURE_DIR / "match.json") as f:
        return json.load(f)


@pytest.fixture
def events_df() -> pd.DataFrame:
    return pd.read_csv(_FIXTURE_DIR / "events_subset.csv", low_memory=False)


@pytest.fixture
def tracking_df() -> pd.DataFrame:
    from ingestion.skillcorner_tracking import parse_tracking_jsonl

    return parse_tracking_jsonl(str(_FIXTURE_DIR / "tracking_subset.jsonl"), match_id="1886347")


@pytest.fixture
def matches_df(match_metadata: dict) -> pd.DataFrame:
    from ingestion.skillcorner_matches import parse_match_json

    return parse_match_json(json.dumps(match_metadata), match_id="1886347")


class TestSkillCornerE2E:
    def test_events_parse(self, events_df: pd.DataFrame) -> None:
        assert len(events_df) > 0
        assert "event_id" in events_df.columns
        assert "event_type" in events_df.columns

    def test_tracking_parse(self, tracking_df: pd.DataFrame) -> None:
        assert len(tracking_df) > 0
        assert "player_id" in tracking_df.columns
        assert "timestamp" in tracking_df.columns
        assert tracking_df["timestamp"].dtype == "Float64"
        assert "is_visible" in tracking_df.columns
        assert "is_detected" not in tracking_df.columns

    def test_matches_parse(self, matches_df: pd.DataFrame) -> None:
        assert len(matches_df) > 0
        assert "player_id" in matches_df.columns
        assert "position_name" in matches_df.columns
        assert "pitch_length" in matches_df.columns

    def test_spadl_conversion(self, events_df: pd.DataFrame, match_metadata: dict) -> None:
        import silly_kicks.spadl.skillcorner as sc

        events_df["match_id"] = "1886347"
        actions, _report = sc.convert_to_actions(events_df, match_metadata)

        assert len(actions) > 0
        assert "type_id" in actions.columns
        assert "start_x" in actions.columns

        # Apply enrichments
        from ingestion.spadl_enrichments import apply_spadl_enrichments

        enriched = apply_spadl_enrichments(actions, source="skillcorner")
        assert "possession_id_heuristic" in enriched.columns
        assert "gk_role" in enriched.columns

    def test_identity_columns(self, events_df: pd.DataFrame, match_metadata: dict) -> None:
        """SPADL actions must have correct identity columns after full UDF logic."""
        import silly_kicks.spadl.skillcorner as sc

        events_df["match_id"] = "1886347"
        actions, _ = sc.convert_to_actions(events_df, match_metadata)

        from shared.identifiers import skillcorner_native_match_id, skillcorner_native_team_id

        # Verify native ID generators work on actual data
        for tid in actions["team_id"].dropna().unique():
            validated = skillcorner_native_team_id(tid)
            assert validated == str(tid)

        validated_mid = skillcorner_native_match_id("1886347")
        assert validated_mid == "1886347"

    def test_matches_playing_time_fields(self, matches_df: pd.DataFrame) -> None:
        """parse_match_json must preserve all playing_time fields from match.json."""
        required_cols = [
            "minutes_played",
            "start_frame",
            "end_frame",
            "minutes_tip",
            "minutes_otip",
            "start_time",
            "end_time",
            "yellow_card",
            "red_card",
            "injured",
            "goal",
            "own_goal",
            "trackable_object",
            "birthday",
            "gender",
            "team_player_id",
        ]
        for col in required_cols:
            assert col in matches_df.columns, f"Missing column: {col}"

        # Spot-check first player's minutes_played is non-null and reasonable
        assert matches_df["minutes_played"].notna().any()
        assert (matches_df["minutes_played"].dropna() >= 0).all()
        assert (matches_df["minutes_played"].dropna() <= 130).all()
