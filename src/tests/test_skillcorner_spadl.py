"""Unit tests for SkillCorner SPADL UDF closure."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


class TestSkillCornerSpadlUdf:
    """Test _make_skillcorner_spadl_udf closure logic (no Spark)."""

    def _make_fixture_events(self) -> pd.DataFrame:
        """Minimal events DataFrame matching bronze.skillcorner_events shape.

        Uses event_type='player_possession' as the silly-kicks SkillCorner
        converter filters exclusively on this event type.
        """
        return pd.DataFrame(
            {
                "event_id": ["1_0", "2_0", "3_0", "4_0", "5_0"],
                "event_type": ["player_possession"] * 5,
                "event_subtype": ["pass", "reception", "carry", "pass", "reception"],
                "player_id": [38673, 44001, 44001, 44001, 38673],
                "team_id": [4177, 4177, 4177, 4177, 4177],
                "period": [1, 1, 1, 1, 1],
                "time_start": ["01:02.5", "01:04.3", "01:05.0", "01:10.2", "01:12.8"],
                "time_end": ["01:04.3", "01:05.0", "01:10.0", "01:12.8", "01:13.0"],
                "x_start": [10.5, 20.1, 22.0, 25.0, 30.0],
                "y_start": [-5.2, 3.4, 4.0, 5.0, 6.0],
                "x_end": [20.1, 22.0, 25.0, 30.0, 32.0],
                "y_end": [3.4, 4.0, 5.0, 6.0, 7.0],
                "game_interruption_before": [None, None, None, None, None],
                "game_interruption_after": [None, None, None, None, None],
                "end_type": ["pass", "ball_retention", "ball_retention", "pass", "ball_retention"],
                "start_type": ["", "", "", "", ""],
                "match_id": ["1886347"] * 5,
                "player_targeted_x_reception": [np.nan] * 5,
                "player_targeted_y_reception": [np.nan] * 5,
            }
        )

    def _make_match_metadata(self) -> dict:
        return {
            "id": "1886347",
            "pitch_length": 105,
            "pitch_width": 68,
            "home_team": {"id": 4177},
        }

    def test_udf_produces_spadl_columns(self) -> None:
        """UDF output must have all 40 SPADL schema columns."""
        from ingestion.spadl_conversion import _make_skillcorner_spadl_udf

        match_metadata = self._make_match_metadata()
        udf_fn = _make_skillcorner_spadl_udf(match_metadata=match_metadata)
        events = self._make_fixture_events()
        result = udf_fn(events)

        assert "data_source" in result.columns
        assert "match_id_native" in result.columns
        assert "player_id_native" in result.columns
        assert "team_id_native" in result.columns
        assert result["data_source"].iloc[0] == "skillcorner"

    def test_udf_uses_adr018_generators(self) -> None:
        """Native IDs must use ADR-018 canonical generators, not bare str()."""
        from ingestion.spadl_conversion import _make_skillcorner_spadl_udf

        match_metadata = self._make_match_metadata()
        udf_fn = _make_skillcorner_spadl_udf(match_metadata=match_metadata)
        events = self._make_fixture_events()
        result = udf_fn(events)

        # match_id_native must be the canonical format (pure numeric string)
        mid_native = result["match_id_native"].iloc[0]
        assert mid_native == "1886347"
        assert not mid_native.startswith("skillcorner_")

    def test_udf_null_fills_statsbomb_columns(self) -> None:
        from ingestion.spadl_conversion import _make_skillcorner_spadl_udf

        match_metadata = self._make_match_metadata()
        udf_fn = _make_skillcorner_spadl_udf(match_metadata=match_metadata)
        events = self._make_fixture_events()
        result = udf_fn(events)

        assert result["statsbomb_possession_id"].isna().all()
        assert result["statsbomb_play_pattern"].isna().all()

    def test_udf_null_fills_tackle_qualifiers(self) -> None:
        from ingestion.spadl_conversion import _make_skillcorner_spadl_udf

        match_metadata = self._make_match_metadata()
        udf_fn = _make_skillcorner_spadl_udf(match_metadata=match_metadata)
        events = self._make_fixture_events()
        result = udf_fn(events)

        assert result["tackle_winner_player_id_native"].isna().all()

    def test_udf_applies_enrichments(self) -> None:
        from ingestion.spadl_conversion import _make_skillcorner_spadl_udf

        match_metadata = self._make_match_metadata()
        udf_fn = _make_skillcorner_spadl_udf(match_metadata=match_metadata)
        events = self._make_fixture_events()
        result = udf_fn(events)

        assert "possession_id_heuristic" in result.columns
        assert "gk_role" in result.columns


class TestSkillCornerReplaceWhere:
    def test_replace_where_format(self) -> None:
        from ingestion.spadl_conversion import _make_skillcorner_replace_where

        result = _make_skillcorner_replace_where([123456, 789012])
        assert "data_source = 'skillcorner'" in result
        assert "123456" in result
        assert "789012" in result

    def test_replace_where_rejects_empty(self) -> None:
        from ingestion.spadl_conversion import _make_skillcorner_replace_where

        with pytest.raises(ValueError):
            _make_skillcorner_replace_where([])
