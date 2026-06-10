"""S5 - schema parity tests for UDF DRY refactor.

Validates that refactored UDFs produce the same output schema (column
names, order, dtypes) as the pre-refactor versions.
"""

from __future__ import annotations

import pandas as pd
import pytest

from ingestion.spadl_conversion import (
    _make_idsse_spadl_udf,
    _make_metrica_spadl_udf,
    _make_sb_spadl_udf,
    _make_ws_spadl_udf,
)
from ingestion.spadl_udf_shared import apply_player_id_native

_EXPECTED_COLUMNS = [
    "game_id",
    "match_id",
    "original_event_id",
    "period_id",
    "time_seconds",
    "team_id",
    "player_id",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
    "type_id",
    "result_id",
    "bodypart_id",
    "action_id",
    "competition_id",
    "season_id",
    "data_source",
    "statsbomb_possession_id",
    "statsbomb_possession_team_id",
    "statsbomb_play_pattern",
    "statsbomb_under_pressure",
    "possession_id_heuristic",
    "gk_role",
    "gk_was_distributing",
    "gk_was_engaged",
    "gk_actions_in_possession",
    "defending_gk_player_id",
    "team_id_native",
    "home_team_id_native",
    "competition_native_id",
    "season_native_id",
    "match_id_native",
    "player_id_native",
    "tackle_winner_player_id_native",
    "tackle_winner_player_key",
    "tackle_winner_team_id_native",
    "tackle_winner_team_key",
    "tackle_loser_player_id_native",
    "tackle_loser_player_key",
    "tackle_loser_team_id_native",
    "tackle_loser_team_key",
    # silly-kicks 4.13.0 (sk ADR-018): is_synthetic provenance flag (manufactured
    # False on these non-GS providers; native bool on GS).
    "is_synthetic",
    # silly-kicks 4.21.0/4.22.0 (ADR-048): result_source (NA-manufactured on
    # non-SkillCorner) + restart-coordinate enrichment columns.
    "result_source",
    "enriched_start_x",
    "enriched_start_y",
    "enriched_end_x",
    "enriched_end_y",
    "start_coord_source",
    "end_coord_source",
    "start_coord_confidence",
    "end_coord_confidence",
]


@pytest.mark.parametrize(
    "source,factory",
    [
        ("statsbomb", _make_sb_spadl_udf),
        ("wyscout", _make_ws_spadl_udf),
        ("idsse", _make_idsse_spadl_udf),
        ("metrica", _make_metrica_spadl_udf),
    ],
)
def test_udf_empty_schema_matches_expected(source: str, factory) -> None:  # type: ignore[type-arg]
    """Empty-DataFrame path returns exactly the expected column set in order."""
    udf = factory()
    result = udf(pd.DataFrame())
    assert list(result.columns) == _EXPECTED_COLUMNS, (
        f"{source}: column mismatch.\n"
        f"  Missing: {set(_EXPECTED_COLUMNS) - set(result.columns)}\n"
        f"  Extra:   {set(result.columns) - set(_EXPECTED_COLUMNS)}"
    )


# ---------------------------------------------------------------------------
# apply_player_id_native — empty-string normalization
# ---------------------------------------------------------------------------


class TestApplyPlayerIdNative:
    """Verify empty-string player_id becomes pd.NA, not ''."""

    def test_statsbomb_nan_becomes_na(self) -> None:
        df = pd.DataFrame({"player_id": [5.0, float("nan")]})
        result = apply_player_id_native(df, source="statsbomb")
        assert result["player_id_native"].iloc[0] == "5"
        assert pd.isna(result["player_id_native"].iloc[1])

    def test_idsse_empty_string_becomes_na(self) -> None:
        df = pd.DataFrame({"player_id": ["DFL-OBJ-ABC", ""]})
        result = apply_player_id_native(df, source="idsse")
        assert result["player_id_native"].iloc[0] == "DFL-OBJ-ABC"
        assert pd.isna(result["player_id_native"].iloc[1])

    def test_metrica_empty_string_becomes_na(self) -> None:
        df = pd.DataFrame({"player_id": ["Player1", ""]})
        result = apply_player_id_native(df, source="metrica")
        assert result["player_id_native"].iloc[0] == "Player1"
        assert pd.isna(result["player_id_native"].iloc[1])

    def test_idsse_populated_values_pass_through(self) -> None:
        df = pd.DataFrame({"player_id": ["DFL-OBJ-ABC", "DFL-OBJ-XYZ"]})
        result = apply_player_id_native(df, source="idsse")
        assert result["player_id_native"].iloc[0] == "DFL-OBJ-ABC"
        assert result["player_id_native"].iloc[1] == "DFL-OBJ-XYZ"
