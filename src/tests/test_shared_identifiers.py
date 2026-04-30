"""Unit tests for shared.identifiers — native ID format generators (ADR-018)."""

from __future__ import annotations

import pytest

from shared.identifiers import (
    idsse_native_competition_id,
    idsse_native_match_id,
    metrica_native_competition_id,
    metrica_native_match_id,
    metrica_native_season_id,
    metrica_native_team_id,
    statsbomb_native_match_id,
    wyscout_native_match_id,
)


class TestIdsseMatchId:
    def test_bare_dfl_id_passes_through(self) -> None:
        assert idsse_native_match_id("J03WMX") == "J03WMX"

    def test_alphanumeric_uppercase_passes(self) -> None:
        assert idsse_native_match_id("J03WR9") == "J03WR9"

    def test_prefixed_form_rejected(self) -> None:
        with pytest.raises(ValueError, match="bare DFL MatchId"):
            idsse_native_match_id("idsse_J03WMX")

    def test_lowercase_rejected(self) -> None:
        with pytest.raises(ValueError):
            idsse_native_match_id("j03wmx")

    def test_empty_rejected(self) -> None:
        with pytest.raises(ValueError):
            idsse_native_match_id("")


class TestIdsseCompetitionId:
    def test_dfl_com_format_passes(self) -> None:
        assert idsse_native_competition_id("DFL-COM-000001") == "DFL-COM-000001"

    def test_dfl_com_alphanumeric_passes(self) -> None:
        assert idsse_native_competition_id("DFL-COM-000002") == "DFL-COM-000002"

    def test_invalid_format_rejected(self) -> None:
        with pytest.raises(ValueError):
            idsse_native_competition_id("CL")

    def test_lowercase_rejected(self) -> None:
        with pytest.raises(ValueError):
            idsse_native_competition_id("dfl-com-000001")


class TestMetricaMatchId:
    def test_sample_game_format(self) -> None:
        assert metrica_native_match_id("Sample_Game_1") == "Sample_Game_1"

    def test_multi_digit_passes(self) -> None:
        assert metrica_native_match_id("Sample_Game_42") == "Sample_Game_42"

    def test_invalid_rejected(self) -> None:
        with pytest.raises(ValueError):
            metrica_native_match_id("game1")


class TestMetricaTeamId:
    def test_home_format(self) -> None:
        assert metrica_native_team_id("Sample_Game_1", "home") == "metrica_Sample_Game_1_home"

    def test_away_format(self) -> None:
        assert metrica_native_team_id("Sample_Game_3", "away") == "metrica_Sample_Game_3_away"

    def test_capital_side_rejected(self) -> None:
        with pytest.raises(ValueError, match="must be 'home' or 'away'"):
            metrica_native_team_id("Sample_Game_1", "Home")  # type: ignore[arg-type]

    def test_unknown_side_rejected(self) -> None:
        with pytest.raises(ValueError):
            metrica_native_team_id("Sample_Game_1", "neutral")  # type: ignore[arg-type]

    def test_invalid_match_id_rejected(self) -> None:
        with pytest.raises(ValueError):
            metrica_native_team_id("bad-match-id", "home")


class TestMetricaConstants:
    def test_competition_id_constant(self) -> None:
        assert metrica_native_competition_id() == "metrica-sample"

    def test_season_id_constant(self) -> None:
        assert metrica_native_season_id() == "metrica-open-2017"


class TestStatsBombMatchId:
    def test_positive_int_to_string(self) -> None:
        assert statsbomb_native_match_id(7298) == "7298"

    def test_zero_rejected(self) -> None:
        with pytest.raises(ValueError):
            statsbomb_native_match_id(0)

    def test_negative_rejected(self) -> None:
        with pytest.raises(ValueError):
            statsbomb_native_match_id(-1)


class TestWyscoutMatchId:
    def test_positive_int_to_string(self) -> None:
        assert wyscout_native_match_id(2576335) == "2576335"

    def test_zero_rejected(self) -> None:
        with pytest.raises(ValueError):
            wyscout_native_match_id(0)
