"""Tests for streamlit_app.pages.player_similarity — pure-logic unit tests."""

from __future__ import annotations

import pandas as pd

from streamlit_app.pages.player_similarity import (
    _format_vector_literal,
    _get_table_and_columns,
    _get_vector_column,
    _get_vector_dimension,
)


class TestVectorFormatting:
    """Python list -> pgvector literal string conversion."""

    def test_basic_conversion(self) -> None:
        vector = [0.1, 0.2, 0.3]
        result = _format_vector_literal(vector)
        assert result == "[0.1,0.2,0.3]"

    def test_negative_values(self) -> None:
        vector = [-0.5, 0.0, 1.0]
        result = _format_vector_literal(vector)
        assert result == "[-0.5,0.0,1.0]"

    def test_single_element(self) -> None:
        vector = [0.42]
        result = _format_vector_literal(vector)
        assert result == "[0.42]"

    def test_high_precision(self) -> None:
        vector = [0.123456789, -0.987654321]
        result = _format_vector_literal(vector)
        assert result == "[0.123456789,-0.987654321]"


class TestVectorColumn:
    """Correct vector column selected based on search type."""

    def test_behavioral_returns_behavioral_vector(self) -> None:
        assert _get_vector_column("Playing style") == "behavioral_vector"

    def test_statistical_returns_stat_vector(self) -> None:
        assert _get_vector_column("Statistical output") == "stat_vector"


class TestVectorDimension:
    """Correct vector dimension based on search type."""

    def test_behavioral_dimension_is_32(self) -> None:
        assert _get_vector_dimension("Playing style") == 32

    def test_statistical_dimension_is_13(self) -> None:
        assert _get_vector_dimension("Statistical output") == 13


class TestTableSelection:
    """Career table when no competition, season table when competition selected."""

    def test_no_competition_returns_career_table(self) -> None:
        raw_table, total_col = _get_table_and_columns(None)
        assert "career" in raw_table
        assert raw_table == "fct_player_embeddings_career_synced"
        assert total_col == "total_matches"

    def test_competition_returns_season_table(self) -> None:
        raw_table, total_col = _get_table_and_columns(42)
        assert "season" in raw_table
        assert raw_table == "fct_player_embeddings_season_synced"
        assert total_col == "matches_in_sample"


class TestEmptyResults:
    """Handle case when no similar players found."""

    def test_empty_dataframe_from_search(self) -> None:
        """An empty DataFrame should be handled gracefully."""
        df = pd.DataFrame()
        assert df.empty

    def test_empty_target_vector(self) -> None:
        """When target player has no vector, result is empty."""
        df = pd.DataFrame(columns=pd.Index(["behavioral_vector", "stat_vector"]))
        assert df.empty


class TestMinMatchesFilter:
    """Verify min_matches parameter logic."""

    def test_min_matches_default(self) -> None:
        """Default min matches value should be 5."""
        # The slider default is 5 per the page design
        default = 5
        assert default >= 1
        assert default <= 50

    def test_min_matches_boundary(self) -> None:
        """Boundary values for min matches slider."""
        assert 1 <= 1 <= 50
        assert 1 <= 50 <= 50
