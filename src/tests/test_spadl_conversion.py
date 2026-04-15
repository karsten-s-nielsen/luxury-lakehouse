"""Unit tests for spadl_conversion: predicate builders + UDF error propagation."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from ingestion.spadl_conversion import (
    _make_sb_spadl_udf,
    _make_statsbomb_replace_where,
    _make_ws_spadl_udf,
    _make_wyscout_replace_where,
)


def test_statsbomb_replace_where_single_match() -> None:
    predicate = _make_statsbomb_replace_where([3749052])
    assert predicate == "data_source = 'statsbomb' AND match_id IN (3749052)"


def test_statsbomb_replace_where_multiple_matches_sorted() -> None:
    predicate = _make_statsbomb_replace_where([3, 1, 2])
    # Sorted for determinism
    assert predicate == "data_source = 'statsbomb' AND match_id IN (1, 2, 3)"


def test_statsbomb_replace_where_empty_raises() -> None:
    with pytest.raises(ValueError, match="at least one match_id"):
        _make_statsbomb_replace_where([])


def test_wyscout_replace_where_single_match() -> None:
    predicate = _make_wyscout_replace_where([5413999])
    assert predicate == "data_source = 'wyscout' AND match_id IN (5413999)"


def test_wyscout_replace_where_multiple_matches_sorted() -> None:
    predicate = _make_wyscout_replace_where([42, 41, 40])
    assert predicate == "data_source = 'wyscout' AND match_id IN (40, 41, 42)"


def test_wyscout_replace_where_empty_raises() -> None:
    with pytest.raises(ValueError, match="at least one match_id"):
        _make_wyscout_replace_where([])


# ---------------------------------------------------------------------------
# UDF error propagation — regression guard for the 2026-04-14 silent swallow
# removal.  If the per-match silly-kicks call fails, the error must propagate
# with match_id context so Spark surfaces it on the driver instead of the
# UDF returning an empty DataFrame and silently dropping the match.
# ---------------------------------------------------------------------------


class TestStatsBombConverterErrorPropagation:
    """`_make_sb_spadl_udf()` must raise RuntimeError with match_id on failure."""

    def _minimal_statsbomb_pdf(self) -> pd.DataFrame:
        """Minimal input with the fields the UDF extracts before the try block."""
        return pd.DataFrame(
            {
                "home_team_id": [999, 999],
                "match_id": [54321, 54321],
                "competition_id": [2, 2],
                "season_id": [44, 44],
                # Placeholder event columns — not validated by the UDF because
                # `adapt_statsbomb_events` is patched to raise below.
                "event_id": ["evt1", "evt2"],
            }
        )

    def test_adapter_failure_raises_runtime_error_with_match_id(self) -> None:
        udf = _make_sb_spadl_udf()
        pdf = self._minimal_statsbomb_pdf()

        with (
            patch(
                "ingestion.spadl_adapter.adapt_statsbomb_events",
                side_effect=KeyError("simulated silly-kicks failure"),
            ),
            pytest.raises(RuntimeError, match=r"StatsBomb SPADL conversion failed for match_id=54321"),
        ):
            udf(pdf)  # type: ignore[operator]

    def test_silly_kicks_convert_failure_raises_runtime_error_with_match_id(self) -> None:
        """Adapter succeeds but `silly_kicks.spadl.statsbomb.convert_to_actions` raises."""
        import silly_kicks.spadl.statsbomb as sb_module

        udf = _make_sb_spadl_udf()
        pdf = self._minimal_statsbomb_pdf()

        with (
            patch(
                "ingestion.spadl_adapter.adapt_statsbomb_events",
                return_value=pd.DataFrame({"dummy": [0]}),
            ),
            patch.object(sb_module, "convert_to_actions", side_effect=ValueError("bad event")),
            pytest.raises(RuntimeError, match=r"StatsBomb SPADL conversion failed for match_id=54321"),
        ):
            udf(pdf)  # type: ignore[operator]

    def test_empty_pdf_returns_empty_df_without_raising(self) -> None:
        """The empty-input short-circuit path must still return cleanly."""
        udf = _make_sb_spadl_udf()
        result = udf(pd.DataFrame())  # type: ignore[operator]
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


class TestWyscoutConverterErrorPropagation:
    """`_make_ws_spadl_udf()` must raise RuntimeError with match_id on failure."""

    def _minimal_wyscout_pdf(self) -> pd.DataFrame:
        """Minimal input — uses `match_id` column; UDF also accepts `matchId`."""
        return pd.DataFrame(
            {
                "home_team_id": [888, 888],
                "match_id": [98765, 98765],
                "competition_id": [6, 6],
                "season_id": [181248, 181248],
                "event_id": ["w1", "w2"],
            }
        )

    def test_adapter_failure_raises_runtime_error_with_match_id(self) -> None:
        udf = _make_ws_spadl_udf()
        pdf = self._minimal_wyscout_pdf()

        with (
            patch(
                "ingestion.spadl_adapter.adapt_wyscout_events",
                side_effect=KeyError("simulated wyscout adapter failure"),
            ),
            pytest.raises(RuntimeError, match=r"Wyscout SPADL conversion failed for match_id=98765"),
        ):
            udf(pdf)  # type: ignore[operator]

    def test_silly_kicks_convert_failure_raises_runtime_error_with_match_id(self) -> None:
        import silly_kicks.spadl.wyscout as ws_module

        udf = _make_ws_spadl_udf()
        pdf = self._minimal_wyscout_pdf()

        with (
            patch(
                "ingestion.spadl_adapter.adapt_wyscout_events",
                return_value=pd.DataFrame({"dummy": [0]}),
            ),
            patch.object(ws_module, "convert_to_actions", side_effect=ValueError("bad wyscout event")),
            pytest.raises(RuntimeError, match=r"Wyscout SPADL conversion failed for match_id=98765"),
        ):
            udf(pdf)  # type: ignore[operator]

    def test_empty_pdf_returns_empty_df_without_raising(self) -> None:
        udf = _make_ws_spadl_udf()
        result = udf(pd.DataFrame())  # type: ignore[operator]
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
