"""Bronze-coverage test for Metrica: tracking + events schema across CSV + EPTS paths.

Retro-validation of task #1 in the PR 1.5 cycle. Exercises both CSV and
EPTS parsers and asserts the unified bronze schema lands. Fails when
either path drops a column that the unified schema contract requires.
"""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pytest

try:
    from coverage_utils import (
        assert_source_covered_by_bronze,
        load_attr_enumeration,
    )
except ImportError:  # pragma: no cover
    from tests.coverage_utils import (  # type: ignore[no-redef]
        assert_source_covered_by_bronze,
        load_attr_enumeration,
    )

from ingestion.metrica_common import (
    _parse_epts_events,
    _parse_epts_metadata,
    _parse_epts_tracking,
)
from ingestion.metrica_tracking import (
    _build_player_columns,
    _parse_tracking_header,
    _reshape_tracking_to_narrow,
)

# Reuse the minimal EPTS XML from the main test suite.
try:
    from test_metrica import _MINIMAL_EPTS_XML
except ImportError:  # pragma: no cover
    from tests.test_metrica import _MINIMAL_EPTS_XML  # type: ignore[no-redef]

_TEST_DIR = Path(__file__).parent
_FIXTURES = _TEST_DIR / "fixtures"
_ENUMERATION_PATH = _FIXTURES / "metrica_attr_enumeration.json"
_TRACKING_CSV = _FIXTURES / "metrica_tracking_home.csv"


@pytest.fixture(scope="module")
def _enumeration() -> dict:
    return load_attr_enumeration(_ENUMERATION_PATH)


# ---- Actual-bronze-col fixtures, one per parser path --------------------


@pytest.fixture(scope="module")
def _tracking_csv_cols() -> set[str]:
    """Bronze cols produced by the Metrica CSV tracking path."""
    csv_text = _TRACKING_CSV.read_text(encoding="utf-8")
    team_row, jersey_row, column_row = _parse_tracking_header(csv_text)
    cols = _build_player_columns(team_row, jersey_row, column_row)
    df = pd.read_csv(io.StringIO(csv_text), skiprows=3, header=None, names=cols)
    narrow = _reshape_tracking_to_narrow(df, "test_match")
    return set(narrow.columns)


@pytest.fixture(scope="module")
def _tracking_epts_cols() -> set[str]:
    """Bronze cols produced by the Metrica EPTS tracking path."""
    meta = _parse_epts_metadata(_MINIMAL_EPTS_XML)
    tracking_text = "1:0.5,0.4;0.3,0.6;0.7,0.2;0.8,0.3:0.5,0.5\n"
    rows = _parse_epts_tracking(tracking_text, meta, "Game_3")
    assert rows, "_parse_epts_tracking produced no rows"
    return set(rows[0].keys())


@pytest.fixture(scope="module")
def _events_epts_cols() -> set[str]:
    """Bronze cols produced by the Metrica EPTS events path (with metadata)."""
    meta = _parse_epts_metadata(_MINIMAL_EPTS_XML)
    events = [
        {
            "index": 1,
            "team": {"name": "Team A"},
            "type": {"name": "PASS"},
            "subtypes": {"name": "HEAD"},
            "period": 1,
            "start": {"frame": 10, "time": 0.4, "x": 0.5, "y": 0.4},
            "end": {"frame": 15, "time": 0.6, "x": 0.6, "y": 0.3},
            "from": {"name": "Player 1"},
            "to": {"name": "Player 2"},
        }
    ]
    df = _parse_epts_events(events, "Game_3", meta)
    assert len(df) == 1
    return set(df.columns)


# ---- Coverage tests ----------------------------------------------------


class TestMetricaBronzeCoverage:
    """Every Metrica source field must appear in the unified bronze schema."""

    def test_enumeration_structure(self, _enumeration: dict) -> None:
        """Guardrail: fixture has the expected top-level sections."""
        assert "tracking" in _enumeration
        assert "events" in _enumeration
        assert "excluded_source_fields" in _enumeration

    def test_tracking_csv_covers_expected_cols(self, _enumeration: dict, _tracking_csv_cols: set[str]) -> None:
        """CSV tracking path must emit all 12 unified bronze cols."""
        expected = set(_enumeration["tracking"]["expected_bronze_cols"])
        assert_source_covered_by_bronze(
            expected_bronze_cols=expected,
            actual_bronze_cols=_tracking_csv_cols,
            excluded={},
            name="Metrica tracking (CSV path)",
        )

    def test_tracking_epts_covers_expected_cols(self, _enumeration: dict, _tracking_epts_cols: set[str]) -> None:
        """EPTS tracking path must emit all 12 unified bronze cols."""
        expected = set(_enumeration["tracking"]["expected_bronze_cols"])
        assert_source_covered_by_bronze(
            expected_bronze_cols=expected,
            actual_bronze_cols=_tracking_epts_cols,
            excluded={},
            name="Metrica tracking (EPTS path)",
        )

    def test_tracking_csv_and_epts_schemas_match(
        self,
        _tracking_csv_cols: set[str],
        _tracking_epts_cols: set[str],
    ) -> None:
        """CSV and EPTS tracking paths must share the SAME bronze schema.

        Schema drift between paths causes Delta write failures when both
        paths MERGE into the same metrica_tracking table.
        """
        only_csv = _tracking_csv_cols - _tracking_epts_cols
        only_epts = _tracking_epts_cols - _tracking_csv_cols
        assert not only_csv, f"cols only in CSV path: {sorted(only_csv)}"
        assert not only_epts, f"cols only in EPTS path: {sorted(only_epts)}"

    def test_events_epts_covers_expected_cols(self, _enumeration: dict, _events_epts_cols: set[str]) -> None:
        """EPTS events path must emit all 19 unified bronze cols (including pitch dims)."""
        expected = set(_enumeration["events"]["expected_bronze_cols"])
        assert_source_covered_by_bronze(
            expected_bronze_cols=expected,
            actual_bronze_cols=_events_epts_cols,
            excluded={},
            name="Metrica events (EPTS path)",
        )

    def test_excluded_fields_have_reasons(self, _enumeration: dict) -> None:
        """Every excluded source field must explain why bronze doesn't emit it."""
        excluded = _enumeration["excluded_source_fields"]
        empty = [k for k, v in excluded.items() if not v]
        assert not empty, f"Excluded fields missing reasons: {empty}"
