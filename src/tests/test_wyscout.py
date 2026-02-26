"""Tests for ingestion.wyscout — event/match normalization and JSON serialization."""

from __future__ import annotations

import io
import json
import logging
import pathlib
import zipfile

import pandas as pd
from ingestion.wyscout import (
    _download_and_extract_zip,
    _load_all_competitions,
    _load_json_local,
    _serialize_json_columns,
)

_FIXTURES = pathlib.Path(__file__).parent / "fixtures"


class TestSerializeJsonColumns:
    """Tests for _serialize_json_columns."""

    def test_serializes_positions(self) -> None:
        df = pd.DataFrame(
            {
                "eventId": [1001],
                "positions": [[{"x": 50, "y": 50}, {"x": 60, "y": 40}]],
            }
        )
        result = _serialize_json_columns(df, ["positions"])
        assert isinstance(result["positions"].iloc[0], str)
        parsed = json.loads(result["positions"].iloc[0])
        assert len(parsed) == 2
        assert parsed[0]["x"] == 50

    def test_serializes_tags(self) -> None:
        df = pd.DataFrame(
            {
                "eventId": [1001],
                "tags": [[{"id": 101}, {"id": 1801}]],
            }
        )
        result = _serialize_json_columns(df, ["tags"])
        assert isinstance(result["tags"].iloc[0], str)
        parsed = json.loads(result["tags"].iloc[0])
        assert len(parsed) == 2
        assert parsed[0]["id"] == 101

    def test_leaves_non_json_columns_unchanged(self) -> None:
        df = pd.DataFrame(
            {
                "eventId": [1001],
                "eventName": ["Pass"],
                "positions": [[{"x": 50, "y": 50}]],
            }
        )
        result = _serialize_json_columns(df, ["positions"])
        assert result["eventName"].iloc[0] == "Pass"
        assert result["eventId"].iloc[0] == 1001

    def test_skips_missing_columns(self) -> None:
        df = pd.DataFrame({"eventId": [1001], "eventName": ["Pass"]})
        # Should not raise even though "positions" doesn't exist
        result = _serialize_json_columns(df, ["positions"])
        assert len(result) == 1


class TestWyscoutEventFixture:
    """Validate the Wyscout event fixture data structure."""

    def test_fixture_has_expected_columns(self) -> None:
        df = pd.read_json(_FIXTURES / "wyscout_events.json")
        expected = {"eventId", "matchId", "eventName", "subEventName", "playerId", "teamId", "matchPeriod", "eventSec"}
        assert expected.issubset(set(df.columns))

    def test_positions_are_lists(self) -> None:
        df = pd.read_json(_FIXTURES / "wyscout_events.json")
        assert isinstance(df["positions"].iloc[0], list)
        assert len(df["positions"].iloc[0]) == 2

    def test_tags_are_lists(self) -> None:
        df = pd.read_json(_FIXTURES / "wyscout_events.json")
        assert isinstance(df["tags"].iloc[0], list)
        assert df["tags"].iloc[0][0]["id"] == 1801


class TestWyscoutMatchFixture:
    """Validate the Wyscout match fixture data structure."""

    def test_fixture_has_expected_columns(self) -> None:
        df = pd.read_json(_FIXTURES / "wyscout_matches.json")
        expected = {"wyId", "competitionId", "seasonId", "dateutc"}
        assert expected.issubset(set(df.columns))

    def test_teams_data_is_dict(self) -> None:
        df = pd.read_json(_FIXTURES / "wyscout_matches.json")
        assert isinstance(df["teamsData"].iloc[0], dict)

    def test_match_count(self) -> None:
        df = pd.read_json(_FIXTURES / "wyscout_matches.json")
        assert len(df) == 2


class TestLoadJsonLocal:
    """Tests for _load_json_local."""

    def test_loads_existing_file(self) -> None:
        logger = logging.getLogger("test")
        df = _load_json_local(_FIXTURES / "wyscout_events.json", logger)
        assert df is not None
        assert len(df) > 0

    def test_returns_none_for_missing_file(self) -> None:
        logger = logging.getLogger("test")
        result = _load_json_local(_FIXTURES / "nonexistent.json", logger)
        assert result is None


class TestDownloadAndExtractZip:
    """Tests for _download_and_extract_zip with synthetic ZIP data."""

    @staticmethod
    def _make_zip(files: dict[str, list[dict]]) -> bytes:
        """Create an in-memory ZIP with JSON files."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for name, data in files.items():
                zf.writestr(name, json.dumps(data))
        return buf.getvalue()

    def test_extracts_competition_dataframes(self, monkeypatch: object) -> None:
        logger = logging.getLogger("test")
        zip_bytes = self._make_zip(
            {
                "events_England.json": [{"eventId": 1}, {"eventId": 2}],
                "events_Italy.json": [{"eventId": 3}],
            }
        )

        class _FakeResponse:
            content = zip_bytes

        import ingestion.wyscout as wyscout_mod

        monkeypatch.setattr(wyscout_mod, "fetch_url", lambda *a, **kw: _FakeResponse())  # type: ignore[attr-defined]
        result = _download_and_extract_zip("https://example.com/test.zip", logger)

        assert "England" in result
        assert "Italy" in result
        assert len(result["England"]) == 2
        assert len(result["Italy"]) == 1

    def test_skips_non_json_files(self, monkeypatch: object) -> None:
        logger = logging.getLogger("test")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("events_England.json", json.dumps([{"eventId": 1}]))
            zf.writestr("README.txt", "Not a JSON file")
        zip_bytes = buf.getvalue()

        class _FakeResponse:
            content = zip_bytes

        import ingestion.wyscout as wyscout_mod

        monkeypatch.setattr(wyscout_mod, "fetch_url", lambda *a, **kw: _FakeResponse())  # type: ignore[attr-defined]
        result = _download_and_extract_zip("https://example.com/test.zip", logger)

        assert "England" in result
        assert len(result) == 1


class TestLoadAllCompetitions:
    """Tests for _load_all_competitions with local-first strategy."""

    def test_local_files_preferred(self, tmp_path: pathlib.Path) -> None:
        logger = logging.getLogger("test")
        # Create local JSON files for all 7 competitions
        from ingestion.wyscout import _COMPETITIONS

        for comp in _COMPETITIONS:
            data = [{"eventId": 1, "competition": comp}]
            (tmp_path / f"events_{comp}.json").write_text(json.dumps(data))

        result = _load_all_competitions(
            "https://example.com/should-not-be-called.zip",
            tmp_path,
            "events",
            logger,
        )

        assert len(result) == len(_COMPETITIONS)
        for comp in _COMPETITIONS:
            assert comp in result
