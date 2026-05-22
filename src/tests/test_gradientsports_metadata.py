"""Tests for gradientsports_metadata.py parser."""

from __future__ import annotations

import json

import pytest

# Minimal valid metadata dict matching the real API shape (single-element list wrapper).
# Field names, types, and structure verified against live pining-for-the-data API
# response for match 10502 on 2026-05-22.
_SAMPLE_METADATA = [
    {
        "id": "10502",
        "homeTeam": {"id": "366", "name": "Netherlands", "shortName": "NED"},
        "awayTeam": {"id": "51", "name": "United States", "shortName": "USA"},
        "competition": {"id": "38", "name": "FIFA Men's World Cup"},
        "season": "2022",
        "date": "2022-12-03T15:00:00",
        "stadium": {
            "id": "187",
            "name": "Khalifa International Stadium",
            "pitches": [
                {
                    "id": "628",
                    "length": 105.0,
                    "width": 68.0,
                    "startDate": "1976-01-01",
                    "endDate": None,
                }
            ],
        },
        "homeTeamStartLeft": True,
        "homeTeamStartLeftExtraTime": None,
        "fps": 29.97,
        "halfPeriod": 40.674,
        "period1": 46.6855666667,
        "period2": 51.1722833333,
        "startPeriod1": 179.046,
        "endPeriod1": 2980.18,
        "startPeriod2": 3020.854,
        "endPeriod2": 6091.191,
        "week": 4,
        "videoUrl": "https://epitome.pff.com/en/film_room/9a732bec-0ed6-4799-ae83-e42db8c0a2f0",
        "homeTeamKit": {
            "name": "Home",
            "primaryColor": "#ff9933",
            "primaryTextColor": "#000000",
            "secondaryColor": "#ff9933",
            "secondaryTextColor": "#000000",
        },
        "awayTeamKit": {
            "name": "Home",
            "primaryColor": "#ffffff",
            "primaryTextColor": "#003366",
            "secondaryColor": "#ffffff",
            "secondaryTextColor": "#ff0000",
        },
    }
]


class TestParseMetadata:
    def test_basic_parse(self) -> None:
        from ingestion.gradientsports_metadata import parse_metadata

        df = parse_metadata(_SAMPLE_METADATA, match_id="10502")
        assert len(df) == 1
        assert df["match_id"].iloc[0] == "10502"
        assert "_ingested_at" in df.columns
        # json_normalize flattens homeTeam.id etc.
        assert "homeTeam.id" in df.columns
        assert "competition.id" in df.columns

    def test_from_json_string(self) -> None:
        from ingestion.gradientsports_metadata import parse_metadata

        df = parse_metadata(json.dumps(_SAMPLE_METADATA), match_id="10502")
        assert len(df) == 1

    def test_match_id_validated(self) -> None:
        from ingestion.gradientsports_metadata import parse_metadata

        with pytest.raises(ValueError, match="invalid Gradient Sports match id"):
            parse_metadata(_SAMPLE_METADATA, match_id="bad_id")

    def test_int_columns_widened_to_float64(self) -> None:
        from ingestion.gradientsports_metadata import parse_metadata

        df = parse_metadata(_SAMPLE_METADATA, match_id="10502")
        int_cols = df.select_dtypes(include=["int64", "int32"]).columns
        assert len(int_cols) == 0, f"Expected no int columns, got: {list(int_cols)}"

    def test_list_fields_serialized_to_json_string(self) -> None:
        from ingestion.gradientsports_metadata import parse_metadata

        df = parse_metadata(_SAMPLE_METADATA, match_id="10502")
        pitches_val = df["stadium.pitches"].iloc[0]
        assert isinstance(pitches_val, str)
        parsed = json.loads(pitches_val)
        assert isinstance(parsed, list)
