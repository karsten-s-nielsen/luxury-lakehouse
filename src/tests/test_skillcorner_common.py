"""Unit tests for the SkillCorner API client (skillcorner_common)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from ingestion.skillcorner_common import (
    API_BASE_URL,
    PROVIDER,
    MatchInfo,
    fetch_artifact,
    fetch_match_list,
)


class TestMatchInfo:
    def test_parse_match_info(self) -> None:
        raw = {
            "id": "1886347",
            "artifacts": {"1886347_dynamic_events": "1886347_dynamic_events.csv"},
            "home": "Auckland FC",
            "away": "Newcastle",
            "date": "2024-11-30",
            "updated_at": "2026-05-04T02:44:12Z",
            "visibility": "public",
        }
        info = MatchInfo.model_validate(raw)
        assert info.id == "1886347"
        assert info.home == "Auckland FC"
        assert info.updated_at == datetime(2026, 5, 4, 2, 44, 12, tzinfo=timezone.utc)


class TestFetchMatchList:
    @patch("ingestion.skillcorner_common.fetch_url")
    def test_fetch_all_matches(self, mock_fetch: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "provider": "skillcorner",
            "matches": [
                {
                    "id": "1886347",
                    "artifacts": {"1886347_match": "1886347_match.json"},
                    "home": "Auckland FC",
                    "away": "Newcastle",
                    "date": "2024-11-30",
                    "updated_at": "2026-05-04T02:44:12Z",
                    "visibility": "public",
                }
            ],
        }
        mock_fetch.return_value = mock_resp

        result = fetch_match_list("fake-token")
        assert len(result) == 1
        assert result[0].id == "1886347"
        assert result[0].home == "Auckland FC"

        # Verify URL construction
        call_url = mock_fetch.call_args[0][0]
        assert call_url == f"{API_BASE_URL}/skillcorner/matches"

    @patch("ingestion.skillcorner_common.fetch_url")
    def test_fetch_with_updated_since(self, mock_fetch: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"provider": "skillcorner", "matches": []}
        mock_fetch.return_value = mock_resp

        fetch_match_list("fake-token", updated_since="2027-01-01T00:00:00Z")

        call_url = mock_fetch.call_args[0][0]
        assert "updatedSince=2027-01-01T00%3A00%3A00Z" in call_url or "updatedSince=2027-01-01T00:00:00Z" in call_url

    @patch("ingestion.skillcorner_common.fetch_url")
    def test_fetch_empty_response(self, mock_fetch: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"provider": "skillcorner", "matches": []}
        mock_fetch.return_value = mock_resp

        result = fetch_match_list("fake-token")
        assert result == []


class TestFetchArtifact:
    @patch("ingestion.skillcorner_common.fetch_url")
    def test_fetch_artifact_constructs_url(self, mock_fetch: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_fetch.return_value = mock_resp

        fetch_artifact("1886347", "1886347_dynamic_events", "fake-token")

        call_url = mock_fetch.call_args[0][0]
        assert "/skillcorner/matches/1886347/1886347_dynamic_events" in call_url


class TestConstants:
    def test_api_base_url_is_https(self) -> None:
        assert API_BASE_URL.startswith("https://")

    def test_provider_name(self) -> None:
        assert PROVIDER == "skillcorner"
