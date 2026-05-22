"""Tests for --backfill-artifacts orchestrator flag."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestBackfillArtifactsFlag:
    def test_backfill_skips_guard(self) -> None:
        """--backfill-artifacts should skip the guard entirely."""
        from ingestion.gradientsports import _backfill_artifacts

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {"match_id": "10502"},
        ]

        # Mock match with metadata + roster artifact keys
        mock_match = MagicMock()
        mock_match.id = "10502"
        mock_match.artifacts = ["match_10502_metadata.json", "match_10502_roster.json"]

        with (
            patch("ingestion.gradientsports.resolve_pining_token", return_value="fake"),
            patch("ingestion.gradientsports.fetch_match_list", return_value=[mock_match]),
            patch("ingestion.gradientsports.fetch_artifact") as mock_fetch,
            patch("ingestion.gradientsports.parse_metadata") as mock_parse_meta,
            patch("ingestion.gradientsports.write_metadata") as mock_write_meta,
            patch("ingestion.gradientsports.parse_roster") as mock_parse_roster,
            patch("ingestion.gradientsports.write_roster") as mock_write_roster,
        ):
            import pandas as pd

            mock_fetch.return_value = MagicMock(text="[{}]")
            mock_parse_meta.return_value = pd.DataFrame({"match_id": ["10502"]})
            mock_parse_roster.return_value = pd.DataFrame({"match_id": ["10502"]})
            mock_write_meta.return_value = 1
            mock_write_roster.return_value = 1

            _backfill_artifacts(mock_spark, "cat", "bronze", MagicMock())

            # Guard not called (no timed_check, no skip_guard)
            mock_write_meta.assert_called_once()
            mock_write_roster.assert_called_once()

    def test_backfill_fetches_only_metadata_and_roster(self) -> None:
        """Backfill must NOT fetch events or tracking artifacts."""
        from ingestion.gradientsports import _backfill_artifacts

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {"match_id": "10502"},
        ]

        # Mock match with all artifact types — backfill should only use metadata + roster
        mock_match = MagicMock()
        mock_match.id = "10502"
        mock_match.artifacts = [
            "match_10502_events.json",
            "match_10502_tracking.json",
            "match_10502_metadata.json",
            "match_10502_roster.json",
        ]

        with (
            patch("ingestion.gradientsports.resolve_pining_token", return_value="fake"),
            patch("ingestion.gradientsports.fetch_match_list", return_value=[mock_match]),
            patch("ingestion.gradientsports.fetch_artifact") as mock_fetch,
            patch("ingestion.gradientsports.parse_metadata") as mock_parse_meta,
            patch("ingestion.gradientsports.write_metadata") as mock_write_meta,
            patch("ingestion.gradientsports.parse_roster") as mock_parse_roster,
            patch("ingestion.gradientsports.write_roster") as mock_write_roster,
        ):
            import pandas as pd

            mock_fetch.return_value = MagicMock(text="[{}]")
            mock_parse_meta.return_value = pd.DataFrame({"match_id": ["10502"]})
            mock_parse_roster.return_value = pd.DataFrame({"match_id": ["10502"]})
            mock_write_meta.return_value = 1
            mock_write_roster.return_value = 1

            _backfill_artifacts(mock_spark, "cat", "bronze", MagicMock())

            # fetch_artifact called with metadata + roster keys, NOT events/tracking
            call_args = [c.args[1] for c in mock_fetch.call_args_list]
            for key in call_args:
                assert "event" not in key.lower()
                assert "track" not in key.lower()
