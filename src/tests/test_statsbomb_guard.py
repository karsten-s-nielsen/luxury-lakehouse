"""Unit tests for StatsBomb anti-join skip guard."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd


class TestStatsbombGuard:
    """Verify the anti-join guard skips when no new competitions/matches exist."""

    def test_no_new_data_skips(self) -> None:
        """When all competitions and matches exist in bronze, count=0."""
        from ingestion.statsbomb import skip_guard

        spark = MagicMock()

        # Mock sb.competitions() returning known competitions
        competitions_df = pd.DataFrame(
            {
                "competition_id": [1, 2],
                "season_id": [10, 20],
            }
        )

        # Mock bronze competitions table — same as API
        bronze_comps = MagicMock()
        bronze_comps.select.return_value = bronze_comps
        bronze_comps.toPandas.return_value = pd.DataFrame(
            {
                "competition_id": [1, 2],
                "season_id": [10, 20],
            }
        )

        # Mock bronze matches table — same match IDs as API
        bronze_matches = MagicMock()
        bronze_matches.select.return_value = bronze_matches
        bronze_matches.toPandas.return_value = pd.DataFrame(
            {
                "match_id": [100, 101, 200, 201],
            }
        )

        # sb.matches() returns same match IDs per sampled competition
        matches_comp1 = pd.DataFrame({"match_id": [100, 101]})
        matches_comp2 = pd.DataFrame({"match_id": [200, 201]})

        def _mock_table(name: str) -> MagicMock:
            if "competitions" in name:
                return bronze_comps
            if "matches" in name:
                return bronze_matches
            return MagicMock()

        spark.table.side_effect = _mock_table

        with patch("ingestion.statsbomb._get_sb") as mock_get_sb:
            mock_sb = MagicMock()
            mock_sb.competitions.return_value = competitions_df
            mock_sb.matches.side_effect = [matches_comp1, matches_comp2]
            mock_get_sb.return_value = mock_sb
            result = skip_guard.check(spark, "catalog", "bronze")

        assert result.count == 0, "No new data → skip"

    def test_new_competition_triggers(self) -> None:
        """When a new competition exists, count=1."""
        from ingestion.statsbomb import skip_guard

        spark = MagicMock()

        # sb.competitions() returns 3 competitions (one new)
        competitions_df = pd.DataFrame(
            {
                "competition_id": [1, 2, 3],
                "season_id": [10, 20, 30],
            }
        )

        # Bronze has only 2
        bronze_comps = MagicMock()
        bronze_comps.select.return_value = bronze_comps
        bronze_comps.toPandas.return_value = pd.DataFrame(
            {
                "competition_id": [1, 2],
                "season_id": [10, 20],
            }
        )

        spark.table.side_effect = lambda name: bronze_comps if "competitions" in name else MagicMock()

        with patch("ingestion.statsbomb._get_sb") as mock_get_sb:
            mock_sb = MagicMock()
            mock_sb.competitions.return_value = competitions_df
            mock_get_sb.return_value = mock_sb
            result = skip_guard.check(spark, "catalog", "bronze")

        assert result.count == 1, "New competition → trigger"

    def test_new_matches_in_existing_competition_triggers(self) -> None:
        """When competitions match but new matches exist, count=1."""
        from ingestion.statsbomb import skip_guard

        spark = MagicMock()

        # Same competitions in API and bronze
        competitions_df = pd.DataFrame(
            {
                "competition_id": [1, 2],
                "season_id": [10, 20],
            }
        )
        bronze_comps = MagicMock()
        bronze_comps.select.return_value = bronze_comps
        bronze_comps.toPandas.return_value = pd.DataFrame(
            {
                "competition_id": [1, 2],
                "season_id": [10, 20],
            }
        )

        # sb.matches returns 3 matches for comp 1; bronze has 2
        matches_df = pd.DataFrame({"match_id": [100, 101, 102]})

        bronze_matches = MagicMock()
        bronze_matches.select.return_value = bronze_matches
        bronze_matches.toPandas.return_value = pd.DataFrame({"match_id": [100, 101]})

        def _mock_table(name: str) -> MagicMock:
            if "competitions" in name:
                return bronze_comps
            if "matches" in name:
                return bronze_matches
            return MagicMock()

        spark.table.side_effect = _mock_table

        with patch("ingestion.statsbomb._get_sb") as mock_get_sb:
            mock_sb = MagicMock()
            mock_sb.competitions.return_value = competitions_df
            mock_sb.matches.return_value = matches_df
            mock_get_sb.return_value = mock_sb
            result = skip_guard.check(spark, "catalog", "bronze")

        assert result.count == 1, "New matches in existing competition → trigger"
