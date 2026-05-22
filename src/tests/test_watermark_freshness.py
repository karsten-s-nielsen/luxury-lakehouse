"""Unit tests for watermark-based skip guard functions."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch


class TestCheckUpstreamFreshness:
    """Tests for check_upstream_freshness."""

    def _make_mock_spark(
        self,
        *,
        history_rows: list[tuple[str, int]] | None = None,
        watermark_rows: list[tuple[str, str, int]] | None = None,
        history_error: Exception | None = None,
    ) -> MagicMock:
        """Build a mock SparkSession that responds to DESCRIBE HISTORY and table reads."""
        spark = MagicMock()

        if history_error is not None:
            spark.sql.side_effect = history_error
            return spark

        # DESCRIBE HISTORY returns a DataFrame with 'operation' and 'version' columns
        if history_rows is not None:
            history_df = MagicMock()
            row_mocks = []
            for op, ver in history_rows:
                row = MagicMock()
                row.operation = op
                row.version = ver
                row_mocks.append(row)
            history_df.collect.return_value = row_mocks
            spark.sql.return_value = history_df

        return spark

    def test_first_run_no_stored_watermarks(self) -> None:
        from ingestion.guards import check_upstream_freshness

        spark = self._make_mock_spark(history_rows=[("WRITE", 5)])
        # Patch the watermark table read to return empty
        with patch("ingestion.guards._load_stored_watermarks", return_value={}):
            with patch("ingestion.guards.ensure_table"):
                result = check_upstream_freshness(spark, "catalog", "wf-test", ["catalog.schema.table_a"])
        assert result.count == 1, "First run (no stored watermarks) should trigger"

    def test_all_versions_match_skips(self) -> None:
        from ingestion.guards import check_upstream_freshness

        spark = self._make_mock_spark(history_rows=[("WRITE", 5)])
        stored = {"catalog.schema.table_a": 5}
        with patch("ingestion.guards._load_stored_watermarks", return_value=stored):
            with patch("ingestion.guards.ensure_table"):
                result = check_upstream_freshness(spark, "catalog", "wf-test", ["catalog.schema.table_a"])
        assert result.count == 0, "All versions match → skip"

    def test_one_upstream_changed_triggers(self) -> None:
        from ingestion.guards import check_upstream_freshness

        spark = self._make_mock_spark(history_rows=[("WRITE", 7)])
        stored = {"catalog.schema.table_a": 5}
        with patch("ingestion.guards._load_stored_watermarks", return_value=stored):
            with patch("ingestion.guards.ensure_table"):
                result = check_upstream_freshness(spark, "catalog", "wf-test", ["catalog.schema.table_a"])
        assert result.count == 1, "Version changed → trigger"

    def test_table_not_found_fails_open(self) -> None:
        from ingestion.guards import check_upstream_freshness

        spark = self._make_mock_spark(history_error=Exception("TABLE_OR_VIEW_NOT_FOUND"))
        with patch("ingestion.guards._load_stored_watermarks", return_value={}):
            with patch("ingestion.guards.ensure_table"):
                result = check_upstream_freshness(spark, "catalog", "wf-test", ["catalog.schema.missing"])
        assert result.count == 1, "Table not found → fail open"

    def test_only_optimize_vacuum_ops_with_stored_watermark_skips(self) -> None:
        from ingestion.guards import check_upstream_freshness

        spark = self._make_mock_spark(history_rows=[("OPTIMIZE", 10), ("VACUUM END", 11)])
        # Stored watermark at version 5 — only maintenance ops since then
        stored = {"catalog.schema.table_a": 5}
        with patch("ingestion.guards._load_stored_watermarks", return_value=stored):
            with patch("ingestion.guards.ensure_table"):
                result = check_upstream_freshness(spark, "catalog", "wf-test", ["catalog.schema.table_a"])
        # Stored watermark exists + no data-changing ops → data unchanged → skip
        assert result.count == 0, "Stored watermark + only OPTIMIZE/VACUUM → skip"

    def test_only_optimize_vacuum_ops_no_stored_watermark_fails_open(self) -> None:
        from ingestion.guards import check_upstream_freshness

        spark = self._make_mock_spark(history_rows=[("OPTIMIZE", 10), ("VACUUM END", 11)])
        # No stored watermark — first run
        with patch("ingestion.guards._load_stored_watermarks", return_value={}):
            with patch("ingestion.guards.ensure_table"):
                result = check_upstream_freshness(spark, "catalog", "wf-test", ["catalog.schema.table_a"])
        # No stored watermark + no data-changing ops → fail open
        assert result.count == 1, "No stored watermark + no data-changing ops → fail open"


class TestRecordWatermarks:
    """Tests for record_watermarks."""

    def test_records_current_versions(self) -> None:
        from ingestion.guards import record_watermarks

        spark = MagicMock()
        # DESCRIBE HISTORY returns version 7 for a WRITE op
        history_df = MagicMock()
        row = MagicMock()
        row.operation = "WRITE"
        row.version = 7
        history_df.collect.return_value = [row]
        spark.sql.return_value = history_df

        with patch("ingestion.guards.ensure_table"):
            record_watermarks(spark, "catalog", "wf-test", ["catalog.schema.table_a"])

        # Verify MERGE was called with correct workflow_id, table, and version
        merge_calls = [str(call) for call in spark.sql.call_args_list if "MERGE" in str(call)]
        assert len(merge_calls) == 1, "Should MERGE watermark record"
        merge_sql = merge_calls[0]
        assert "'wf-test'" in merge_sql, "MERGE should contain workflow_id"
        assert "'catalog.schema.table_a'" in merge_sql, "MERGE should contain table FQN"
        assert "7 AS last_seen_version" in merge_sql, "MERGE should contain version"


class TestResolveUpstreamTablesFromCard:
    """Tests for resolve_upstream_tables_from_card."""

    def _cards_dir(self) -> Path:
        """Resolve workflow-cards/ from repo root for test use."""
        from ingestion.guards import _repo_cards_dir

        return _repo_cards_dir()

    def test_resolves_placeholders(self) -> None:
        from ingestion.guards import resolve_upstream_tables_from_card

        result = resolve_upstream_tables_from_card(
            "wf-publish-spadl-vaep",
            "soccer_analytics",
            "dev_gold",
            cards_dir=self._cards_dir(),
        )
        assert "soccer_analytics.dev_gold.fct_action_values" in result

    def test_filters_delta_table_source_only(self) -> None:
        from ingestion.guards import resolve_upstream_tables_from_card

        result = resolve_upstream_tables_from_card(
            "wf-publish-spadl-vaep",
            "soccer_analytics",
            "dev_gold",
            cards_dir=self._cards_dir(),
        )
        # All returned entries should be fully-qualified table names
        for table in result:
            assert table.count(".") >= 2, f"Expected FQN, got {table}"

    def test_resolves_from_wheel_install_path(self, tmp_path: Path) -> None:
        """When wheel-install path exists, resolver uses it (not source-tree)."""
        from ingestion.guards import resolve_upstream_tables_from_card

        # Create a fake wheel-install layout: <site-packages>/workflow_cards/wf-test.yaml
        fake_site_packages = tmp_path / "site_packages"
        cards_in_wheel = fake_site_packages / "workflow_cards"
        cards_in_wheel.mkdir(parents=True)
        card = cards_in_wheel / "wf-test.yaml"
        card.write_text(
            "inputs:\n  datasets:\n    - id: '{catalog}.{schema}.my_table'\n      source: delta-table\n",
            encoding="utf-8",
        )

        # Monkeypatch the package-level anchor so the resolver thinks
        # ingestion/__init__.py lives at <site-packages>/ingestion/__init__.py.
        # This matches the hf_publish.py test pattern (_WHEEL_INGESTION_FILE).
        import ingestion.guards as guards_mod

        original = guards_mod._WHEEL_INGESTION_FILE
        try:
            guards_mod._WHEEL_INGESTION_FILE = fake_site_packages / "ingestion" / "__init__.py"
            result = resolve_upstream_tables_from_card("wf-test", "cat", "sch")
        finally:
            guards_mod._WHEEL_INGESTION_FILE = original

        assert result == ["cat.sch.my_table"]

    def test_falls_back_to_source_tree(self, tmp_path: Path) -> None:
        """When wheel-install path does not exist, resolver falls back to source-tree."""
        import ingestion.guards as guards_mod
        from ingestion.guards import resolve_upstream_tables_from_card

        original = guards_mod._WHEEL_INGESTION_FILE
        try:
            guards_mod._WHEEL_INGESTION_FILE = tmp_path / "nonexistent" / "ingestion" / "__init__.py"
            # Source-tree fallback should find the real workflow-cards/ at repo root
            result = resolve_upstream_tables_from_card("wf-publish-spadl-vaep", "soccer_analytics", "dev_gold")
        finally:
            guards_mod._WHEEL_INGESTION_FILE = original

        assert len(result) > 0
        assert all("soccer_analytics" in t for t in result)


class TestDeriveUpstreamTables:
    """Tests for _derive_upstream_tables in refresh_synced_tables."""

    def test_strips_synced_suffix_default_schema(self) -> None:
        # Monkeypatch SYNCED_TABLES to a known list for isolation
        import ingestion.refresh_synced_tables as mod
        from ingestion.refresh_synced_tables import SyncedTableConfig, _derive_upstream_tables

        original = mod.SYNCED_TABLES
        mod.SYNCED_TABLES = [
            SyncedTableConfig("fct_shots_synced", "fct_shots", ("shot_id",)),
            SyncedTableConfig("dim_players_synced", "dim_players", ("player_id",)),
        ]
        try:
            result = _derive_upstream_tables("cat", "gold")
        finally:
            mod.SYNCED_TABLES = original

        assert result == ["cat.gold.fct_shots", "cat.gold.dim_players"]

    def test_applies_schema_override(self) -> None:
        import ingestion.refresh_synced_tables as mod
        from ingestion.refresh_synced_tables import SyncedTableConfig, _derive_upstream_tables

        original = mod.SYNCED_TABLES
        mod.SYNCED_TABLES = [
            SyncedTableConfig(
                "workflow_cost_live_synced",
                "workflow_cost_live",
                ("run_id",),
                schema_override="observability",
            ),
            SyncedTableConfig("fct_shots_synced", "fct_shots", ("shot_id",)),
        ]
        try:
            result = _derive_upstream_tables("cat", "gold")
        finally:
            mod.SYNCED_TABLES = original

        assert result == [
            "cat.observability.workflow_cost_live",
            "cat.gold.fct_shots",
        ]
