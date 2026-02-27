"""Tests for streamlit_app.config module."""

from __future__ import annotations

import pytest

from streamlit_app.config import AppSettings


class TestAppSettings:
    """Test AppSettings validation and defaults."""

    def test_valid_settings(self) -> None:
        settings = AppSettings(
            lakebase_host="example.database.cloud.databricks.com",
            lakebase_instance_name="test-instance",
        )
        assert settings.lakebase_host == "example.database.cloud.databricks.com"
        assert settings.lakebase_database == "databricks_postgres"
        assert settings.gold_schema == "dev_gold"
        assert settings.cache_ttl_seconds == 600
        assert settings.pool_connection_max_age_seconds == 3300

    def test_custom_schema(self) -> None:
        settings = AppSettings(
            lakebase_host="host",
            lakebase_instance_name="inst",
            gold_schema="prod_gold",
        )
        assert settings.gold_schema == "prod_gold"

    def test_rejects_sql_injection_schema(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            AppSettings(
                lakebase_host="host",
                lakebase_instance_name="inst",
                gold_schema="dev_gold; DROP TABLE--",
            )

    def test_rejects_schema_with_spaces(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            AppSettings(
                lakebase_host="host",
                lakebase_instance_name="inst",
                gold_schema="dev gold",
            )

    def test_rejects_empty_schema(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            AppSettings(
                lakebase_host="host",
                lakebase_instance_name="inst",
                gold_schema="",
            )

    def test_accepts_underscored_schema(self) -> None:
        settings = AppSettings(
            lakebase_host="host",
            lakebase_instance_name="inst",
            gold_schema="_internal_gold",
        )
        assert settings.gold_schema == "_internal_gold"
