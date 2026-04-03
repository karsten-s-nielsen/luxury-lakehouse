"""Tests for shared constants and utility functions."""

import re

from shared.constants import (
    COST_TABLE_NAME,
    DEFAULT_CATALOG,
    DEFAULT_GOLD_SCHEMA,
    DEFAULT_OBSERVABILITY_SCHEMA,
    IDENTIFIER_RE,
    mlflow_model_uri,
)


class TestIdentifierRe:
    """Verify the SQL identifier regex matches the existing pattern."""

    def test_valid_identifiers(self) -> None:
        for name in ("soccer_analytics", "dev_gold", "_private", "a1b2c3"):
            assert IDENTIFIER_RE.match(name), f"{name} should be valid"

    def test_invalid_identifiers(self) -> None:
        for name in ("1leading", "has space", "semi;colon", "", "has-dash"):
            assert not IDENTIFIER_RE.match(name), f"{name!r} should be invalid"

    def test_pattern_is_compiled(self) -> None:
        assert isinstance(IDENTIFIER_RE, re.Pattern)


class TestMlflowModelUri:
    """Verify MLflow model URI builder."""

    def test_builds_fqn(self) -> None:
        result = mlflow_model_uri("soccer_analytics", "dev_gold", "xg_model")
        assert result == "soccer_analytics.dev_gold.xg_model"

    def test_custom_catalog(self) -> None:
        result = mlflow_model_uri("prod_catalog", "gold", "vaep_model")
        assert result == "prod_catalog.gold.vaep_model"


class TestDefaults:
    """Verify default constant values match existing codebase conventions."""

    def test_default_catalog(self) -> None:
        assert DEFAULT_CATALOG == "soccer_analytics"

    def test_default_gold_schema(self) -> None:
        assert DEFAULT_GOLD_SCHEMA == "dev_gold"

    def test_observability_schema(self) -> None:
        assert DEFAULT_OBSERVABILITY_SCHEMA == "observability"

    def test_cost_table_name(self) -> None:
        assert COST_TABLE_NAME == "workflow_cost_live"
