"""Cross-package constants and identifier validation.

This module has zero external dependencies — stdlib only.
It is safe to import from any package (analytics, ingestion, workflows)
and from the Taipy Docker image (via wheel install).
"""

import re

IDENTIFIER_RE: re.Pattern[str] = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
"""SQL-safe identifier pattern. Use for catalog, schema, and table name validation."""

DEFAULT_CATALOG = "soccer_analytics"
DEFAULT_GOLD_SCHEMA = "dev_gold"
DEFAULT_OBSERVABILITY_SCHEMA = "observability"
COST_TABLE_NAME = "workflow_cost_live"


def mlflow_model_uri(catalog: str, schema: str, model_name: str) -> str:
    """Build a fully qualified MLflow Unity Catalog model URI."""
    return f"{catalog}.{schema}.{model_name}"
