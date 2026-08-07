"""Cross-package constants and identifier validation.

This module has zero external dependencies — stdlib only.
It is safe to import from any package (analytics, ingestion, workflows)
and from the Taipy Docker image (via wheel install).
"""

import re

IDENTIFIER_RE: re.Pattern[str] = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
"""SQL-safe identifier pattern. Use for catalog, schema, and table name validation."""

DEFAULT_CATALOG = "soccer_analytics"

# ── Medallion layer schemas (ADR-073) ──────────────────────────────────
#
# There is exactly ONE environment. These are constants, not per-run
# parameters: a single ``--schema`` value threaded to consumers that each
# mean a DIFFERENT layer by it resolves correctly for at most one of them.
# hf_sync was passed ``--schema bronze`` and handed it to six consumers
# that wanted gold — five failed on every run and a sixth swallowed the
# miss at INFO (2026-08-07).
#
# Name the layer at the point of use: ``f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_x"``.
# Enforced by ``src/tests/test_layer_schema_conformance.py``.
DEFAULT_BRONZE_SCHEMA = "bronze"
DEFAULT_SILVER_SCHEMA = "dev_silver"
DEFAULT_GOLD_SCHEMA = "dev_gold"
DEFAULT_OBSERVABILITY_SCHEMA = "observability"
COST_TABLE_NAME = "workflow_cost_live"


def mlflow_model_uri(catalog: str, schema: str, model_name: str) -> str:
    """Build a fully qualified MLflow Unity Catalog model URI."""
    return f"{catalog}.{schema}.{model_name}"
