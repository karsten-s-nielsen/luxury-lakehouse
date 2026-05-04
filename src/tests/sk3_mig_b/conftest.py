"""Shared fixtures for SK3-MIG-B post-retrain smoke gates.

Each gate runs after Champion promotion + mart write. The orchestrator invokes
the gate via `pytest src/tests/sk3_mig_b/test_<item>_post_retrain_smoke.py -v`.
Failure halts the orchestrator before Lakebase synced refresh fires (per spec §5.2).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


@pytest.fixture(scope="session")
def workspace_client() -> WorkspaceClient:
    """Databricks SDK client — authenticated via env (DATABRICKS_TOKEN + DATABRICKS_HOST).

    Skips the test if Databricks credentials aren't configured (CI without
    Databricks secrets). The smoke gates are runtime gates designed to fire
    against a deployed Champion + populated mart; they have no semantics in
    a CI context that lacks lakehouse access.
    """
    if not os.environ.get("DATABRICKS_HOST"):
        pytest.skip("DATABRICKS_HOST not set — smoke gate cannot run without lakehouse access")
    from databricks.sdk import WorkspaceClient

    try:
        return WorkspaceClient()
    except Exception as exc:
        pytest.skip(f"WorkspaceClient construction failed (no Databricks auth in CI): {exc}")


@pytest.fixture(scope="session")
def warehouse_id() -> str:
    """Serverless SQL warehouse ID for statement_execution queries.

    Resolves from DATABRICKS_SQL_WAREHOUSE_ID, falling back to the last
    path segment of DATABRICKS_HTTP_PATH (`/sql/1.0/warehouses/<id>`).
    """
    wh_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
    if not wh_id and (http_path := os.environ.get("DATABRICKS_HTTP_PATH", "")):
        wh_id = http_path.rstrip("/").rsplit("/", 1)[-1]
    if not wh_id:
        pytest.skip("DATABRICKS_SQL_WAREHOUSE_ID / DATABRICKS_HTTP_PATH not set — smoke gate cannot run without it")
    return wh_id


@pytest.fixture(scope="session")
def catalog() -> str:
    """Lakehouse catalog (env-overridable per project pattern)."""
    return os.environ.get("DATABRICKS_CATALOG", "soccer_analytics")


@pytest.fixture(scope="session")
def gold_schema(catalog: str) -> str:
    """Gold-layer schema (FQN: catalog.schema)."""
    return f"{catalog}.dev_gold"


@pytest.fixture(scope="session")
def bronze_schema(catalog: str) -> str:
    """Bronze-layer schema."""
    return f"{catalog}.bronze"


def execute_sql(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    sql: str,
) -> list[list[Any]]:
    """Run a SQL query via WorkspaceClient.statement_execution; return data_array.

    Uses the SDK statement_execution path (per reference_sdk_over_sql_connector.md):
    auto-resolves auth + auto-starts warehouse + bypasses Thrift retry-bug class.
    """
    result = workspace_client.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )
    if result.result is None or result.result.data_array is None:
        return []
    return result.result.data_array
