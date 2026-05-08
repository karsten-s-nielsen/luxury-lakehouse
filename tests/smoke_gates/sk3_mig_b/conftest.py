"""Shared fixtures for SK3-MIG-B post-retrain smoke gates.

Each gate runs after Champion promotion + mart write. The orchestrator invokes
the gate via `pytest tests/smoke_gates/sk3_mig_b/test_<item>_post_retrain_smoke.py -v`.
Failure halts the orchestrator before Lakebase synced refresh fires (per spec §5.2).
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    import pandas as pd
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


_MAX_POLL_SECONDS = 600  # 10 min walltime cap on a single smoke-gate fetch.


def chunked_sql_to_pandas(host: str, token: str, sql: str, warehouse_id: str) -> pd.DataFrame:
    """Submit a SQL query, fetch all chunks via EXTERNAL_LINKS, return a pandas DataFrame.

    Used by the ext_v2 post-retrain smoke gates which evaluate Singh / KDE-smoothed
    Singh producers against the FULL fct_action_values fold (8.8M+ rows) — too
    large for inline `data_array` (capped at ~25 MB). Mirrors the Arrow-stream
    chunked-fetch pattern from `scripts/train_football2vec.py`.

    Caller passes a pre-stripped host (no `https://` prefix). Honours the
    project's HTTPS-only + explicit-timeout + verify=True security posture, plus
    a `_MAX_POLL_SECONDS` walltime cap on the polling loop so a stuck warehouse
    doesn't hang pytest indefinitely.
    """
    import pyarrow as pa
    import requests

    base = f"https://{host}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {token}"}
    payload: dict[str, Any] = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "50s",
        "disposition": "EXTERNAL_LINKS",
        "format": "ARROW_STREAM",
    }

    resp = requests.post(base, json=payload, headers=headers, timeout=(10, 120), verify=True)
    resp.raise_for_status()
    result = resp.json()
    statement_id = result.get("statement_id")
    status = result.get("status", {}).get("state")

    poll_started = time.time()
    while status in ("PENDING", "RUNNING"):
        if time.time() - poll_started > _MAX_POLL_SECONDS:
            raise RuntimeError(
                f"chunked_sql_to_pandas polling exceeded {_MAX_POLL_SECONDS}s "
                f"(statement_id={statement_id}, last_status={status}). "
                f"Warehouse stuck; investigate before retry."
            )
        time.sleep(2.0)
        poll = requests.get(f"{base}/{statement_id}", headers=headers, timeout=(10, 30), verify=True)
        poll.raise_for_status()
        result = poll.json()
        status = result.get("status", {}).get("state")

    if status != "SUCCEEDED":
        err = result.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL {status}: {err.get('message', '?')}")

    manifest = result.get("manifest", {})
    total_chunks = int(manifest.get("total_chunk_count", 0) or 0)

    arrow_tables: list[pa.Table] = []
    for chunk_idx in range(total_chunks):
        chunk_url = f"{base}/{statement_id}/result/chunks/{chunk_idx}"
        chunk_resp = requests.get(chunk_url, headers=headers, timeout=(10, 300), verify=True)
        chunk_resp.raise_for_status()
        for link_info in chunk_resp.json().get("external_links", []):
            dl = requests.get(link_info["external_link"], timeout=(10, 300), verify=True)
            dl.raise_for_status()
            reader = pa.ipc.open_stream(dl.content)
            arrow_tables.append(reader.read_all())

    if not arrow_tables:
        raise RuntimeError("No data chunks returned for SQL fetch")
    return pa.concat_tables(arrow_tables).to_pandas()
