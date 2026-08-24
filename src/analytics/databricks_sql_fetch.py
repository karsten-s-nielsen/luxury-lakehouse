"""Databricks SQL Statement Execution API helper for HF Jobs trainers.

Extracted from scripts/train_football2vec.py (f2v_v1) so gamma trainers
(f2v_v2, f2v_360, scoutgpt) can import from the wheel without copy-paste.

NOT for Databricks-runtime code -- use spark.sql() there. This helper exists
for PEP 723 scripts running on HF Jobs (no Spark, no databricks-sdk SQL
connector) that need to query gold marts via HTTP.
"""

from __future__ import annotations

import logging
import time

import pandas as pd
import requests
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

logger = logging.getLogger(__name__)

_POLL_INTERVAL_S = 2.0
_TIMEOUT_SUBMIT = (10, 120)
_TIMEOUT_POLL = (10, 30)
_TIMEOUT_CHUNK = (10, 300)

# EXTERNAL_LINKS chunk downloads are large (~20 MB+) presigned-URL fetches that can
# drop the connection near the end (ChunkedEncodingError/IncompleteRead) on a flaky
# network — observed reliably on the 9.76M-row fct_action_values read (ScoutGPT).
# Retry each chunk (re-fetching the manifest so a stale presigned link is refreshed)
# with exponential backoff, catching only the transient connection failures.
_CHUNK_MAX_ATTEMPTS = 4
_CHUNK_BACKOFF_S = 2.0
_CHUNK_RETRYABLE = (ChunkedEncodingError, RequestsConnectionError, RequestsTimeout)


def _fetch_chunk_tables(url: str, statement_id: str, chunk_idx: int, headers: dict[str, str]) -> list:
    """Download one result chunk's Arrow tables, retrying transient download failures.

    Each attempt re-fetches the chunk manifest (``.../result/chunks/{idx}``) so the
    re-download uses a FRESH presigned ``external_link`` (the previous one may have
    expired), then reads every link. Retries only ``_CHUNK_RETRYABLE`` connection
    failures with exponential backoff; a non-transient error (auth, 4xx) propagates
    immediately via ``raise_for_status``.
    """
    import pyarrow as pa

    chunk_url = f"{url}/{statement_id}/result/chunks/{chunk_idx}"
    last_exc: Exception | None = None
    for attempt in range(1, _CHUNK_MAX_ATTEMPTS + 1):
        try:
            chunk_resp = requests.get(chunk_url, headers=headers, timeout=_TIMEOUT_CHUNK, verify=True)
            chunk_resp.raise_for_status()
            tables: list[pa.Table] = []
            for link_info in chunk_resp.json().get("external_links", []):
                dl_resp = requests.get(link_info["external_link"], timeout=_TIMEOUT_CHUNK, verify=True)
                dl_resp.raise_for_status()
                reader = pa.ipc.open_stream(dl_resp.content)
                tables.append(reader.read_all())
            return tables
        except _CHUNK_RETRYABLE as exc:
            last_exc = exc
            if attempt < _CHUNK_MAX_ATTEMPTS:
                sleep_s = _CHUNK_BACKOFF_S * (2 ** (attempt - 1))
                logger.warning(
                    "SQL chunk %d download failed (attempt %d/%d): %s — retrying in %.1fs",
                    chunk_idx,
                    attempt,
                    _CHUNK_MAX_ATTEMPTS,
                    exc,
                    sleep_s,
                )
                time.sleep(sleep_s)
    raise RuntimeError(f"SQL chunk {chunk_idx} download failed after {_CHUNK_MAX_ATTEMPTS} attempts") from last_exc


def query_databricks_sql(host: str, token: str, sql: str, warehouse_id: str) -> pd.DataFrame:
    """Execute SQL via Databricks Statement Execution API + Arrow chunks.

    Args:
        host: Databricks workspace hostname (no scheme, no trailing slash).
        token: Databricks PAT or OAuth token.
        sql: SQL statement to execute.
        warehouse_id: SQL warehouse ID.

    Returns:
        pandas DataFrame with query results.

    Raises:
        RuntimeError: If SQL execution fails or returns no data chunks.
    """
    url = f"https://{host}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "50s",
        "disposition": "EXTERNAL_LINKS",
        "format": "ARROW_STREAM",
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=_TIMEOUT_SUBMIT, verify=True)
    resp.raise_for_status()
    result = resp.json()
    statement_id = result.get("statement_id")
    status = result.get("status", {}).get("state")
    while status in ("PENDING", "RUNNING"):
        time.sleep(_POLL_INTERVAL_S)
        poll_resp = requests.get(f"{url}/{statement_id}", headers=headers, timeout=_TIMEOUT_POLL, verify=True)
        poll_resp.raise_for_status()
        result = poll_resp.json()
        status = result.get("status", {}).get("state")
    if status != "SUCCEEDED":
        err = result.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL {status}: {err.get('message', '?')}")

    manifest = result.get("manifest", {})
    total_chunks = int(manifest.get("total_chunk_count", 0) or 0)

    import pyarrow as pa

    arrow_tables: list[pa.Table] = []
    for chunk_idx in range(total_chunks):
        arrow_tables.extend(_fetch_chunk_tables(url, statement_id, chunk_idx, headers))
    if not arrow_tables:
        raise RuntimeError("No data chunks returned from Databricks SQL")
    combined = pa.concat_tables(arrow_tables).to_pandas()
    logger.info("SQL fetch returned %d rows", len(combined))
    return combined
