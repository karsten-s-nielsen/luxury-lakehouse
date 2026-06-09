# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.25-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish SPADL/VAEP action values from Databricks gold layer to HF Hub.

Queries Databricks SQL Statement Execution API to extract all action values
from the gold-layer ``fct_action_values`` table and publishes as a Parquet
dataset partitioned by ``data_source`` on HF Hub.

The published dataset is the primary input for VAEP model training
(``train_vaep_model_hf.py``) and downstream analytics scripts that
need pre-computed action values (xT grid, EPV transition).

Usage (HF Jobs CLI):
    hf jobs uv run scripts/publish_spadl_vaep_hf.py \\
        --flavor cpu-basic --timeout 30m \\
        --secrets HF_TOKEN \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN \\
        --env DATABRICKS_SQL_WAREHOUSE_ID=$DATABRICKS_SQL_WAREHOUSE_ID
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd
import requests

from ingestion.hf_publish import get_hf_card_path, upload_hf_readme

# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HF_ORG = "luxury-lakehouse"
DATASET_REPO = f"{HF_ORG}/spadl-vaep-action-values"

# SQL to extract action values from the gold-layer fact table.
# Excludes _loaded_at (internal audit column) — all other columns are published.
#
# fct_action_values is Kimball-conformed post-PR 4b: emits match_key + competition_key
# (new canonical Kimball surrogates) AND legacy match_id + competition_id (90-day
# dual-column window, sunset 2026-07-22 per ADR-011). Consumers migrate to match_key /
# competition_key at their own pace; the HF dataset README documents the window.
_ACTION_VALUES_SQL = """\
SELECT
    action_value_id,
    match_key,                   -- new: Kimball surrogate (ADR-011)
    competition_key,             -- new: Kimball surrogate
    match_id,                    -- LEGACY: sunset 2026-07-22
    competition_id,              -- LEGACY: sunset 2026-07-22
    player_id,
    player_key,                  -- new: Kimball surrogate (PR 7, ADR-011)
    team_id,
    team_key,                    -- new: Kimball surrogate (PR 7, ADR-011)
    season_id,
    period,
    time_seconds,
    minute,
    second,
    start_x,
    start_y,
    end_x,
    end_y,
    action_type,
    action_result,
    bodypart,
    offensive_value,
    defensive_value,
    vaep_value,
    original_event_id,
    data_source
FROM soccer_analytics.dev_gold.fct_action_values
    -- HF license gate: GS data computed internally but not published
    -- until license secured. Remove this filter when license is in place.
WHERE data_source != 'gradientsports'
"""

# Databricks SQL Statement Execution API polling interval (seconds)
_POLL_INTERVAL_S = 2.0

# HTTP timeouts: (connect, read) in seconds
_TIMEOUT_SUBMIT = (10, 120)
_TIMEOUT_POLL = (10, 30)
_TIMEOUT_CHUNK = (10, 120)


# ---------------------------------------------------------------------------
# Databricks SQL Statement Execution API
# ---------------------------------------------------------------------------


def query_databricks_sql(host: str, token: str, sql: str, warehouse_id: str) -> pd.DataFrame:
    """Execute SQL via Databricks Statement Execution API and return DataFrame.

    Handles asynchronous execution (PENDING/RUNNING states) and paginated
    result chunks using EXTERNAL_LINKS disposition with Arrow stream format.

    Args:
        host: Databricks workspace hostname (without ``https://`` prefix).
        token: Databricks personal access token or OAuth token.
        sql: SQL statement to execute.
        warehouse_id: SQL warehouse ID to target.

    Returns:
        DataFrame with columns matching the SQL result schema.

    Raises:
        RuntimeError: If the SQL statement fails on the server or returns no data.
        requests.HTTPError: If an API call returns a non-2xx status.
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

    logger.info("Submitting SQL query to Databricks (warehouse=%s)", warehouse_id)
    resp = requests.post(url, json=payload, headers=headers, timeout=_TIMEOUT_SUBMIT, verify=True)
    if resp.status_code != 200:
        logger.error("SQL API error %d: %s", resp.status_code, resp.text[:500])
    resp.raise_for_status()
    result = resp.json()

    # Poll until terminal state if the query is still running
    statement_id = result.get("statement_id")
    status = result.get("status", {}).get("state")
    logger.info("Statement %s — initial state: %s", statement_id, status)

    while status in ("PENDING", "RUNNING"):
        time.sleep(_POLL_INTERVAL_S)
        poll_resp = requests.get(
            f"{url}/{statement_id}",
            headers=headers,
            timeout=_TIMEOUT_POLL,
            verify=True,
        )
        poll_resp.raise_for_status()
        result = poll_resp.json()
        status = result.get("status", {}).get("state")
        logger.info("Statement %s — polled state: %s", statement_id, status)

    if status == "FAILED":
        error = result.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL statement failed: {error.get('message', 'unknown error')}")

    if status != "SUCCEEDED":
        raise RuntimeError(f"Unexpected terminal state: {status}")

    # Extract column names and row count from manifest
    manifest = result.get("manifest", {})
    columns = [col["name"] for col in manifest.get("schema", {}).get("columns", [])]
    total_row_count = manifest.get("total_row_count", "unknown")
    total_chunk_count = manifest.get("total_chunk_count", 0)
    logger.info("Query returned %s rows in %s chunks, columns: %s", total_row_count, total_chunk_count, columns)

    # EXTERNAL_LINKS disposition: fetch each chunk via presigned URLs
    import pyarrow as pa

    arrow_tables: list[pa.Table] = []
    n_chunks = int(total_chunk_count) if total_chunk_count else 0

    for chunk_idx in range(n_chunks):
        chunk_url = f"{url}/{statement_id}/result/chunks/{chunk_idx}"
        logger.info("Fetching chunk %d/%d", chunk_idx + 1, n_chunks)
        chunk_resp = requests.get(chunk_url, headers=headers, timeout=_TIMEOUT_CHUNK, verify=True)
        chunk_resp.raise_for_status()
        chunk_data = chunk_resp.json()
        for link_info in chunk_data.get("external_links", []):
            link_url = link_info["external_link"]
            byte_count = link_info.get("byte_count", 0)
            logger.info("  Downloading %d bytes", byte_count)
            dl_resp = requests.get(link_url, timeout=_TIMEOUT_CHUNK, verify=True)
            dl_resp.raise_for_status()
            reader = pa.ipc.open_stream(dl_resp.content)
            arrow_tables.append(reader.read_all())

    if not arrow_tables:
        raise RuntimeError("No data chunks returned from Databricks SQL")

    combined = pa.concat_tables(arrow_tables)
    logger.info("Collected %d total rows from %d Arrow chunks", combined.num_rows, len(arrow_tables))
    return combined.to_pandas()


# ---------------------------------------------------------------------------
# dtype normalization
# ---------------------------------------------------------------------------


def normalize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Apply correct dtypes to action value columns after Arrow deserialization.

    Args:
        df: Raw DataFrame from the Databricks SQL API response.

    Returns:
        DataFrame with enforced dtypes matching the HF Hub schema contract.
    """
    # String columns
    for col in ("action_value_id", "action_type", "action_result", "bodypart", "original_event_id", "data_source"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Integer columns (Kimball surrogates + legacy + other IDs)
    for col in (
        "match_key",
        "competition_key",
        "match_id",
        "competition_id",
        "player_id",
        "team_id",
        "season_id",
        "period",
        "minute",
        "second",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Float columns
    for col in (
        "time_seconds",
        "start_x",
        "start_y",
        "end_x",
        "end_y",
        "offensive_value",
        "defensive_value",
        "vaep_value",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    return df


# ---------------------------------------------------------------------------
# HF Hub publishing
# ---------------------------------------------------------------------------


def publish_to_hf_hub(df: pd.DataFrame, hf_token: str) -> str:
    """Write action value data as partitioned Parquet and upload to HF Hub.

    Data is partitioned by ``data_source`` (``data_source=statsbomb``,
    ``data_source=wyscout``) to allow downstream scripts to load subsets
    efficiently by source.

    Args:
        df: Action values DataFrame to publish.
        hf_token: HuggingFace API token.

    Returns:
        URL of the published dataset.
    """
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)

    api.create_repo(
        DATASET_REPO,
        exist_ok=True,
        repo_type="dataset",
        token=hf_token,
    )
    logger.info("Ensured dataset repo exists: %s", DATASET_REPO)

    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)

        for source, source_df in df.groupby("data_source"):
            partition_dir = staging_dir / f"data_source={source}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            partition_df = source_df.drop(columns=["data_source"])
            out_path = partition_dir / "data.parquet"
            partition_df.to_parquet(out_path, index=False, engine="pyarrow")
            logger.info(
                "Wrote partition data_source=%s: %s rows -> %s (%s bytes)",
                source,
                f"{len(partition_df):,}",
                out_path,
                f"{out_path.stat().st_size:,}",
            )

        # Upload the entire data/ directory (replaces existing partitions)
        api.upload_folder(
            folder_path=str(staging_dir),
            path_in_repo="data",
            repo_id=DATASET_REPO,
            repo_type="dataset",
            token=hf_token,
            delete_patterns=["data/*"],
        )

    dataset_url = f"https://huggingface.co/datasets/{DATASET_REPO}"
    logger.info("Published dataset to %s", dataset_url)
    return dataset_url


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def main() -> None:
    """Extract SPADL/VAEP action values from Databricks gold layer and publish to HF Hub."""
    logger.info("Starting SPADL/VAEP action values publication pipeline")

    # ------------------------------------------------------------------
    # 1. Validate environment
    # ------------------------------------------------------------------
    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        from huggingface_hub import get_token

        hf_token = get_token() or ""

    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    databricks_host = os.environ.get("DATABRICKS_HOST", "")
    if not databricks_host:
        raise RuntimeError("DATABRICKS_HOST environment variable required")

    databricks_token = os.environ.get("DATABRICKS_TOKEN", "")
    if not databricks_token:
        raise RuntimeError("DATABRICKS_TOKEN environment variable required")

    warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
    if not warehouse_id:
        raise RuntimeError(
            "DATABRICKS_SQL_WAREHOUSE_ID environment variable required. "
            "Get it from: terraform -chdir=terraform/environments/dev output -raw warehouse_id"
        )

    # Strip protocol prefix if provided (e.g., "https://host" -> "host")
    databricks_host = databricks_host.replace("https://", "").replace("http://", "").rstrip("/")

    logger.info("Databricks host: %s", databricks_host)
    logger.info("Warehouse ID: %s", warehouse_id)

    # ------------------------------------------------------------------
    # 2. Query action values from Databricks gold layer
    # ------------------------------------------------------------------
    logger.info("Querying action values from soccer_analytics.dev_gold.fct_action_values")
    raw_df = query_databricks_sql(
        host=databricks_host,
        token=databricks_token,
        sql=_ACTION_VALUES_SQL,
        warehouse_id=warehouse_id,
    )

    if raw_df.empty:
        raise RuntimeError("Query returned no rows — check that fct_action_values has been built by dbt")

    logger.info("Retrieved %s raw action value rows", f"{len(raw_df):,}")

    # ------------------------------------------------------------------
    # 3. Normalize dtypes
    # ------------------------------------------------------------------
    logger.info("Normalizing column dtypes")
    actions_df = normalize_dtypes(raw_df)

    # Summary statistics
    n_matches = actions_df["match_id"].nunique()
    source_counts = actions_df["data_source"].value_counts().to_dict()
    type_counts = actions_df["action_type"].value_counts().head(5).to_dict()
    logger.info(
        "Action values summary: %s actions, %s matches, by source: %s, top types: %s",
        f"{len(actions_df):,}",
        f"{n_matches:,}",
        source_counts,
        type_counts,
    )

    # ------------------------------------------------------------------
    # 4. Publish to HF Hub
    # ------------------------------------------------------------------
    logger.info("Publishing action values to HF Hub: %s", DATASET_REPO)
    dataset_url = publish_to_hf_hub(actions_df, hf_token)

    # ------------------------------------------------------------------
    # 5. Publish README alongside data (PR 4c)
    # ------------------------------------------------------------------
    readme_result = upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_hf_card_path("spadl-vaep-action-values.md", kind="dataset"),
        hf_token=hf_token,
    )
    logger.info(
        "Uploaded README: %s (sha256=%s)",
        readme_result["commit_url"],
        readme_result["sha256"][:8],
    )

    logger.info("Pipeline complete. Dataset: %s", dataset_url)
    logger.info(
        "Final stats: %s actions, %s matches, sources: %s. "
        "Dual-column schema (match_id/competition_id sunset 2026-07-22).",
        f"{len(actions_df):,}",
        f"{n_matches:,}",
        source_counts,
    )


if __name__ == "__main__":
    main()
