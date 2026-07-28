# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.84-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish OBSO+PAUSA prerequisite inputs (IDSSE events + ELASTIC sync) to HF Hub.

Migrated from notebooks/publish_obso_data.py per HF4 (SK3-MIG-B). PEP 723
single-file: runs locally + on HF Jobs. Uses Databricks SQL Statement Execution
API + Arrow chunks (no Spark / .toPandas() OOM risk).

Dataset: luxury-lakehouse/obso-pausa-inputs

Usage (HF Jobs):
    hf jobs uv run scripts/publish_obso_pausa_inputs_hf.py \\
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

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
DATASET_REPO = f"{HF_ORG}/obso-pausa-inputs"

# Combine IDSSE events + ELASTIC sync results in a single denormalized payload.
_INPUTS_SQL = """\
SELECT
    e.match_id,
    e.event_id,
    e.event_type,
    e.timestamp_seconds,
    e.period,
    e.player_id,
    e.team,
    e.x,
    e.y,
    sync.frame_id,
    sync.alignment_confidence,
    sync.alignment_error_seconds
FROM soccer_analytics.bronze.idsse_events e
INNER JOIN soccer_analytics.bronze.elastic_sync_results sync
    ON e.match_id = sync.match_id AND e.event_id = sync.event_id
"""

_POLL_INTERVAL_S = 2.0
_TIMEOUT_SUBMIT = (10, 120)
_TIMEOUT_POLL = (10, 30)
_TIMEOUT_CHUNK = (10, 120)


def query_databricks_sql(host: str, token: str, sql: str, warehouse_id: str) -> pd.DataFrame:
    """Execute SQL via Databricks Statement Execution API and return DataFrame."""
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

    statement_id = result.get("statement_id")
    status = result.get("status", {}).get("state")
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

    if status == "FAILED":
        error = result.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL failed: {error.get('message', 'unknown')}")
    if status != "SUCCEEDED":
        raise RuntimeError(f"Unexpected state: {status}")

    manifest = result.get("manifest", {})
    total_chunks = int(manifest.get("total_chunk_count", 0) or 0)

    import pyarrow as pa

    arrow_tables: list[pa.Table] = []
    for chunk_idx in range(total_chunks):
        chunk_url = f"{url}/{statement_id}/result/chunks/{chunk_idx}"
        chunk_resp = requests.get(chunk_url, headers=headers, timeout=_TIMEOUT_CHUNK, verify=True)
        chunk_resp.raise_for_status()
        chunk_data = chunk_resp.json()
        for link_info in chunk_data.get("external_links", []):
            dl_resp = requests.get(link_info["external_link"], timeout=_TIMEOUT_CHUNK, verify=True)
            dl_resp.raise_for_status()
            reader = pa.ipc.open_stream(dl_resp.content)
            arrow_tables.append(reader.read_all())

    if not arrow_tables:
        raise RuntimeError("No data chunks returned")
    combined = pa.concat_tables(arrow_tables)
    logger.info("Collected %d rows from %d chunks", combined.num_rows, len(arrow_tables))
    return combined.to_pandas()


def publish_to_hf_hub(df: pd.DataFrame, hf_token: str) -> str:
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)
    api.create_repo(DATASET_REPO, exist_ok=True, repo_type="dataset", token=hf_token)

    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)
        # Partition by match_id for efficient downstream loading by match.
        for match_id, sub_df in df.groupby("match_id"):
            partition_dir = staging_dir / f"match_id={match_id}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            sub_df.drop(columns=["match_id"]).to_parquet(
                partition_dir / "data.parquet",
                index=False,
                engine="pyarrow",
            )
        api.upload_folder(
            folder_path=str(staging_dir),
            path_in_repo="data",
            repo_id=DATASET_REPO,
            repo_type="dataset",
            token=hf_token,
            delete_patterns=["data/*"],
        )
    url = f"https://huggingface.co/datasets/{DATASET_REPO}"
    logger.info("Published %s", url)
    return url


def main() -> None:
    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        from huggingface_hub import get_token

        hf_token = get_token() or ""
    if not hf_token:
        raise RuntimeError("HF_TOKEN required")

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "").rstrip("/")
    db_token = os.environ["DATABRICKS_TOKEN"]
    warehouse_id = os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]

    df = query_databricks_sql(host, db_token, _INPUTS_SQL, warehouse_id)
    if df.empty:
        raise RuntimeError("Query returned 0 rows — verify idsse_events + elastic_sync_results are populated")
    logger.info("Retrieved %s rows across %s matches", f"{len(df):,}", df["match_id"].nunique())

    url = publish_to_hf_hub(df, hf_token)
    upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_hf_card_path("obso-pausa-inputs.md", kind="dataset"),
        hf_token=hf_token,
    )
    logger.info("Pipeline complete: %s", url)


if __name__ == "__main__":
    main()
