# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.96-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish shot freeze-frame positions from Databricks to HF Hub.

Queries Databricks SQL Statement Execution API to extract freeze-frame
player positions from StatsBomb events, normalizes coordinates, and
publishes as a Parquet dataset on HF Hub.

Reference: StatsBomb open data — shot freeze frames contain player
positions at the moment of each shot (CC-BY 4.0).

Usage (HF Jobs CLI):
    hf jobs uv run scripts/publish_freeze_frame_hf.py \\
        --flavor cpu-basic --timeout 30m \\
        --secrets HF_TOKEN \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN \\
        --env DATABRICKS_SQL_WAREHOUSE_ID=$DATABRICKS_SQL_WAREHOUSE_ID
"""

from __future__ import annotations

import json
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
DATASET_REPO = f"{HF_ORG}/xg-freeze-frame-data"

# StatsBomb coordinate system: 120 x 80 yards
STATSBOMB_PITCH_LENGTH = 120.0
STATSBOMB_PITCH_WIDTH = 80.0

# SQL to extract shot freeze frames from the silver events table
_FREEZE_FRAME_SQL = """\
SELECT
    e.event_id,
    e.match_id,
    m.competition_id,
    m.season_id,
    e.shot_freeze_frame
FROM soccer_analytics.dev_silver.stg_statsbomb__events e
INNER JOIN soccer_analytics.dev_silver.stg_statsbomb__matches m
    ON e.match_id = m.match_id
WHERE e.event_type = 'Shot'
  AND e.shot_freeze_frame IS NOT NULL
  AND e.shot_freeze_frame != '[]'
"""

# Databricks SQL Statement Execution API polling interval (seconds)
_POLL_INTERVAL_S = 2.0

# HTTP timeouts: (connect, read) in seconds
_TIMEOUT_SUBMIT = (10, 120)
_TIMEOUT_POLL = (10, 30)
_TIMEOUT_CHUNK = (10, 60)


# ---------------------------------------------------------------------------
# Databricks SQL Statement Execution API
# ---------------------------------------------------------------------------


def query_databricks_sql(host: str, token: str, sql: str, warehouse_id: str) -> pd.DataFrame:
    """Execute SQL via Databricks Statement Execution API and return DataFrame.

    Handles asynchronous execution (PENDING/RUNNING states) and paginated
    result chunks. All values are returned as strings by the API and must
    be cast by the caller.

    Args:
        host: Databricks workspace hostname (no protocol prefix).
        token: Databricks personal access token or OAuth token.
        sql: SQL statement to execute.
        warehouse_id: SQL warehouse ID to target.

    Returns:
        DataFrame with string columns matching the SQL result schema.

    Raises:
        RuntimeError: If the SQL statement fails on the server.
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

    # Extract column names from manifest
    manifest = result.get("manifest", {})
    columns = [col["name"] for col in manifest.get("schema", {}).get("columns", [])]
    total_row_count = manifest.get("total_row_count", "unknown")
    total_chunk_count = manifest.get("total_chunk_count", 0)
    logger.info("Query returned %s rows in %s chunks, columns: %s", total_row_count, total_chunk_count, columns)

    # EXTERNAL_LINKS disposition: fetch each chunk via /result/chunks/{index}
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
# Freeze-frame parsing
# ---------------------------------------------------------------------------


def parse_freeze_frames(df: pd.DataFrame) -> pd.DataFrame:
    """Explode shot_freeze_frame JSON into per-player rows with normalized coordinates.

    StatsBomb coordinates (120 x 80) are normalized to [0, 1] on both axes.

    Args:
        df: DataFrame with columns event_id, match_id, competition_id,
            season_id, shot_freeze_frame (JSON string).

    Returns:
        DataFrame with one row per player per shot, containing normalized
        positions and player role flags.
    """
    rows: list[dict[str, object]] = []
    parse_errors = 0

    for _, shot in df.iterrows():
        try:
            players = json.loads(shot["shot_freeze_frame"])
            if not isinstance(players, list):
                parse_errors += 1
                continue

            for p in players:
                loc = p.get("location", [0.0, 0.0])
                if not isinstance(loc, list) or len(loc) < 2:
                    continue

                rows.append(
                    {
                        "event_id": str(shot["event_id"]),
                        "match_id": int(shot["match_id"]),
                        "competition_id": int(shot["competition_id"]),
                        "season_id": int(shot["season_id"]),
                        "player_x_norm": float(loc[0]) / STATSBOMB_PITCH_LENGTH,
                        "player_y_norm": float(loc[1]) / STATSBOMB_PITCH_WIDTH,
                        "is_keeper": bool(p.get("keeper", False)),
                        "is_teammate": bool(p.get("teammate", False)),
                    }
                )
        except (json.JSONDecodeError, TypeError, IndexError, ValueError):
            parse_errors += 1
            continue

    if parse_errors > 0:
        logger.warning("Skipped %d shots due to parse errors", parse_errors)

    logger.info("Parsed %d player-position rows from %d shots", len(rows), len(df))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# HF Hub publishing
# ---------------------------------------------------------------------------


def publish_to_hf_hub(df: pd.DataFrame, hf_token: str) -> str:
    """Write freeze-frame data as partitioned Parquet and upload to HF Hub.

    Data is partitioned by competition_id for efficient downstream loading.

    Args:
        df: Freeze-frame DataFrame to publish.
        hf_token: HuggingFace API token.

    Returns:
        URL of the published dataset.
    """
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)

    # Create or reuse the dataset repo
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

        # Write partitioned Parquet files (one per competition_id)
        for comp_id, comp_df in df.groupby("competition_id"):
            partition_dir = staging_dir / f"competition_id={comp_id}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            # Drop the partition column from the file (it's encoded in the path)
            partition_df = comp_df.drop(columns=["competition_id"])
            out_path = partition_dir / "data.parquet"
            partition_df.to_parquet(out_path, index=False, engine="pyarrow")
            logger.info(
                "Wrote partition competition_id=%s: %d rows -> %s",
                comp_id,
                len(partition_df),
                out_path,
            )

        # Upload the entire data/ directory
        api.upload_folder(
            folder_path=str(staging_dir),
            path_in_repo="data",
            repo_id=DATASET_REPO,
            repo_type="dataset",
            token=hf_token,
        )

    dataset_url = f"https://huggingface.co/datasets/{DATASET_REPO}"
    logger.info("Published dataset to %s", dataset_url)
    return dataset_url


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def main() -> None:
    """Extract freeze-frame data from Databricks and publish to HF Hub."""
    logger.info("Starting freeze-frame data publication pipeline")

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
    # 2. Query freeze-frame data from Databricks
    # ------------------------------------------------------------------
    logger.info("Querying freeze-frame data from Databricks")
    raw_df = query_databricks_sql(
        host=databricks_host,
        token=databricks_token,
        sql=_FREEZE_FRAME_SQL,
        warehouse_id=warehouse_id,
    )

    if raw_df.empty:
        raise RuntimeError("Query returned no rows — check that stg_statsbomb__events has shot data")

    logger.info("Retrieved %d shots with freeze frames", len(raw_df))

    # ------------------------------------------------------------------
    # 3. Parse freeze frames into per-player rows
    # ------------------------------------------------------------------
    logger.info("Parsing freeze-frame JSON into player-position rows")
    freeze_df = parse_freeze_frames(raw_df)

    if freeze_df.empty:
        raise RuntimeError("No player positions parsed from freeze frames")

    # Enforce correct dtypes
    freeze_df["event_id"] = freeze_df["event_id"].astype(str)
    freeze_df["match_id"] = freeze_df["match_id"].astype("int64")
    freeze_df["competition_id"] = freeze_df["competition_id"].astype("int64")
    freeze_df["season_id"] = freeze_df["season_id"].astype("int64")
    freeze_df["player_x_norm"] = freeze_df["player_x_norm"].astype("float64")
    freeze_df["player_y_norm"] = freeze_df["player_y_norm"].astype("float64")
    freeze_df["is_keeper"] = freeze_df["is_keeper"].astype(bool)
    freeze_df["is_teammate"] = freeze_df["is_teammate"].astype(bool)

    # Summary statistics
    n_shots = freeze_df["event_id"].nunique()
    n_matches = freeze_df["match_id"].nunique()
    n_competitions = freeze_df["competition_id"].nunique()
    logger.info(
        "Freeze-frame summary: %d player rows from %d shots across %d matches in %d competitions",
        len(freeze_df),
        n_shots,
        n_matches,
        n_competitions,
    )

    # ------------------------------------------------------------------
    # 4. Publish to HF Hub
    # ------------------------------------------------------------------
    logger.info("Publishing freeze-frame data to HF Hub")
    dataset_url = publish_to_hf_hub(freeze_df, hf_token)

    # ------------------------------------------------------------------
    # 5. Publish README alongside data (PR 4c)
    # ------------------------------------------------------------------
    readme_result = upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_hf_card_path("xg-freeze-frame-data.md", kind="dataset"),
        hf_token=hf_token,
    )
    logger.info(
        "Uploaded README: %s (sha256=%s)",
        readme_result["commit_url"],
        readme_result["sha256"][:8],
    )

    logger.info("Pipeline complete. Dataset: %s", dataset_url)
    logger.info(
        "Final stats: %d player-position rows, %d shots, %d matches, %d competitions",
        len(freeze_df),
        n_shots,
        n_matches,
        n_competitions,
    )


if __name__ == "__main__":
    main()
