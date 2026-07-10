# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.75-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish shot data from Databricks gold layer to HF Hub.

Queries Databricks SQL Statement Execution API to extract shot records from
the gold-layer ``fct_shots`` table, applies dtype normalization, and publishes
as a Parquet dataset partitioned by ``data_source`` on HF Hub.

The published dataset is the primary input for both the v1 XGBoost xG training
script (``train_xg_model_hf.py``) and the v2 Deep Sets xG training script
(``train_xg_v2_hf.py``).

Columns published:
    shot_id          - surrogate key (links to freeze-frame dataset for v2 training)
    shot_id          - surrogate key (dbt_utils.generate_surrogate_key)
    match_key        - Kimball surrogate BIGINT FK to dim_matches (ADR-011; primary match id as of 2026-04-22)
    match_id         - integer match identifier (DEPRECATED 2026-04-22; removed >= 2026-07-22 per ADR-013)
    competition_id   - NULL for Wyscout shots (no StatsBomb match join)
    season_id        - NULL for Wyscout shots
    player_id        - player identifier
    player_key       - Kimball surrogate BIGINT FK to dim_players (PR 7, ADR-011)
    team_id          - team identifier
    team_key         - Kimball surrogate BIGINT FK to dim_teams (PR 7, ADR-011)
    period           - match period (1, 2, ET...)
    minute           - match minute
    second           - second within minute
    location_x       - shot x coordinate (StatsBomb: 120-yard pitch)
    location_y       - shot y coordinate (StatsBomb: 80-yard pitch)
    end_location_x   - shot destination x
    end_location_y   - shot destination y
    shot_outcome     - categorical: Goal, Saved, Blocked, etc.
    shot_body_part   - categorical: Right Foot, Left Foot, Head
    shot_technique   - categorical: Normal, Volley, Half Volley, etc.
    shot_type        - categorical: Open Play, Free Kick, Corner, Penalty
    is_goal          - boolean target variable (derived from shot_outcome = 'Goal')
    distance_to_goal - Euclidean distance to goal centre (yards)
    shot_angle       - angle subtended by goal posts from shot location (radians)
    is_first_time    - boolean: shot taken without prior control
    play_pattern     - categorical: Regular Play, From Counter, etc.
    statsbomb_xg     - StatsBomb proprietary xG (NULL for Wyscout; benchmark label)
    data_source      - partition key: 'statsbomb' or 'wyscout'

Usage (HF Jobs CLI):
    hf jobs uv run scripts/publish_xg_shots_hf.py \\
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

from ingestion.hf_leak_guard import assert_no_private_leak
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
DATASET_REPO = f"{HF_ORG}/xg-shot-data"

# SQL to extract shot features from the gold-layer fact table.
#
# fct_shots already contains competition_id and season_id from the dbt
# LEFT JOIN to stg_statsbomb__matches — Wyscout rows will have these as NULL,
# which is expected and handled by both training scripts.
#
# is_goal is a pre-computed column in fct_shots (not re-derived here) to
# ensure perfect alignment with the dbt gold contract.
_SHOTS_SQL = """\
SELECT
    s.shot_id,
    s.match_key,
    -- try_cast (not plain CAST) avoids Spark cast-pushdown failures on non-BIGINT
    -- native IDs (IDSSE 'J03WOY', Metrica). fct_shots is SB+WS today so this
    -- is dormant; the guard exists for ADR-011's cross-provider direction.
    try_cast(dm.native_match_id as bigint)                 as match_id,
    s.competition_id,
    s.season_id,
    s.player_id,
    s.player_key,                -- new: Kimball surrogate (PR 7, ADR-011)
    s.team_id,
    s.team_key,                  -- new: Kimball surrogate (PR 7, ADR-011)
    s.period,
    s.minute,
    s.second,
    s.location_x,
    s.location_y,
    s.end_location_x,
    s.end_location_y,
    s.shot_outcome,
    s.shot_body_part,
    s.shot_technique,
    s.shot_type,
    s.is_goal,
    s.distance_to_goal,
    s.shot_angle,
    s.is_first_time,
    s.play_pattern,
    s.statsbomb_xg,
    s.data_source,
    -- Per-match HF redistribution tier (spec §6.7/D11). fct_shots carries no SkillCorner today
    -- (safe-by-absence); derived from dim_matches so the fail-closed leak guard halts the publish
    -- if a restricted match ever appears. NULL (unmatched) → guard fails closed.
    dm.access_tier
FROM soccer_analytics.dev_gold.fct_shots s
LEFT JOIN soccer_analytics.dev_gold.dim_matches dm
    ON s.match_key = dm.match_key
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
    result chunks using EXTERNAL_LINKS disposition with Arrow stream format.

    Args:
        host: Databricks workspace hostname (no protocol prefix).
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
# dtype normalization
# ---------------------------------------------------------------------------


def normalize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Apply correct dtypes to shot columns after Arrow deserialization.

    Args:
        df: Raw DataFrame from the Databricks SQL API response.

    Returns:
        DataFrame with enforced dtypes matching the HF Hub schema contract.
    """
    # String columns (IDs and categoricals)
    for col in (
        "shot_id",
        "shot_outcome",
        "shot_body_part",
        "shot_technique",
        "shot_type",
        "play_pattern",
        "data_source",
    ):
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Nullable integer columns (competition_id/season_id are NULL for Wyscout).
    # match_key (Kimball surrogate BIGINT) added 2026-04-22; match_id kept for
    # 90-day deprecation window per ADR-013 dual-column policy.
    for col in ("match_key", "match_id", "player_id", "team_id", "period", "minute", "second"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ("competition_id", "season_id"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Float columns
    for col in (
        "location_x",
        "location_y",
        "end_location_x",
        "end_location_y",
        "distance_to_goal",
        "shot_angle",
        "statsbomb_xg",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # Boolean columns
    for col in ("is_goal", "is_first_time"):
        if col in df.columns:
            # Handle both numeric (0/1) and string ('true'/'false') representations
            df[col] = df[col].map(lambda v: bool(int(v)) if str(v).isdigit() else str(v).lower() == "true").astype(bool)

    return df


# ---------------------------------------------------------------------------
# HF Hub publishing
# ---------------------------------------------------------------------------


def publish_to_hf_hub(df: pd.DataFrame, hf_token: str) -> str:
    """Write shot data as partitioned Parquet and upload to HF Hub.

    Data is partitioned by ``data_source`` (e.g., ``data_source=statsbomb``,
    ``data_source=wyscout``) to allow training scripts to load subsets
    efficiently by source.

    Args:
        df: Shot DataFrame to publish.
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

        # Write partitioned Parquet files (one per data_source)
        for source, source_df in df.groupby("data_source"):
            partition_dir = staging_dir / f"data_source={source}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            # Drop the partition column from the file (it's encoded in the path)
            partition_df = source_df.drop(columns=["data_source"])
            out_path = partition_dir / "data.parquet"
            partition_df.to_parquet(out_path, index=False, engine="pyarrow")
            logger.info(
                "Wrote partition data_source=%s: %d rows -> %s",
                source,
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
    """Extract shot data from Databricks gold layer and publish to HF Hub."""
    logger.info("Starting xG shot data publication pipeline")

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
    # 2. Query shot data from Databricks gold layer
    # ------------------------------------------------------------------
    logger.info("Querying shot data from soccer_analytics.dev_gold.fct_shots")
    raw_df = query_databricks_sql(
        host=databricks_host,
        token=databricks_token,
        sql=_SHOTS_SQL,
        warehouse_id=warehouse_id,
    )

    if raw_df.empty:
        raise RuntimeError("Query returned no rows — check that fct_shots has been built by dbt")

    logger.info("Retrieved %d raw shot rows", len(raw_df))

    # ------------------------------------------------------------------
    # 3. Normalize dtypes
    # ------------------------------------------------------------------
    logger.info("Normalizing column dtypes")
    shots_df = normalize_dtypes(raw_df)

    if shots_df.empty:
        raise RuntimeError("No shots remain after dtype normalization")

    # Summary statistics
    n_goals = int(shots_df["is_goal"].sum())
    n_matches = shots_df["match_id"].nunique()
    source_counts = shots_df["data_source"].value_counts().to_dict()
    logger.info(
        "Shot summary: %d shots, %d goals (%.1f%%), %d matches, by source: %s",
        len(shots_df),
        n_goals,
        100.0 * n_goals / max(len(shots_df), 1),
        n_matches,
        source_counts,
    )

    # Warn if competition_id is NULL for any statsbomb rows (unexpected)
    sb_mask = shots_df["data_source"] == "statsbomb"
    sb_null_comp = shots_df.loc[sb_mask, "competition_id"].isna().sum()
    if sb_null_comp > 0:
        logger.warning(
            "%d StatsBomb shots have NULL competition_id — check stg_statsbomb__matches join in fct_shots",
            sb_null_comp,
        )

    # ------------------------------------------------------------------
    # 4. Publish to HF Hub
    # ------------------------------------------------------------------
    # Fail-closed leak guard (spec §6.7/D11): this mart carries no SkillCorner today, but the guard
    # halts the publish (rather than leaking) if a restricted row ever appears. Drop the internal
    # access_tier column AFTER the guard, before upload (R2).
    assert_no_private_leak(shots_df, publisher="publish_xg_shots_hf")
    shots_df = shots_df.drop(columns=["access_tier"], errors="ignore")

    logger.info("Publishing shot data to HF Hub: %s", DATASET_REPO)
    dataset_url = publish_to_hf_hub(shots_df, hf_token)

    # ------------------------------------------------------------------
    # 5. Publish README alongside data (PR 4c)
    # ------------------------------------------------------------------
    readme_result = upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_hf_card_path("xg-shot-data.md", kind="dataset"),
        hf_token=hf_token,
    )
    logger.info(
        "Uploaded README: %s (sha256=%s)",
        readme_result["commit_url"],
        readme_result["sha256"][:8],
    )

    logger.info("Pipeline complete. Dataset: %s", dataset_url)
    logger.info(
        "Final stats: %d shots, %d goals, %d matches, sources: %s",
        len(shots_df),
        n_goals,
        n_matches,
        source_counts,
    )


if __name__ == "__main__":
    main()
