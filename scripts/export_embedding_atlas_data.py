# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.102-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
#     "umap-learn>=0.5.4",
#     "scikit-learn>=1.3.0",
# ]
# ///
"""Export player embeddings for the Embedding Explorer static HF Space.

Queries Databricks SQL for season-level and career-level player embeddings,
computes UMAP 2D projections and k-NN neighbor lists, then publishes as
Parquet on HF Hub for consumption by the Embedding Atlas static viewer.

The published dataset is consumed by the ``luxury-lakehouse/embedding-explorer``
HF Space which uses Apple's Embedding Atlas + DuckDB-WASM for in-browser
visualization.

Output columns:
    row_id           - unique row identifier (canonical_player_id + level + comp + season)
    canonical_player_id - player identifier (links to dim_players)
    player_name      - human-readable display name
    position_group   - Goalkeeper, Defender, Midfielder, Forward
    primary_position - more specific position label
    level            - "season" or "career"
    competition_id   - competition (NULL for career)
    season_id        - season (NULL for career)
    matches          - number of matches in the embedding sample
    data_sources     - comma-separated list of source providers
    x                - UMAP 2D x projection
    y                - UMAP 2D y projection
    neighbors        - JSON string: {"indices": [...], "distances": [...]}

Usage (HF Jobs CLI):
    hf jobs uv run scripts/export_embedding_atlas_data.py \\
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
from pathlib import Path

import numpy as np
import pandas as pd

from analytics.databricks_sql_fetch import query_databricks_sql

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
DATASET_REPO = f"{HF_ORG}/embedding-explorer-data"

CATALOG = "soccer_analytics"
GOLD_SCHEMA = "dev_gold"

UMAP_N_NEIGHBORS = 15
UMAP_MIN_DIST = 0.1
KNN_NEIGHBORS = 50


# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

_SEASON_SQL = f"""\
SELECT
    CAST(e.canonical_player_id AS STRING) AS canonical_player_id,
    dp.player_display_name AS player_name,
    dp.position_group,
    dp.primary_position,
    CAST(e.competition_id AS STRING) AS competition_id,
    CAST(e.season_id AS STRING) AS season_id,
    e.matches_in_sample AS matches,
    e.data_sources,
    e.behavioral_vector
FROM {CATALOG}.{GOLD_SCHEMA}.fct_player_embeddings_season e
INNER JOIN {CATALOG}.{GOLD_SCHEMA}.dim_players dp
    ON CAST(e.canonical_player_id AS INT) = dp.canonical_player_id
WHERE e.behavioral_vector IS NOT NULL
"""  # noqa: S608

_CAREER_SQL = f"""\
SELECT
    CAST(e.canonical_player_id AS STRING) AS canonical_player_id,
    dp.player_display_name AS player_name,
    dp.position_group,
    dp.primary_position,
    NULL AS competition_id,
    NULL AS season_id,
    e.total_matches AS matches,
    e.data_sources,
    e.behavioral_vector
FROM {CATALOG}.{GOLD_SCHEMA}.fct_player_embeddings_career e
INNER JOIN {CATALOG}.{GOLD_SCHEMA}.dim_players dp
    ON CAST(e.canonical_player_id AS INT) = dp.canonical_player_id
WHERE e.behavioral_vector IS NOT NULL
"""  # noqa: S608


# ---------------------------------------------------------------------------
# Vector parsing
# ---------------------------------------------------------------------------


def parse_vectors(df: pd.DataFrame, col: str = "behavioral_vector") -> np.ndarray:
    """Parse string-encoded vectors to a 2D numpy array.

    Handles both JSON array strings (``[0.1, 0.2, ...]``) and
    WrappedArray/list representations from Databricks Arrow serialization.
    """
    vectors: list[list[float]] = []
    for raw in df[col]:
        if isinstance(raw, (list, np.ndarray)):
            vectors.append([float(v) for v in raw])
        elif isinstance(raw, str):
            cleaned = raw.strip("[]").replace(" ", "")
            vectors.append([float(v) for v in cleaned.split(",") if v])
        else:
            raise ValueError(f"Unexpected vector type: {type(raw)}")
    return np.array(vectors, dtype=np.float32)


# ---------------------------------------------------------------------------
# UMAP + k-NN
# ---------------------------------------------------------------------------


def compute_projections(vectors: np.ndarray) -> np.ndarray:
    """Compute 2D UMAP projection from high-dimensional embedding vectors."""
    import umap

    logger.info("Running UMAP on %d vectors of dim %d", vectors.shape[0], vectors.shape[1])
    reducer = umap.UMAP(
        n_neighbors=UMAP_N_NEIGHBORS,
        min_dist=UMAP_MIN_DIST,
        n_components=2,
        metric="cosine",
        random_state=42,
    )
    projection = reducer.fit_transform(vectors)
    logger.info("UMAP complete: output shape %s", projection.shape)
    return projection


def compute_neighbors(vectors: np.ndarray, k: int = KNN_NEIGHBORS) -> list[str]:
    """Compute k-NN neighbor lists as JSON strings for Embedding Atlas.

    Returns a list of JSON strings, one per row, in the format:
    ``{"indices": [i1, i2, ...], "distances": [d1, d2, ...]}``
    """
    from sklearn.neighbors import NearestNeighbors

    logger.info("Computing %d-NN on %d vectors", k, vectors.shape[0])
    # Use min(k+1, n) to handle small datasets
    actual_k = min(k + 1, vectors.shape[0])
    nn = NearestNeighbors(n_neighbors=actual_k, metric="cosine", algorithm="brute")
    nn.fit(vectors)
    distances, indices = nn.kneighbors(vectors)

    neighbor_jsons: list[str] = []
    for i in range(vectors.shape[0]):
        # Skip self (index 0) — the nearest neighbor is the point itself
        neighbor_indices = indices[i, 1:].tolist()
        neighbor_distances = [round(float(d), 6) for d in distances[i, 1:]]
        neighbor_jsons.append(json.dumps({"indices": neighbor_indices, "distances": neighbor_distances}))

    logger.info("k-NN complete")
    return neighbor_jsons


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Export embedding data with UMAP projections to HF Hub."""
    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
    hf_token = os.environ.get("HF_TOKEN", "")

    if not all([host, token, warehouse_id]):
        logger.error("Missing required env vars: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_SQL_WAREHOUSE_ID")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 1. Fetch embedding data
    # ------------------------------------------------------------------
    logger.info("Fetching season-level embeddings")
    season_df = query_databricks_sql(host, token, _SEASON_SQL, warehouse_id)
    season_df["level"] = "season"
    logger.info("Season embeddings: %d rows", len(season_df))

    logger.info("Fetching career-level embeddings")
    career_df = query_databricks_sql(host, token, _CAREER_SQL, warehouse_id)
    career_df["level"] = "career"
    logger.info("Career embeddings: %d rows", len(career_df))

    df = pd.concat([season_df, career_df], ignore_index=True)
    logger.info("Combined embeddings: %d rows", len(df))

    if df.empty:
        logger.error("No embeddings found — aborting")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 2. Parse vectors and compute projections
    # ------------------------------------------------------------------
    vectors = parse_vectors(df)
    projection = compute_projections(vectors)
    df["x"] = projection[:, 0].astype(float)
    df["y"] = projection[:, 1].astype(float)

    # ------------------------------------------------------------------
    # 3. Compute k-NN neighbors
    # ------------------------------------------------------------------
    df["neighbors"] = compute_neighbors(vectors)

    # ------------------------------------------------------------------
    # 4. Build row_id and clean up
    # ------------------------------------------------------------------
    df["row_id"] = (
        df["canonical_player_id"].astype(str)
        + "_"
        + df["level"]
        + "_"
        + df["competition_id"].fillna("all").astype(str)
        + "_"
        + df["season_id"].fillna("all").astype(str)
    )

    # Convert data_sources array to string
    df["data_sources"] = df["data_sources"].apply(
        lambda x: ",".join(x) if isinstance(x, list) else (str(x) if x else "")
    )

    # Drop the raw vector column (too large for the explorer)
    output_cols = [
        "row_id",
        "canonical_player_id",
        "player_name",
        "position_group",
        "primary_position",
        "level",
        "competition_id",
        "season_id",
        "matches",
        "data_sources",
        "x",
        "y",
        "neighbors",
    ]
    output_df = df[output_cols].copy()

    # ------------------------------------------------------------------
    # 5. Write Parquet and upload to HF Hub
    # ------------------------------------------------------------------
    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = Path(tmpdir) / "embeddings.parquet"
        output_df.to_parquet(parquet_path, index=False, engine="pyarrow")
        file_size_mb = parquet_path.stat().st_size / (1024 * 1024)
        logger.info("Wrote %s (%.1f MB, %d rows)", parquet_path.name, file_size_mb, len(output_df))

        if hf_token:
            from huggingface_hub import HfApi

            api = HfApi(token=hf_token)
            api.create_repo(repo_id=DATASET_REPO, repo_type="dataset", exist_ok=True, private=False)
            api.upload_file(
                path_or_fileobj=str(parquet_path),
                path_in_repo="data/embeddings.parquet",
                repo_id=DATASET_REPO,
                repo_type="dataset",
                commit_message="Update embedding atlas data with UMAP projections",
            )
            logger.info("Uploaded to %s", DATASET_REPO)
        else:
            logger.warning(  # nosemgrep: python-logger-credential-disclosure -- logs env var NAME, not value
                "HF_TOKEN not set — skipping upload. Parquet at: %s", parquet_path
            )

    logger.info("Export complete: %d rows", len(output_df))


if __name__ == "__main__":
    main()
