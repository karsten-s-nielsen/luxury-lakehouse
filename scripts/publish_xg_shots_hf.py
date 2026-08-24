# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.101-py3-none-any.whl",
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

The published dataset is a historical training input; the canonical pre-shot xG
model (xg_model_v3) now trains from the SPADL-native corpora (xg-shot-data-v3 +
xg-shot-freeze-frames) via ``scripts/train_xg_v3_hf.py``.

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
from pathlib import Path

import pandas as pd

from analytics.databricks_sql_fetch import query_databricks_sql
from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from ingestion.hf_upload_seam import GuardedFrame, prepare_public_upload, upload_guarded

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


def publish_to_hf_hub(guarded: GuardedFrame, hf_token: str) -> str:
    """Write shot data as partitioned Parquet and upload to HF Hub.

    Data is partitioned by ``data_source`` (e.g., ``data_source=statsbomb``,
    ``data_source=wyscout``) to allow training scripts to load subsets
    efficiently by source.

    Args:
        guarded: Shot frame that passed the ADR-072 seam guard.
        hf_token: HuggingFace API token.

    Returns:
        URL of the published dataset.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)

        # Write partitioned Parquet files (one per data_source). The partition column is dropped
        # from the file because it is encoded in the path.
        for source, sub in guarded.groupby("data_source"):
            sub.drop_columns(["data_source"]).write_parquet(staging_dir / f"data_source={source}" / "data.parquet")

        dataset_url = upload_guarded(staging_dir, frames=[guarded], repo_id=DATASET_REPO, token=hf_token)

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
    prepared = prepare_public_upload(shots_df, publisher="publish_xg_shots_hf")

    logger.info("Publishing shot data to HF Hub: %s", DATASET_REPO)
    dataset_url = publish_to_hf_hub(prepared.public, hf_token)

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
