"""Export on-target shots from Databricks gold layer to HF Hub.

Reads ``fct_shots`` from the Unity Catalog gold layer, filters to true on-target
shots (``shot_outcome IN ('Goal','Saved','Post','Saved to Post')`` with a non-null
``end_location_z`` coordinate guard), writes a Parquet staging file to a UC Volume
path, and uploads the dataset to HF Hub.

This script is a D39 prerequisite — the exported dataset is the primary input
for PSxG (Post-Shot Expected Goals) model training on HF Jobs.

Columns exported:
    event_id         - shot surrogate key (aliased from shot_id)
    match_key        - Kimball surrogate BIGINT FK to dim_matches (ADR-011; primary match id as of 2026-04-22)
    match_id         - string match identifier (DEPRECATED 2026-04-22; removed on or after 2026-07-22 per ADR-013)
    player_id        - player identifier
    player_key       - Kimball surrogate BIGINT FK to dim_players (PR 7, ADR-011)
    team_id          - team identifier
    team_key         - Kimball surrogate BIGINT FK to dim_teams (PR 7, ADR-011)
    end_location_y   - shot destination y (vertical position on goal face)
    end_location_z   - shot destination z (height on goal face)
    shot_outcome     - categorical: Goal, Saved, Blocked, etc.
    is_goal          - integer target variable (1 = Goal, 0 = otherwise)

Usage (Databricks workflow task):
    python scripts/export_shots_on_target.py \\
        --catalog soccer_analytics \\
        --schema dev_gold \\
        --volume-path /Volumes/soccer_analytics/dev_gold/model_weights/psxg
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import TYPE_CHECKING

from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from ingestion.utils import resolve_hf_token
from shared.constants import IDENTIFIER_RE
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HF_ORG = "luxury-lakehouse"
DATASET_REPO = f"{HF_ORG}/statsbomb-shots-on-target"

# Parquet output filename within the staging volume path
_PARQUET_FILENAME = "shots_on_target.parquet"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_identifier(field_name: str, value: str) -> None:
    """Validate a SQL identifier against the safe-name pattern.

    Args:
        field_name: Human-readable name for error messages (e.g. "catalog").
        value: The identifier string to validate.

    Raises:
        SystemExit: If the value does not match the safe identifier pattern.
    """
    if not IDENTIFIER_RE.match(value):
        msg = f"Invalid {field_name} '{value}': must match {IDENTIFIER_RE.pattern}"
        raise SystemExit(msg)


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


def _build_query(catalog: str, schema: str) -> str:
    """Build the SQL query for on-target shots.

    On-target = ``shot_outcome IN ('Goal','Saved','Post','Saved to Post')``
    (D-0, spec 2026-06-20-psxg-tracking-extension). The prior
    ``end_location_z IS NOT NULL`` filter was ~46% off-target (``Off T`` carries
    a recorded end-z too), which contaminated the PSxG training population.
    ``Post`` / ``Saved to Post`` are kept because P-1 verified the tracking
    ``shot_on_target_derived`` geometry counts post/bar strikes as on-target —
    so the two modalities share one definition. ``end_location_z IS NOT NULL``
    is retained as a coordinate-usability guard (the model needs the height).

    Args:
        catalog: Unity Catalog name (already validated).
        schema: Schema name (already validated).

    Returns:
        SQL string selecting on-target shot columns.
    """
    # catalog and schema are validated by _validate_identifier before this call
    return f"""\
SELECT
    s.shot_id                                              AS event_id,
    s.match_key,
    CAST(dm.native_match_id AS STRING)                     AS match_id,
    s.player_id,
    s.player_key,
    s.team_id,
    s.team_key,
    s.end_location_y,
    s.end_location_z,
    s.shot_outcome,
    CASE WHEN s.shot_outcome = 'Goal' THEN 1 ELSE 0 END    AS is_goal
FROM {catalog}.{schema}.fct_shots s
LEFT JOIN {catalog}.{schema}.dim_matches dm
    ON s.match_key = dm.match_key
WHERE s.shot_outcome IN ('Goal', 'Saved', 'Post', 'Saved to Post')
  AND s.end_location_z IS NOT NULL
"""  # noqa: S608


# ---------------------------------------------------------------------------
# HF Hub upload
# ---------------------------------------------------------------------------


def _upload_to_hf_hub(volume_path: str) -> str:
    """Upload staged Parquet from UC Volume to HF Hub."""
    from ingestion.utils import upload_volume_to_hf_hub

    return upload_volume_to_hf_hub(volume_path, DATASET_REPO, logger=logger)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


@workflow("wf-export-shots", phase="export")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    volume_path: str,
    *,
    ctx: object = None,
) -> int:
    """Query on-target shots from gold layer, stage to UC Volume, upload to HF Hub."""
    # ------------------------------------------------------------------
    # 1. Query on-target shots from gold layer
    # ------------------------------------------------------------------
    sql = _build_query(catalog, schema)
    source_table = f"{catalog}.{schema}.fct_shots"
    logger.info("Querying on-target shots from %s", source_table)

    start = time.time()
    shots_df = spark.sql(sql)
    row_count = int(shots_df.count())
    elapsed = time.time() - start

    logger.info("Retrieved %d on-target shot rows in %.2fs", row_count, elapsed)

    if row_count == 0:
        raise RuntimeError(
            f"No on-target shots found in {source_table} "
            "(shot_outcome IN ('Goal','Saved','Post','Saved to Post') AND end_location_z IS NOT NULL "
            "returned zero rows — check that fct_shots has been built by dbt)"
        )

    # ------------------------------------------------------------------
    # 2. Write Parquet to UC Volume staging path
    # ------------------------------------------------------------------
    parquet_path = f"{volume_path}/{_PARQUET_FILENAME}"
    logger.info("Writing %d rows to %s", row_count, parquet_path)

    start = time.time()
    shots_df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
    elapsed = time.time() - start
    logger.info("Parquet write complete in %.2fs", elapsed)

    # ------------------------------------------------------------------
    # 3. Upload to HF Hub
    # ------------------------------------------------------------------
    logger.info("Uploading to HF Hub dataset: %s", DATASET_REPO)
    dataset_url = _upload_to_hf_hub(parquet_path)

    # ------------------------------------------------------------------
    # 4. Upload README alongside data (PR 4c).
    # ------------------------------------------------------------------
    # ``upload_volume_to_hf_hub`` writes a file:// URL when no HF token is
    # available; skip README in that case to match the data-side behaviour.
    if not dataset_url.startswith("file://"):
        hf_token = resolve_hf_token()
        readme_result = upload_hf_readme(
            repo_id=DATASET_REPO,
            readme_path=get_hf_card_path("statsbomb-shots-on-target.md", kind="dataset"),
            hf_token=hf_token,
        )
        logger.info(
            "Uploaded README: %s (sha256=%s)",
            readme_result["commit_url"],
            readme_result["sha256"][:8],
        )

    logger.info("Pipeline complete. Dataset: %s", dataset_url)
    logger.info("Final stats: %d on-target shots exported", row_count)
    return row_count


def main() -> None:
    """CLI entry point for on-target shots export."""
    # Late import — PySpark only available in Databricks runtime
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    logger.info("Starting on-target shots export pipeline")

    parser = argparse.ArgumentParser(description="Export on-target shots to HF Hub")
    parser.add_argument(
        "--catalog",
        default="soccer_analytics",
        help="Unity Catalog name (default: soccer_analytics)",
    )
    parser.add_argument(
        "--schema",
        default="dev_gold",
        help="Schema name (default: dev_gold)",
    )
    parser.add_argument(
        "--volume-path",
        default="/Volumes/soccer_analytics/dev_gold/model_weights/psxg",
        help="UC Volume staging path for Parquet output",
    )
    args = parser.parse_args()

    catalog: str = args.catalog
    schema: str = args.schema
    volume_path: str = args.volume_path.rstrip("/")

    # Validate SQL identifiers before interpolating into queries
    _validate_identifier("catalog", catalog)
    _validate_identifier("schema", schema)

    spark = SparkSession.builder.getOrCreate()  # type: ignore[attr-defined]

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, catalog, schema)

    run_pipeline(spark, catalog, schema, volume_path)


if __name__ == "__main__":
    main()
