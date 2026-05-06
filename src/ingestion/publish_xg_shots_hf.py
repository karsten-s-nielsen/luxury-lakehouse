"""Publish xG shot data from Databricks gold layer to HF Hub.

Databricks-runtime counterpart of ``scripts/publish_xg_shots_hf.py``.
Reads from ``fct_shots`` via ``spark.sql()``, writes partitioned Parquet
to a temp directory, and uploads to HF Hub as ``luxury-lakehouse/xg-shot-data``.

Wired into the daily pipeline via ``src/ingestion/hf_sync.py`` sub-operations.
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
DATASET_REPO = f"{HF_ORG}/xg-shot-data"

_SHOTS_SQL = """\
SELECT
    s.shot_id,
    s.match_key,
    try_cast(dm.native_match_id as bigint) as match_id,
    s.competition_id,
    s.season_id,
    s.player_id,
    s.player_key,
    s.team_id,
    s.team_key,
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
    s.data_source
FROM {catalog}.{schema}.fct_shots s
LEFT JOIN {catalog}.{schema}.dim_matches dm
    ON s.match_key = dm.match_key
"""


@workflow("wf-publish-xg-shots", phase="export")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    pipeline_logger: logging.Logger,
    *,
    ctx: object | None = None,
) -> int:
    """Query shot data from gold layer and publish to HF Hub."""
    _ = ctx
    from huggingface_hub import HfApi, get_token

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN required for HF Hub upload")

    sql = _SHOTS_SQL.format(catalog=catalog, schema=schema)
    pipeline_logger.info("Querying fct_shots from %s.%s", catalog, schema)
    df = spark.sql(sql).toPandas()
    row_count = len(df)
    pipeline_logger.info("Retrieved %d shot rows", row_count)

    if row_count == 0:
        raise RuntimeError("Query returned no rows — check that fct_shots has been built by dbt")

    api = HfApi(token=hf_token)
    api.create_repo(DATASET_REPO, exist_ok=True, repo_type="dataset", token=hf_token)

    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)

        for source, source_df in df.groupby("data_source"):
            partition_dir = staging_dir / f"data_source={source}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            partition_df = source_df.drop(columns=["data_source"])
            out_path = partition_dir / "data.parquet"
            partition_df.to_parquet(out_path, index=False, engine="pyarrow")
            pipeline_logger.info("Wrote partition data_source=%s: %d rows", source, len(partition_df))

        api.upload_folder(
            folder_path=str(staging_dir),
            path_in_repo="data",
            repo_id=DATASET_REPO,
            repo_type="dataset",
            token=hf_token,
            delete_patterns=["data/*"],
        )

    readme_result = upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_hf_card_path("xg-shot-data.md", kind="dataset"),
        hf_token=hf_token,
    )
    pipeline_logger.info(
        "Published %d rows to %s (README sha256=%s)",
        row_count,
        DATASET_REPO,
        readme_result["sha256"][:8],
    )
    return row_count


def main() -> None:
    """CLI entry point for xG shots publisher."""
    configure_logging("publish_xg_shots_hf")
    args = parse_ingestion_args("Publish xG shot data to HF Hub")
    spark = get_spark_session()
    run_pipeline(spark, args.catalog, args.schema, logger)
