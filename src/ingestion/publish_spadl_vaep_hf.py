"""Publish SPADL/VAEP action values from Databricks gold layer to HF Hub.

Databricks-runtime counterpart of ``scripts/publish_spadl_vaep_hf.py``.
Reads from ``fct_action_values`` via ``spark.sql()``, writes partitioned
Parquet to a temp directory, and uploads to HF Hub as
``luxury-lakehouse/spadl-vaep-action-values``.

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
DATASET_REPO = f"{HF_ORG}/spadl-vaep-action-values"

_ACTION_VALUES_SQL = """\
SELECT
    action_value_id,
    match_key,
    competition_key,
    match_id,
    competition_id,
    player_id,
    player_key,
    team_id,
    team_key,
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
FROM {catalog}.{schema}.fct_action_values
"""


@workflow("wf-publish-spadl-vaep", phase="export")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    pipeline_logger: logging.Logger,
    *,
    ctx: object | None = None,
) -> int:
    """Query action values from gold layer and publish to HF Hub."""
    _ = ctx
    from huggingface_hub import HfApi, get_token

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN required for HF Hub upload")

    sql = _ACTION_VALUES_SQL.format(catalog=catalog, schema=schema)
    pipeline_logger.info("Querying fct_action_values from %s.%s", catalog, schema)
    df = spark.sql(sql).toPandas()
    row_count = len(df)
    pipeline_logger.info("Retrieved %d action value rows", row_count)

    if row_count == 0:
        raise RuntimeError("Query returned no rows — check that fct_action_values has been built by dbt")

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
        readme_path=get_hf_card_path("spadl-vaep-action-values.md", kind="dataset"),
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
    """CLI entry point for SPADL/VAEP publisher."""
    configure_logging("publish_spadl_vaep_hf")
    args = parse_ingestion_args("Publish SPADL/VAEP action values to HF Hub")
    spark = get_spark_session()
    run_pipeline(spark, args.catalog, args.schema, logger)
