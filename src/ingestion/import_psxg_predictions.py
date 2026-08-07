"""Import PSxG predictions from HF Hub to bronze Delta table.

Usage (Databricks):
    import_psxg_predictions --catalog soccer_analytics --schema bronze \
        --volume-path /Volumes/soccer_analytics/dev_gold/model_weights/psxg
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from typing import TYPE_CHECKING

from huggingface_hub import hf_hub_download

from ingestion.guards import FilterResult, check_hf_dataset_freshness, record_import_sha, timed_check
from shared.constants import DEFAULT_BRONZE_SCHEMA, IDENTIFIER_RE
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

HF_REPO = "luxury-lakehouse/psxg-predictions"


class _ImportPsxgGuard:
    workflow_id = "wf-import-psxg"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return check_hf_dataset_freshness(spark, catalog, self.workflow_id, HF_REPO)


skip_guard = _ImportPsxgGuard()
TABLE_NAME = "psxg_predictions"


# ---------------------------------------------------------------------------
# Structured JSON logging (mirrors src/ingestion/utils.py pattern)
# ---------------------------------------------------------------------------


class _JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON for Databricks log aggregation."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, object] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "source": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, default=str)


def _configure_logging() -> logging.Logger:
    """Create a logger that emits JSON lines to stdout."""
    logger = logging.getLogger("import_psxg_predictions")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


@workflow("wf-import-psxg", phase="import")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    volume_path: str,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Download PSxG predictions from HF Hub and write to bronze Delta table."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")
    from pyspark.sql import functions as spark_fn  # type: ignore[import-not-found]

    logger = logging.getLogger("import_psxg_predictions")

    # ------------------------------------------------------------------
    # 1. Download from HF Hub to local cache, then copy to UC Volume
    # ------------------------------------------------------------------
    parquet_filename = "data/psxg_predictions.parquet"
    logger.info("Downloading %s from %s", parquet_filename, HF_REPO)
    local_path = hf_hub_download(repo_id=HF_REPO, filename=parquet_filename, repo_type="dataset")
    logger.info("Downloaded to local cache: %s", local_path)

    volume_file = f"{volume_path}/psxg_predictions.parquet"
    logger.info("Copying to UC Volume: %s", volume_file)
    import shutil

    from ingestion.utils import ensure_volume_directory

    ensure_volume_directory(volume_path)
    shutil.copy2(local_path, volume_file)
    logger.info("Copy complete")

    # ------------------------------------------------------------------
    # 2. Read from Volume and write to Delta
    # ------------------------------------------------------------------
    # This is the ONE hf_sync sub-operation that genuinely writes BRONZE — it is why
    # hf_sync is passed --schema bronze at all, and why every gold-reading sibling in
    # that task got the wrong layer. Name it rather than inherit it (ADR-073).
    _ = schema  # writes to DEFAULT_BRONZE_SCHEMA, not the pipeline schema
    table = f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.{TABLE_NAME}"

    logger.info("Reading PSxG predictions from %s", volume_file)
    df = spark.read.parquet(volume_file)
    df = df.withColumn("_ingested_at", spark_fn.current_timestamp())
    row_count = int(df.count())
    logger.info("PSxG predictions row count: %d", row_count)

    if row_count > 0:
        match_ids = [str(row["match_id"]) for row in df.select("match_id").distinct().collect()]
        if match_ids:
            quoted = ", ".join(f"'{mid}'" for mid in match_ids)
            replace_where = f"match_id IN ({quoted})"
            start = time.time()
            (
                df.write.format("delta")
                .option("mergeSchema", "true")
                .option("replaceWhere", replace_where)
                .mode("overwrite")
                .saveAsTable(table)
            )
            elapsed = time.time() - start
            logger.info(
                "Wrote %d rows to %s (replaceWhere=%s) in %.2fs",
                row_count,
                table,
                replace_where,
                elapsed,
            )
        else:
            logger.warning("No match_ids found in predictions — skipping write")
    else:
        logger.info("No predictions to import — skipping")

    record_import_sha(spark, catalog, "wf-import-psxg", HF_REPO, filter_result.metadata.get("commit_sha"))

    logger.info("PSxG import complete")
    return 0


def main() -> None:
    """CLI entry point for PSxG predictions import."""
    # Late import — PySpark only available in Databricks runtime
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    _configure_logging()

    parser = argparse.ArgumentParser(description="Import PSxG predictions to Delta")
    parser.add_argument(
        "--catalog",
        default="soccer_analytics",
        help="Unity Catalog name (default: soccer_analytics)",
    )
    parser.add_argument(
        "--schema",
        default="bronze",
        help="Schema name (default: bronze)",
    )
    parser.add_argument(
        "--volume-path",
        required=True,
        help="UC Volume path for staging the downloaded Parquet file",
    )
    args = parser.parse_args()

    catalog: str = args.catalog
    schema: str = args.schema
    volume_path: str = args.volume_path

    # Validate identifiers to prevent SQL injection
    for field_name, value in [("catalog", catalog), ("schema", schema)]:
        if not IDENTIFIER_RE.match(value):
            msg = f"Invalid {field_name} name '{value}': must match {IDENTIFIER_RE.pattern}"
            raise SystemExit(msg)

    spark = SparkSession.builder.getOrCreate()  # type: ignore[attr-defined]

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, catalog, schema)

    filter_result = timed_check(skip_guard, spark, catalog, schema)

    run_pipeline(spark, catalog, schema, volume_path, filter_result=filter_result)


if __name__ == "__main__":
    main()
