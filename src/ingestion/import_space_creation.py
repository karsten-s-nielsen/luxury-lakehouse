"""Import space creation values from HF Hub to bronze Delta table.

Usage (Databricks):
    import_space_creation --catalog soccer_analytics --schema bronze \
        --volume-path /Volumes/soccer_analytics/dev_gold/model_weights/space_creation

References:
    Fernandez & Bornn (2018). "Wide Open Spaces: A statistical technique for
    measuring space creation in professional soccer." MIT Sloan.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from typing import TYPE_CHECKING

from huggingface_hub import hf_hub_download

from ingestion.guards import FilterResult
from shared.constants import IDENTIFIER_RE
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

HF_REPO = "luxury-lakehouse/space-creation-values"


class _ImportSpaceCreationGuard:
    workflow_id = "wf-import-space-creation"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _ImportSpaceCreationGuard()
TABLE_NAME = "space_creation_values"


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
    logger = logging.getLogger("import_space_creation")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


@workflow("wf-import-space-creation", phase="import")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    volume_path: str,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> None:
    """Download space creation values from HF Hub and write to bronze Delta table."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")
    from pyspark.sql import functions as spark_fn  # type: ignore[import-not-found]

    logger = logging.getLogger("import_space_creation")

    # ------------------------------------------------------------------
    # 1. Download from HF Hub to local cache, then copy to UC Volume
    # ------------------------------------------------------------------
    parquet_filename = "data/space_creation.parquet"
    logger.info("Downloading %s from %s", parquet_filename, HF_REPO)
    local_path = hf_hub_download(repo_id=HF_REPO, filename=parquet_filename, repo_type="dataset")
    logger.info("Downloaded to local cache: %s", local_path)

    volume_file = f"{volume_path}/space_creation_values.parquet"
    logger.info("Copying to UC Volume: %s", volume_file)
    import shutil
    from pathlib import Path

    Path(volume_path).mkdir(parents=True, exist_ok=True)
    shutil.copy2(local_path, volume_file)
    logger.info("Copy complete")

    # ------------------------------------------------------------------
    # 2. Read from Volume and write to Delta
    # ------------------------------------------------------------------
    table = f"{catalog}.{schema}.{TABLE_NAME}"

    logger.info("Reading space creation values from %s", volume_file)
    df = spark.read.parquet(volume_file)
    df = df.withColumn("_ingested_at", spark_fn.current_timestamp())
    row_count = int(df.count())
    logger.info("Space creation row count: %d", row_count)

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
            logger.warning("No match_ids found in space creation values — skipping write")
    else:
        logger.info("No space creation values to import — skipping")

    logger.info("Space creation import complete")


def main() -> None:
    """CLI entry point for space creation values import."""
    # Late import — PySpark only available in Databricks runtime
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    _configure_logging()

    parser = argparse.ArgumentParser(description="Import space creation values to Delta")
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

    spark = SparkSession.builder.getOrCreate()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, catalog, schema)

    from ingestion.guards import read_gate_result

    filter_result = read_gate_result("wf-import-space-creation")
    if filter_result is None:
        filter_result = skip_guard.check(spark, catalog, schema)

    run_pipeline(spark, catalog, schema, volume_path, filter_result=filter_result)


if __name__ == "__main__":
    main()
