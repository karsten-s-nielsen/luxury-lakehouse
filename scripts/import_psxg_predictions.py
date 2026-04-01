"""Import PSxG predictions from HF Hub to bronze Delta table.

Usage (Databricks):
    import_psxg_predictions --catalog soccer_analytics --schema bronze \
        --volume-path /Volumes/soccer_analytics/dev_gold/model_weights/psxg
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time

from huggingface_hub import hf_hub_download

# Regex for safe SQL identifiers — prevents injection via catalog/schema names
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

HF_REPO = "luxury-lakehouse/psxg-predictions"
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


def main() -> None:
    """Download PSxG predictions from HF Hub and write to bronze Delta table."""
    # Late import — PySpark only available in Databricks runtime
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]
    from pyspark.sql import functions as spark_fn  # type: ignore[import-not-found]

    logger = _configure_logging()

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
        if not _IDENTIFIER_RE.match(value):
            msg = f"Invalid {field_name} name '{value}': must match {_IDENTIFIER_RE.pattern}"
            raise SystemExit(msg)

    spark = SparkSession.builder.getOrCreate()

    # ------------------------------------------------------------------
    # 1. Download from HF Hub to local cache, then copy to UC Volume
    # ------------------------------------------------------------------
    parquet_filename = "data/psxg_predictions.parquet"
    logger.info("Downloading %s from %s", parquet_filename, HF_REPO)
    local_path = hf_hub_download(repo_id=HF_REPO, filename=parquet_filename, repo_type="dataset")
    logger.info("Downloaded to local cache: %s", local_path)

    volume_file = f"{volume_path}/psxg_predictions.parquet"
    logger.info("Copying to UC Volume: %s", volume_file)
    dbutils = spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils()  # type: ignore[attr-defined]
    dbutils.fs().cp(f"file://{local_path}", volume_file, True)
    logger.info("Copy complete")

    # ------------------------------------------------------------------
    # 2. Read from Volume and write to Delta
    # ------------------------------------------------------------------
    table = f"{catalog}.{schema}.{TABLE_NAME}"

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

    logger.info("PSxG import complete")


if __name__ == "__main__":
    main()
