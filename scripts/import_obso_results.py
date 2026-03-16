"""Import OBSO results from Parquet into bronze Delta tables.

Standalone PySpark script (run as a Databricks notebook, not an entry point).
Reads OBSO Parquet from a UC Volume staging path and writes to
``obso_surfaces`` and ``pausa_raw_scores`` bronze Delta tables.

Usage (Databricks notebook):
    %run /Workspace/Users/.../import_obso_results

Or from CLI:
    databricks jobs create --json '...'

References:
    Spearman (2018). "Beyond Expected Goals." MIT Sloan.
    Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa." MIT Sloan 2026.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time

# Regex for safe SQL identifiers — prevents injection via catalog/schema names
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


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
    logger = logging.getLogger("import_obso_results")
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
    """Read OBSO Parquet from UC Volume and write to Delta tables."""
    # Late import — PySpark only available in Databricks runtime
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    logger = _configure_logging()

    parser = argparse.ArgumentParser(description="Import OBSO results to Delta")
    parser.add_argument(
        "--catalog",
        default="soccer_analytics",
        help="Unity Catalog name (default: soccer_analytics)",
    )
    parser.add_argument(
        "--schema",
        default="dev_bronze",
        help="Schema name (default: dev_bronze)",
    )
    parser.add_argument(
        "--volume-path",
        default="/Volumes/soccer_analytics/dev_gold/model_weights/obso",
        help="UC Volume path containing OBSO Parquet files",
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
    # 1. Import PAUSA raw scores
    # ------------------------------------------------------------------
    scores_path = f"{volume_path}/pausa_raw_scores.parquet"
    scores_table = f"{catalog}.{schema}.pausa_raw_scores"

    logger.info("Reading PAUSA raw scores from %s", scores_path)
    scores_df = spark.read.parquet(scores_path)
    scores_df = scores_df.withColumn("_ingested_at", F.current_timestamp())
    scores_row_count = int(scores_df.count())
    logger.info("PAUSA raw scores row count: %d", scores_row_count)

    if scores_row_count > 0:
        # Collect distinct match_ids for replaceWhere predicate
        match_ids = [str(row["match_id"]) for row in scores_df.select("match_id").distinct().collect()]
        if match_ids:
            quoted = ", ".join(f"'{mid}'" for mid in match_ids)
            replace_where = f"match_id IN ({quoted})"
            start = time.time()
            (
                scores_df.write.format("delta")
                .option("mergeSchema", "true")
                .option("replaceWhere", replace_where)
                .mode("overwrite")
                .saveAsTable(scores_table)
            )
            elapsed = time.time() - start
            logger.info(
                "Wrote %d rows to %s (replaceWhere=%s) in %.2fs",
                scores_row_count,
                scores_table,
                replace_where,
                elapsed,
            )
        else:
            logger.warning("No match_ids found in scores — skipping write")
    else:
        logger.info("No scores to import — skipping")

    # ------------------------------------------------------------------
    # 2. Import OBSO surfaces (if present)
    # ------------------------------------------------------------------
    surfaces_path = f"{volume_path}/obso_surfaces.parquet"
    surfaces_table = f"{catalog}.{schema}.obso_surfaces"

    try:
        logger.info("Reading OBSO surfaces from %s", surfaces_path)
        surfaces_df = spark.read.parquet(surfaces_path)
        surfaces_df = surfaces_df.withColumn("_ingested_at", F.current_timestamp())
        surface_row_count = int(surfaces_df.count())
        logger.info("OBSO surfaces row count: %d", surface_row_count)

        if surface_row_count > 0:
            surface_match_ids = [str(row["match_id"]) for row in surfaces_df.select("match_id").distinct().collect()]
            if surface_match_ids:
                quoted = ", ".join(f"'{mid}'" for mid in surface_match_ids)
                replace_where = f"match_id IN ({quoted})"
                start = time.time()
                (
                    surfaces_df.write.format("delta")
                    .option("mergeSchema", "true")
                    .option("replaceWhere", replace_where)
                    .mode("overwrite")
                    .saveAsTable(surfaces_table)
                )
                elapsed = time.time() - start
                logger.info(
                    "Wrote %d rows to %s (replaceWhere=%s) in %.2fs",
                    surface_row_count,
                    surfaces_table,
                    replace_where,
                    elapsed,
                )
            else:
                logger.warning("No match_ids found in surfaces — skipping write")
        else:
            logger.info("No surfaces to import — skipping")
    except Exception:
        logger.info("OBSO surfaces not found at %s — skipping (surfaces are optional)", surfaces_path)

    logger.info("OBSO import complete")


if __name__ == "__main__":
    main()
