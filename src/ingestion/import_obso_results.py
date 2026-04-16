"""Import OBSO results from HF Hub into bronze Delta tables.

Downloads OBSO/PAUSA Parquet from HF Hub (``luxury-lakehouse/obso-pausa-values``),
stages to a UC Volume path, and writes to ``pausa_raw_scores`` and (optionally)
``obso_surfaces`` bronze Delta tables.

Usage (Databricks):
    import_obso_results --catalog soccer_analytics --schema bronze \
        --volume-path /Volumes/soccer_analytics/dev_gold/model_weights/obso \
        --hf-repo luxury-lakehouse/obso-pausa-values

References:
    Spearman (2018). "Beyond Expected Goals." MIT Sloan.
    Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa." MIT Sloan 2026.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING

from huggingface_hub import hf_hub_download

from ingestion.guards import FilterResult, check_hf_dataset_freshness, record_import_sha, timed_check
from shared.constants import IDENTIFIER_RE
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

HF_REPO = "luxury-lakehouse/obso-pausa-values"


class _ImportObsoGuard:
    workflow_id = "wf-import-obso"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return check_hf_dataset_freshness(spark, catalog, self.workflow_id, HF_REPO)


skip_guard = _ImportObsoGuard()
PAUSA_TABLE = "pausa_raw_scores"
OBSO_TABLE = "obso_surfaces"

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
# HF Hub download bridge
# ---------------------------------------------------------------------------


def _download_from_hf(repo_id: str, filename: str, volume_path: str) -> Path:
    """Download a file from HF Hub to UC Volume staging path.

    Returns:
        Path to the staged file on the UC Volume.
    """
    logger = logging.getLogger("import_obso_results")
    logger.info("Downloading %s from %s", filename, repo_id)
    local_path = hf_hub_download(
        repo_id=repo_id,
        filename=filename,
        repo_type="dataset",
    )
    logger.info("Downloaded to local cache: %s", local_path)
    from ingestion.utils import ensure_volume_directory

    ensure_volume_directory(volume_path)
    volume_file = Path(volume_path) / Path(filename).name
    shutil.copy2(local_path, volume_file)
    logger.info("Staged %s → %s", filename, volume_file)
    return volume_file


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


@workflow("wf-import-obso", phase="import")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    volume_path: str,
    hf_repo: str = HF_REPO,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Download OBSO results from HF Hub and write to bronze Delta tables."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")
    from pyspark.sql import functions as spark_fn  # type: ignore[import-not-found]

    logger = logging.getLogger("import_obso_results")

    # ------------------------------------------------------------------
    # 0. Download from HF Hub to UC Volume staging path
    # ------------------------------------------------------------------
    _download_from_hf(hf_repo, f"data/{PAUSA_TABLE}.parquet", volume_path)

    # ------------------------------------------------------------------
    # 1. Import PAUSA raw scores
    # ------------------------------------------------------------------
    scores_path = f"{volume_path}/pausa_raw_scores.parquet"
    scores_table = f"{catalog}.{schema}.pausa_raw_scores"

    logger.info("Reading PAUSA raw scores from %s", scores_path)
    scores_df = spark.read.parquet(scores_path)
    scores_df = scores_df.withColumn("_ingested_at", spark_fn.current_timestamp())
    scores_row_count = int(scores_df.count())
    logger.info("PAUSA raw scores row count: %d", scores_row_count)

    if scores_row_count > 0:
        # Collect distinct match_ids for replaceWhere predicate
        match_ids = [str(row["match_id"]) for row in scores_df.select("match_id").distinct().collect()]
        if match_ids:
            quoted = ", ".join(f"'{mid.replace(chr(39), '')}'" for mid in match_ids)
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
        surfaces_df = surfaces_df.withColumn("_ingested_at", spark_fn.current_timestamp())
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
    except FileNotFoundError:
        logger.info("OBSO surfaces not found at %s — skipping (surfaces are optional)", surfaces_path)
    except Exception:
        logger.error("Unexpected error reading OBSO surfaces at %s — skipping", surfaces_path, exc_info=True)

    record_import_sha(spark, catalog, "wf-import-obso", hf_repo, filter_result.metadata.get("commit_sha"))

    logger.info("OBSO import complete")
    return 0


def main() -> None:
    """CLI entry point for OBSO results import."""
    # Late import — PySpark only available in Databricks runtime
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    _configure_logging()

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
    parser.add_argument(
        "--hf-repo",
        default=HF_REPO,
        help="HF Hub dataset repo to download OBSO results from (default: %(default)s)",
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

    filter_result = timed_check(skip_guard, spark, catalog, schema)

    run_pipeline(spark, catalog, schema, volume_path, hf_repo=args.hf_repo, filter_result=filter_result)


if __name__ == "__main__":
    main()
