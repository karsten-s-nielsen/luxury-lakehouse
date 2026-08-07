"""Publish xG shot data from Databricks gold layer to HF Hub.

Databricks-runtime counterpart of ``scripts/publish_xg_shots_hf.py``.
Reads from ``fct_shots`` via ``spark.sql()``, writes partitioned Parquet
to a temp directory, and uploads to HF Hub as ``luxury-lakehouse/xg-shot-data``.

Wired into the daily pipeline via ``src/ingestion/hf_sync.py`` sub-operations.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from ingestion.hf_upload_seam import GuardedFrame, prepare_public_upload, upload_guarded
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from shared.constants import DEFAULT_GOLD_SCHEMA
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
    s.data_source,
    -- Per-match HF redistribution tier (spec §6.7/D11). fct_shots carries no SkillCorner today;
    -- derived from dim_matches so the fail-closed leak guard halts the publish if a restricted
    -- match ever appears. NULL (unmatched) → guard fails closed.
    dm.access_tier
FROM {catalog}.{gold}.fct_shots s
LEFT JOIN {catalog}.{gold}.dim_matches dm
    ON s.match_key = dm.match_key
"""


def publish_to_hf_hub(guarded: GuardedFrame, hf_token: str) -> str:
    """Write data_source-partitioned Parquet and upload, sweeping stale siblings.

    ``delete_patterns`` are matched RELATIVE to ``path_in_repo`` ("data"), so the only correct
    whole-path sweep is ``["**"]`` — this call previously passed ``["data/*"]``, which matches
    NOTHING and had silently no-opped since it was written (the ADR-049 stale-part-file class;
    CLAUDE.md mandates ``["**"]``). Re-uploaded files are pruned from the delete set by
    ``upload_folder`` itself, so the sweep removes stale siblings and keeps what we just wrote.

    Extracted from ``run_pipeline`` (ADR-072) so the staged tree and upload contract are testable
    without Spark or credentials, and so this twin has the same shape as
    ``scripts/publish_xg_shots_hf.py``.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)
        for source, sub in guarded.groupby("data_source"):
            sub.drop_columns(["data_source"]).write_parquet(staging_dir / f"data_source={source}" / "data.parquet")
        return upload_guarded(
            staging_dir,
            frames=[guarded],
            repo_id=DATASET_REPO,
            token=hf_token,
            delete_patterns=["**"],
        )


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
    # resolve_hf_token() is the ONLY sanctioned resolver: env -> Databricks secret scope
    # "hf"/"token" -> cached CLI login. This module runs on Databricks serverless, where there is
    # no HF_TOKEN env var and no CLI cache -- ONLY the secret scope. The previous
    # `os.environ.get(...) or get_token()` skipped that middle source, so this publisher raised
    # before doing any work on every job run, and hf_sync swallowed it and reported SUCCESS.
    from ingestion.utils import resolve_hf_token

    hf_token = resolve_hf_token()
    if not hf_token:
        raise RuntimeError(
            "No HF token from any source (HF_TOKEN env / Databricks secret scope 'hf' key 'token' / "
            "cached CLI login) — cannot upload to HF Hub"
        )

    # fct_shots + dim_matches are GOLD marts. hf_sync passes --schema bronze (its import
    # leg writes there), so the passed schema is the wrong layer for this publisher —
    # it resolved to `bronze.fct_shots`, which does not exist. Name the layer (ADR-073).
    _ = schema  # reads from DEFAULT_GOLD_SCHEMA, not the pipeline schema
    sql = _SHOTS_SQL.format(catalog=catalog, gold=DEFAULT_GOLD_SCHEMA)
    pipeline_logger.info("Querying fct_shots from %s.%s", catalog, DEFAULT_GOLD_SCHEMA)
    df = spark.sql(sql).toPandas()
    row_count = len(df)
    pipeline_logger.info("Retrieved %d shot rows", row_count)

    if row_count == 0:
        raise RuntimeError("Query returned no rows — check that fct_shots has been built by dbt")

    # Fail-closed leak guard (spec §6.7/D11): halts the publish if a restricted row ever appears.
    # Drop the internal access_tier column AFTER the guard, before upload (R2).
    prepared = prepare_public_upload(df, publisher="publish_xg_shots_hf")
    publish_to_hf_hub(prepared.public, hf_token)

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
