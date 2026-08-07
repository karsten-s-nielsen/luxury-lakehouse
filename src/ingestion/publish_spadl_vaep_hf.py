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

from ingestion.hf_publish import (
    RESTRICTED_HF_PROVIDERS,
    get_hf_card_path,
    restricted_repo_id,
    upload_hf_readme,
)
from ingestion.hf_upload_seam import GuardedFrame, prepare_public_upload, upload_guarded
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
DATASET_REPO = f"{HF_ORG}/spadl-vaep-action-values"
# PRIVATE companion repo for per-match restricted partitions (ADR-049; org-members only). C5/B2:
# this Databricks-runtime twin reads fct_action_values (carries SkillCorner + GradientSports) and
# previously had NO split — migrated to the per-match access_tier split so it can never be a
# no-split leak path on a SkillCorner-carrying mart.
RESTRICTED_DATASET_REPO = restricted_repo_id(DATASET_REPO)

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
    data_source,
    access_tier
FROM {catalog}.{schema}.fct_action_values
"""


def _publish_partitioned(
    guarded: GuardedFrame,
    repo_id: str,
    hf_token: str,
    pipeline_logger: logging.Logger,
) -> None:
    """Write data_source-partitioned Parquet for ``guarded`` and upload to ``repo_id`` (both repos).

    Repo privacy is DERIVED from ``guarded.tier`` inside ``upload_guarded`` (ADR-072) — there is no
    ``private`` flag to forget, and the ADR-049 ``-restricted`` suffix is asserted both ways.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)
        if guarded.frame.empty:
            pipeline_logger.info("0 partitions for %s — sweep-only publish (delete_patterns clears data/)", repo_id)
        for source, sub in guarded.groupby("data_source"):
            sub.drop_columns(["data_source"]).write_parquet(staging_dir / f"data_source={source}" / "data.parquet")
            pipeline_logger.info("Wrote partition data_source=%s -> %s", source, repo_id)
        # delete_patterns match RELATIVE to path_in_repo ("data/"), so the pattern MUST be "**".
        upload_guarded(
            staging_dir,
            frames=[guarded],
            repo_id=repo_id,
            token=hf_token,
            delete_patterns=["**"],
        )


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
    from huggingface_hub import get_token

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

    # Per-match split keyed on access_tier (spec §6.5/C5): public rows → public repo, restricted
    # AND NULL/unknown → the private companion (fail-safe; split_restricted never leaks).
    prepared = prepare_public_upload(df, publisher="publish_spadl_vaep_hf")
    if prepared.restricted is None:
        raise RuntimeError("publish_spadl_vaep_hf is registered 'split' — expected a restricted frame")
    public_df, restricted_df = prepared.public.frame, prepared.restricted.frame

    # Fail-closed leak guard on the PUBLIC frame BEFORE upload — needs access_tier present.

    # Per-tier observability (spec C7).
    pub_by = public_df["data_source"].value_counts().to_dict()
    res_by = restricted_df["data_source"].value_counts().to_dict() if not restricted_df.empty else {}
    pipeline_logger.info(
        "Per-tier publish counts — public: %d rows %s; restricted: %d rows %s",
        len(public_df),
        pub_by,
        len(restricted_df),
        res_by,
    )

    # R2: drop the internal access_tier column from BOTH frames AFTER split + guard, before upload.

    _publish_partitioned(prepared.public, DATASET_REPO, hf_token, pipeline_logger)

    # Fail-loud only when the policy expects restricted data the mart doesn't have (silent
    # corpus-shrink class — Champions v10 trained without GS by inheriting a SQL-side filter).
    if RESTRICTED_HF_PROVIDERS and restricted_df.empty:
        raise RuntimeError(
            f"No rows for restricted providers {sorted(RESTRICTED_HF_PROVIDERS)} in fct_action_values — "
            "refusing to publish an empty restricted dataset while the policy expects data."
        )
    _publish_partitioned(prepared.restricted, RESTRICTED_DATASET_REPO, hf_token, pipeline_logger)

    for repo, card in (
        (DATASET_REPO, "spadl-vaep-action-values.md"),
        (RESTRICTED_DATASET_REPO, "spadl-vaep-action-values-restricted.md"),
    ):
        readme_result = upload_hf_readme(
            repo_id=repo,
            readme_path=get_hf_card_path(card, kind="dataset"),
            hf_token=hf_token,
        )
        pipeline_logger.info("Uploaded README to %s (sha256=%s)", repo, readme_result["sha256"][:8])
    pipeline_logger.info("Published %d rows (public+restricted) from %s", row_count, DATASET_REPO)
    return row_count


def main() -> None:
    """CLI entry point for SPADL/VAEP publisher."""
    configure_logging("publish_spadl_vaep_hf")
    args = parse_ingestion_args("Publish SPADL/VAEP action values to HF Hub")
    spark = get_spark_session()
    run_pipeline(spark, args.catalog, args.schema, logger)
