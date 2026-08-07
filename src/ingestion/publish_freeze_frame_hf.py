"""Publish shot freeze-frame positions from Databricks to HF Hub.

Databricks-runtime counterpart of ``scripts/publish_freeze_frame_hf.py``.
Reads from ``stg_statsbomb__events`` (silver) via ``spark.sql()``, parses
freeze-frame JSON into per-player rows with normalized coordinates, and
uploads to HF Hub as ``luxury-lakehouse/xg-freeze-frame-data``.

Wired into the daily pipeline via ``src/ingestion/hf_sync.py`` sub-operations.
"""

from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from ingestion.hf_upload_seam import GuardedFrame, prepare_public_upload, upload_guarded
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from shared.access_tier import classify_access_tier
from shared.constants import DEFAULT_SILVER_SCHEMA
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
DATASET_REPO = f"{HF_ORG}/xg-freeze-frame-data"

STATSBOMB_PITCH_LENGTH = 120.0
STATSBOMB_PITCH_WIDTH = 80.0

_FREEZE_FRAME_SQL = """\
SELECT
    e.event_id,
    e.match_id,
    m.competition_id,
    m.season_id,
    e.shot_freeze_frame
FROM {catalog}.{silver}.stg_statsbomb__events e
INNER JOIN {catalog}.{silver}.stg_statsbomb__matches m
    ON e.match_id = m.match_id
WHERE e.event_type = 'Shot'
  AND e.shot_freeze_frame IS NOT NULL
  AND e.shot_freeze_frame != '[]'
"""


def _parse_freeze_frames(df: pd.DataFrame) -> pd.DataFrame:
    """Explode shot_freeze_frame JSON into per-player rows with normalized coordinates."""
    rows: list[dict[str, object]] = []
    parse_errors = 0

    for _, shot in df.iterrows():
        try:
            players = json.loads(shot["shot_freeze_frame"])
            if not isinstance(players, list):
                parse_errors += 1
                continue

            for p in players:
                loc = p.get("location", [0.0, 0.0])
                if not isinstance(loc, list) or len(loc) < 2:
                    continue

                rows.append(
                    {
                        "event_id": str(shot["event_id"]),
                        "match_id": int(shot["match_id"]),
                        "competition_id": int(shot["competition_id"]),
                        "season_id": int(shot["season_id"]),
                        "player_x_norm": float(loc[0]) / STATSBOMB_PITCH_LENGTH,
                        "player_y_norm": float(loc[1]) / STATSBOMB_PITCH_WIDTH,
                        "is_keeper": bool(p.get("keeper", False)),
                        "is_teammate": bool(p.get("teammate", False)),
                    }
                )
        except (json.JSONDecodeError, TypeError, IndexError, ValueError):
            parse_errors += 1
            continue

    if parse_errors > 0:
        logger.warning("Skipped %d shots due to parse errors", parse_errors)

    logger.info("Parsed %d player-position rows from %d shots", len(rows), len(df))
    return pd.DataFrame(rows)


def publish_to_hf_hub(guarded: GuardedFrame, hf_token: str) -> str:
    """Write competition-partitioned Parquet and upload, sweeping stale siblings.

    ``delete_patterns`` are matched RELATIVE to ``path_in_repo`` ("data"), so the only correct
    whole-path sweep is ``["**"]`` — this call previously passed ``["data/*"]``, which matches
    NOTHING and had silently no-opped since it was written (the ADR-049 stale-part-file class;
    CLAUDE.md mandates ``["**"]``). Re-uploaded files are pruned from the delete set by
    ``upload_folder`` itself, so the sweep removes stale siblings and keeps what we just wrote.

    Extracted from ``run_pipeline`` (ADR-072) so the staged tree and upload contract are testable
    without Spark or credentials — ``test_publisher_upload_contract.py`` — and so this twin has the
    same shape as ``scripts/publish_freeze_frame_hf.py``.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)
        for comp_id, part in guarded.groupby("competition_id"):
            part.drop_columns(["competition_id"]).write_parquet(
                staging_dir / f"competition_id={comp_id}" / "data.parquet"
            )
        return upload_guarded(
            staging_dir,
            frames=[guarded],
            repo_id=DATASET_REPO,
            token=hf_token,
            delete_patterns=["**"],
        )


@workflow("wf-publish-freeze-frames", phase="export")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    pipeline_logger: logging.Logger,
    *,
    ctx: object | None = None,
) -> int:
    """Query freeze-frame data from silver layer and publish to HF Hub."""
    _ = (schema, ctx)
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

    sql = _FREEZE_FRAME_SQL.format(catalog=catalog, silver=DEFAULT_SILVER_SCHEMA)
    pipeline_logger.info("Querying freeze-frame data from %s.dev_silver", catalog)
    raw_df = spark.sql(sql).toPandas()
    pipeline_logger.info("Retrieved %d shots with freeze frames", len(raw_df))

    if raw_df.empty:
        raise RuntimeError("Query returned no rows — check that stg_statsbomb__events has shot data")

    freeze_df = _parse_freeze_frames(raw_df)
    row_count = len(freeze_df)

    if row_count == 0:
        raise RuntimeError("No player positions parsed from freeze frames")

    # Fail-closed leak guard (spec §6.7/D11). StatsBomb-only by construction → single per-match
    # tier is the StatsBomb provider default (public), stamped via the classifier so the guard is
    # a real check. Drop the internal column AFTER the guard, before upload (R2).
    freeze_df["access_tier"] = classify_access_tier(provider="statsbomb", visibility=None).value
    prepared = prepare_public_upload(freeze_df, publisher="publish_freeze_frame_hf")
    publish_to_hf_hub(prepared.public, hf_token)

    readme_result = upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_hf_card_path("xg-freeze-frame-data.md", kind="dataset"),
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
    """CLI entry point for freeze-frame publisher."""
    configure_logging("publish_freeze_frame_hf")
    args = parse_ingestion_args("Publish freeze-frame data to HF Hub")
    spark = get_spark_session()
    run_pipeline(spark, args.catalog, args.schema, logger)
