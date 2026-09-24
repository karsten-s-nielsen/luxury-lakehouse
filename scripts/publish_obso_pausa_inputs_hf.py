# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.116-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish OBSO+PAUSA prerequisite inputs (IDSSE SPADL actions + ELASTIC action-frame alignment) to HF Hub.

Migrated from notebooks/publish_obso_data.py per HF4 (SK3-MIG-B). PEP 723
single-file: runs locally + on HF Jobs. Uses Databricks SQL Statement Execution
API + Arrow chunks (no Spark / .toPandas() OOM risk).

Dataset: luxury-lakehouse/obso-pausa-inputs

Usage (HF Jobs):
    hf jobs uv run scripts/publish_obso_pausa_inputs_hf.py \\
        --flavor cpu-basic --timeout 30m \\
        --secrets HF_TOKEN \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN \\
        --env DATABRICKS_SQL_WAREHOUSE_ID=$DATABRICKS_SQL_WAREHOUSE_ID
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from pathlib import Path

from analytics.databricks_sql_fetch import query_databricks_sql
from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from ingestion.hf_upload_seam import GuardedFrame, prepare_public_upload, upload_guarded

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
DATASET_REPO = f"{HF_ORG}/obso-pausa-inputs"

# IDSSE SPADL actions + their ELASTIC action-frame alignment, in a single denormalized payload.
# B-ac (2026-09-20): elastic is now the AC drain's action-grain output (elastic_frame_id +
# elastic_receive_*), sourced from stg_action_context__values — the retired standalone
# bronze.elastic_sync_results (event-grain) is gone. access_tier is stamped per-row on the AC
# rows (ADR-064), so no dim_matches join is needed; the NULL-tier fail-loud in main() still holds.
# The elastic_frame_id IS NOT NULL filter is the action-grain equivalent of the old event INNER
# JOIN — only actions ELASTIC actually anchored to a frame ship (unaligned actions are dropped).
_INPUTS_SQL = """\
SELECT
    ac.native_match_id AS match_id,
    ac.action_id,
    ac.type_name,
    ac.period_id AS period,
    ac.time_seconds AS timestamp_seconds,
    ac.player_id_native AS player_id,
    ac.team_id_native AS team,
    ac.start_x,
    ac.start_y,
    ac.elastic_frame_id,
    ac.elastic_confidence,
    ac.elastic_error_seconds,
    ac.elastic_receive_frame_id,
    ac.elastic_receive_confidence,
    ac.elastic_receive_error_seconds,
    ac.access_tier
FROM soccer_analytics.dev_silver.stg_action_context__values ac
WHERE ac.data_source = 'idsse'
    AND ac.elastic_frame_id IS NOT NULL
"""


def publish_to_hf_hub(guarded: GuardedFrame, hf_token: str) -> str:
    """Write match-partitioned Parquet and upload. Repo creation is handled by upload_guarded."""
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)
        # Partition by match_id for efficient downstream loading by match.
        for match_id, sub in guarded.groupby("match_id"):
            sub.drop_columns(["match_id"]).write_parquet(staging_dir / f"match_id={match_id}" / "data.parquet")
        # delete_patterns are matched RELATIVE to path_in_repo ("data"), so the only correct
        # whole-path sweep is ["**"] — this call previously passed ["data/*"], which matches
        # NOTHING and had silently no-opped since it was written (the ADR-049 stale-part-file
        # class; CLAUDE.md mandates ["**"]). Re-uploaded files are pruned from the delete set by
        # upload_folder itself, so the sweep removes stale siblings and keeps what we just wrote.
        url = upload_guarded(
            staging_dir,
            frames=[guarded],
            repo_id=DATASET_REPO,
            token=hf_token,
            delete_patterns=["**"],
        )
    logger.info("Published %s", url)
    return url


def main() -> None:
    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        from huggingface_hub import get_token

        hf_token = get_token() or ""
    if not hf_token:
        raise RuntimeError("HF_TOKEN required")

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "").rstrip("/")
    db_token = os.environ["DATABRICKS_TOKEN"]
    warehouse_id = os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]

    df = query_databricks_sql(host, db_token, _INPUTS_SQL, warehouse_id)
    if df.empty:
        raise RuntimeError(
            "Query returned 0 rows — verify stg_action_context__values (data_source='idsse') is "
            "populated with elastic_frame_id (the AC drain must have run for IDSSE)"
        )
    logger.info("Retrieved %s rows across %s matches", f"{len(df):,}", df["match_id"].nunique())

    # R-13 (B-ac): access_tier is stamped per-row on the AC rows (ADR-064), not joined from
    # dim_matches. A NULL here means a pre-cycle AC row that predates the tier stamp; split_restricted
    # fail-safes NULL to restricted, which for this fail_closed public-by-licence publisher would
    # silently WITHHOLD public open data. Fail loud instead (the AC re-materialize backfills the tier).
    unmatched = int(df["access_tier"].isna().sum())
    if unmatched:
        raise RuntimeError(
            f"publish_obso_pausa_inputs_hf: {unmatched} rows have NULL access_tier "
            f"(idsse AC rows missing the ADR-064 tier stamp) — refusing to publish and silently withhold public data"
        )

    prepared = prepare_public_upload(df, publisher="publish_obso_pausa_inputs_hf")
    url = publish_to_hf_hub(prepared.public, hf_token)
    upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_hf_card_path("obso-pausa-inputs.md", kind="dataset"),
        hf_token=hf_token,
    )
    logger.info("Pipeline complete: %s", url)


if __name__ == "__main__":
    main()
