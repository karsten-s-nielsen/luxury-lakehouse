# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.105-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish pitch-control tracking frames to HF Hub.

Migrated from the pitch-control cell of notebooks/publish_datasets.py per HF4
(SK3-MIG-B). Inventory-only — NOT fired by SK3-MIG-B Group 3 republishes
(tracking adapters pinned to absolute_frame; not coord-dependent).

Dataset: luxury-lakehouse/pitch-control-tracking

Note on flavor sizing: tracking frame counts can exceed cpu-basic 16 GB driver
memory. If the publish fails OOM, escalate to a larger flavor (gpu-medium has
~50 GB) — the dataset is single-shot, not in the SK3-MIG-B regular cycle.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from pathlib import Path

from analytics.databricks_sql_fetch import query_databricks_sql
from ingestion.hf_publish import (
    RESTRICTED_HF_PROVIDERS,
    get_hf_card_path,
    restricted_repo_id,
    upload_hf_readme,
)
from ingestion.hf_upload_seam import GuardedFrame, prepare_public_upload, upload_guarded

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
DATASET_REPO = f"{HF_ORG}/pitch-control-tracking"
# PRIVATE companion repo for per-match restricted tracking frames (ADR-049; org-members only).
# fct_tracking_frames carries SkillCorner (raw positional tracking) — the moment a restricted
# SkillCorner match is ingested, its frames must route here, NOT to the public repo. The pair is
# PERMANENT infrastructure: both repos are ensured on every run, even when the restricted set is
# empty (no restricted tracking match ingested yet — the healthy default today).
RESTRICTED_DATASET_REPO = restricted_repo_id(DATASET_REPO)

# The SQL pulls ALL providers + the per-match access_tier; the redistribution gate is applied at
# the PUBLISH split (ingestion.hf_publish.split_restricted, keyed on access_tier — spec §6.5/D9).
_TRACKING_SQL = """\
SELECT t.tracking_id,
       t.match_key, t.team_key, t.player_key,
       t.match_id, t.player_id, t.team_id, t.team,
       t.period, t.frame, t.timestamp_seconds,
       t.x, t.y, t.ball_x, t.ball_y,
       t.velocity_x, t.velocity_y, t.speed_ms,
       pc.pitch_control_value,
       t.source_provider, t.frame_rate,
       t.access_tier
FROM soccer_analytics.dev_gold.fct_tracking_frames t
INNER JOIN soccer_analytics.dev_silver.stg_pitch_control__values pc
    ON t.tracking_id = pc.tracking_id
"""


def publish_to_hf_hub(guarded: GuardedFrame, hf_token: str, *, repo_id: str = DATASET_REPO) -> str:
    """Write Hive-partitioned (``source_provider=<p>``) Parquet and upload to a HF dataset repo.

    Args:
        guarded: Tracking frames that passed the ADR-072 seam guard (may be empty — a
            sweep-only restricted publish).
        hf_token: HuggingFace API token.
        repo_id: Target dataset repo (default: the public DATASET_REPO; the restricted companion
            passes RESTRICTED_DATASET_REPO).

    Repo privacy is DERIVED from ``guarded.tier`` inside ``upload_guarded`` — no ``private`` flag
    to forget, and a restricted frame targeting a repo without the ADR-049 suffix is refused.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)
        if guarded.frame.empty:
            # Sweep-only publish (ADR-049): zero partitions uploaded; the recursive delete_patterns
            # below removes any previously-restricted partitions — the migration-to-public mechanic.
            logger.info("0 partitions for %s — sweep-only publish (delete_patterns clears stale data/)", repo_id)
        for provider, sub in guarded.groupby("source_provider"):
            sub.drop_columns(["source_provider"]).write_parquet(
                staging_dir / f"source_provider={provider}" / "data.parquet"
            )
        # delete_patterns match RELATIVE to path_in_repo ("data/"), so the pattern MUST be "**" —
        # a "data/"-prefixed pattern matches nothing and silently no-ops (ADR-049).
        return upload_guarded(
            staging_dir,
            frames=[guarded],
            repo_id=repo_id,
            token=hf_token,
            delete_patterns=["**"],
        )


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

    df = query_databricks_sql(host, db_token, _TRACKING_SQL, warehouse_id)
    if df.empty:
        raise RuntimeError("0 rows from fct_tracking_frames")
    logger.info("Retrieved %s frames across %s providers", f"{len(df):,}", df["source_provider"].nunique())

    # ADR-072 seam: split -> guard -> drop access_tier, all inside prepare_public_upload. The
    # per-match split (spec §6.5/D9) routes restricted AND NULL/unknown frames to the private
    # companion (fail-safe; split_restricted never leaks an unclassified row).
    prepared = prepare_public_upload(df, publisher="publish_pitch_control_tracking_hf")
    if prepared.restricted is None:
        raise RuntimeError("publish_pitch_control_tracking_hf is registered 'split' — expected a restricted frame")
    public_df, restricted_df = prepared.public.frame, prepared.restricted.frame

    # Per-tier observability (spec C7): row counts per repo at INFO. fct_tracking_frames carries
    # no GradientSports and (today) no restricted SkillCorner, so an empty restricted partition is
    # the healthy default — NOT an error here (unlike the GS-carrying datasets). It populates once
    # a restricted SkillCorner match is ingested.
    pub_by = public_df["source_provider"].value_counts().to_dict()
    res_by = restricted_df["source_provider"].value_counts().to_dict() if not restricted_df.empty else {}
    logger.info(
        "Per-tier publish counts — public: %d frames %s; restricted: %d frames %s "
        "(provider-default restricted set: %s; tracking carries none of these, so an empty "
        "restricted partition is the healthy default until a restricted SkillCorner match is ingested)",
        len(public_df),
        pub_by,
        len(restricted_df),
        res_by,
        sorted(RESTRICTED_HF_PROVIDERS),
    )

    logger.info("Publishing PUBLIC pitch-control tracking to HF Hub: %s", DATASET_REPO)
    url = publish_to_hf_hub(prepared.public, hf_token)

    logger.info(
        "Publishing RESTRICTED pitch-control tracking (%s frames) to PRIVATE repo: %s",
        f"{len(restricted_df):,}",
        RESTRICTED_DATASET_REPO,
    )
    publish_to_hf_hub(prepared.restricted, hf_token, repo_id=RESTRICTED_DATASET_REPO)

    for repo, card in (
        (DATASET_REPO, "pitch-control-tracking.md"),
        (RESTRICTED_DATASET_REPO, "pitch-control-tracking-restricted.md"),
    ):
        upload_hf_readme(
            repo_id=repo,
            readme_path=get_hf_card_path(card, kind="dataset"),
            hf_token=hf_token,
        )
    logger.info("Pipeline complete: %s", url)


if __name__ == "__main__":
    main()
