# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.84-py3-none-any.whl",
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
import time
from pathlib import Path

import pandas as pd
import requests

from ingestion.hf_leak_guard import assert_no_private_leak
from ingestion.hf_publish import (
    RESTRICTED_HF_PROVIDERS,
    get_hf_card_path,
    restricted_repo_id,
    split_restricted,
    upload_hf_readme,
)

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

_POLL_INTERVAL_S = 2.0
_TIMEOUT_SUBMIT = (10, 120)
_TIMEOUT_POLL = (10, 30)
_TIMEOUT_CHUNK = (10, 300)


def query_databricks_sql(host: str, token: str, sql: str, warehouse_id: str) -> pd.DataFrame:
    url = f"https://{host}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "50s",
        "disposition": "EXTERNAL_LINKS",
        "format": "ARROW_STREAM",
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=_TIMEOUT_SUBMIT, verify=True)
    resp.raise_for_status()
    result = resp.json()
    statement_id = result.get("statement_id")
    status = result.get("status", {}).get("state")
    while status in ("PENDING", "RUNNING"):
        time.sleep(_POLL_INTERVAL_S)
        poll_resp = requests.get(f"{url}/{statement_id}", headers=headers, timeout=_TIMEOUT_POLL, verify=True)
        poll_resp.raise_for_status()
        result = poll_resp.json()
        status = result.get("status", {}).get("state")
    if status != "SUCCEEDED":
        err = result.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL {status}: {err.get('message', '?')}")

    manifest = result.get("manifest", {})
    total_chunks = int(manifest.get("total_chunk_count", 0) or 0)
    import pyarrow as pa

    arrow_tables: list[pa.Table] = []
    for chunk_idx in range(total_chunks):
        chunk_url = f"{url}/{statement_id}/result/chunks/{chunk_idx}"
        chunk_resp = requests.get(chunk_url, headers=headers, timeout=_TIMEOUT_CHUNK, verify=True)
        chunk_resp.raise_for_status()
        for link_info in chunk_resp.json().get("external_links", []):
            dl_resp = requests.get(link_info["external_link"], timeout=_TIMEOUT_CHUNK, verify=True)
            dl_resp.raise_for_status()
            reader = pa.ipc.open_stream(dl_resp.content)
            arrow_tables.append(reader.read_all())
    if not arrow_tables:
        raise RuntimeError("No data chunks")
    return pa.concat_tables(arrow_tables).to_pandas()


def publish_to_hf_hub(df: pd.DataFrame, hf_token: str, *, repo_id: str = DATASET_REPO, private: bool = False) -> str:
    """Write Hive-partitioned (``source_provider=<p>``) Parquet and upload to a HF dataset repo.

    Args:
        df: Tracking frames to publish (may be empty — a sweep-only restricted publish).
        hf_token: HuggingFace API token.
        repo_id: Target dataset repo (default: the public DATASET_REPO; the restricted companion
            passes RESTRICTED_DATASET_REPO).
        private: Create the repo private (org-members only) if it does not exist yet.
    """
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)
    api.create_repo(repo_id, exist_ok=True, repo_type="dataset", token=hf_token, private=private)
    logger.info("Ensured dataset repo exists: %s (private=%s)", repo_id, private)
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)
        if df.empty:
            # Sweep-only publish (ADR-049): zero partitions uploaded; the recursive delete_patterns
            # below removes any previously-restricted partitions — the migration-to-public mechanic.
            logger.info("0 partitions for %s — sweep-only publish (delete_patterns clears stale data/)", repo_id)
        for provider, sub_df in df.groupby("source_provider"):
            partition_dir = staging_dir / f"source_provider={provider}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            sub_df.drop(columns=["source_provider"]).to_parquet(
                partition_dir / "data.parquet",
                index=False,
                engine="pyarrow",
            )
        # delete_patterns match RELATIVE to path_in_repo ("data/"), so the pattern MUST be "**" —
        # a "data/"-prefixed pattern matches nothing and silently no-ops (ADR-049).
        api.upload_folder(
            folder_path=str(staging_dir),
            path_in_repo="data",
            repo_id=repo_id,
            repo_type="dataset",
            token=hf_token,
            delete_patterns=["**"],
        )
    return f"https://huggingface.co/datasets/{repo_id}"


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

    # Per-match split keyed on access_tier (spec §6.5/D9): public frames → public repo, restricted
    # AND NULL/unknown frames → the private companion (fail-safe; split_restricted never leaks).
    public_df, restricted_df = split_restricted(df, column="access_tier")

    # Fail-closed leak guard on the PUBLIC frame BEFORE upload — needs access_tier present.
    assert_no_private_leak(public_df, publisher="publish_pitch_control_tracking_hf")

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

    # R2: drop the internal access_tier column from BOTH frames AFTER split + guard, before upload.
    public_df = public_df.drop(columns=["access_tier"], errors="ignore")
    restricted_df = restricted_df.drop(columns=["access_tier"], errors="ignore")

    logger.info("Publishing PUBLIC pitch-control tracking to HF Hub: %s", DATASET_REPO)
    url = publish_to_hf_hub(public_df, hf_token)

    logger.info(
        "Publishing RESTRICTED pitch-control tracking (%s frames) to PRIVATE repo: %s",
        f"{len(restricted_df):,}",
        RESTRICTED_DATASET_REPO,
    )
    publish_to_hf_hub(restricted_df, hf_token, repo_id=RESTRICTED_DATASET_REPO, private=True)

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
