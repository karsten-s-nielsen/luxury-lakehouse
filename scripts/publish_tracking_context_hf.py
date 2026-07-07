# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.64-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish tracking context features (fct_tracking_context) to HF Hub.

Dataset: luxury-lakehouse/spadl-tracking-context
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
DATASET_REPO = f"{HF_ORG}/spadl-tracking-context"
# PRIVATE companion repo (ADR-049; org-members only). D6: migrated off the legacy SQL-side
# GradientSports exclusion onto the uniform per-match access_tier split — restricted rows
# (GradientSports, or any per-match-restricted provider) now route here instead of being silently
# dropped. Both repos are ensured on every run (permanent infrastructure).
RESTRICTED_DATASET_REPO = restricted_repo_id(DATASET_REPO)

# fct_tracking_context has no per-row access_tier column of its own; derive it from dim_matches
# (the per-match source of truth) via match_key, then split at publish time (spec §6.5/D6). NO
# SQL-side provider filter — the redistribution gate is ingestion.hf_publish.split_restricted.
_TRACKING_CONTEXT_SQL = """\
SELECT t.*, dm.access_tier, dm.visibility
FROM soccer_analytics.dev_gold.fct_tracking_context t
LEFT JOIN soccer_analytics.dev_gold.dim_matches dm
    ON t.match_key = dm.match_key
"""

_POLL_INTERVAL_S = 2.0
_TIMEOUT_SUBMIT = (10, 120)
_TIMEOUT_POLL = (10, 30)
_TIMEOUT_CHUNK = (10, 120)


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
    """Write Hive-partitioned (``data_source=<p>``) Parquet and upload to a HF dataset repo.

    Args:
        df: Tracking-context rows to publish (may be empty — a sweep-only restricted publish).
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
            logger.info("0 partitions for %s — sweep-only publish (delete_patterns clears stale data/)", repo_id)
        for source, sub_df in df.groupby("data_source"):
            partition_dir = staging_dir / f"data_source={source}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            sub_df.drop(columns=["data_source"]).to_parquet(
                partition_dir / "data.parquet",
                index=False,
                engine="pyarrow",
            )
        # delete_patterns match RELATIVE to path_in_repo ("data/"), so the pattern MUST be "**".
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

    df = query_databricks_sql(host, db_token, _TRACKING_CONTEXT_SQL, warehouse_id)
    if df.empty:
        raise RuntimeError("0 rows from fct_tracking_context — verify dbt build")
    logger.info("Retrieved %s tracking context rows", f"{len(df):,}")

    # Per-match split keyed on access_tier (spec §6.5/D6): public rows → public repo, restricted
    # AND NULL/unknown → the private companion (fail-safe; split_restricted never leaks).
    public_df, restricted_df = split_restricted(df, column="access_tier")

    # Fail-closed leak guard on the PUBLIC frame BEFORE upload — needs access_tier present.
    assert_no_private_leak(public_df, publisher="publish_tracking_context_hf")

    # Per-tier observability (spec C7): per-repo counts at INFO; ERROR-log (not raise — this
    # publisher is deprecation-bound) if the restricted partition is empty while the policy
    # expects restricted providers.
    pub_by = public_df["data_source"].value_counts().to_dict()
    res_by = restricted_df["data_source"].value_counts().to_dict() if not restricted_df.empty else {}
    logger.info(
        "Per-tier publish counts — public: %d rows %s; restricted: %d rows %s",
        len(public_df),
        pub_by,
        len(restricted_df),
        res_by,
    )
    if RESTRICTED_HF_PROVIDERS and restricted_df.empty:
        logger.error(
            "restricted partition is EMPTY while policy expects restricted providers %s — "
            "token-misconfig / silent-corpus-shrink backstop (spec C7)",
            sorted(RESTRICTED_HF_PROVIDERS),
        )

    # R2: drop the internal access_tier + visibility columns from BOTH frames AFTER split + guard, before upload.
    # (visibility is carried only so the leak guard's divergence check can fire on-path; it never ships.)
    public_df = public_df.drop(columns=["access_tier", "visibility"], errors="ignore")
    restricted_df = restricted_df.drop(columns=["access_tier", "visibility"], errors="ignore")

    logger.info("Publishing PUBLIC tracking context to HF Hub: %s", DATASET_REPO)
    url = publish_to_hf_hub(public_df, hf_token)

    logger.info(
        "Publishing RESTRICTED tracking context (%s rows) to PRIVATE repo: %s",
        f"{len(restricted_df):,}",
        RESTRICTED_DATASET_REPO,
    )
    publish_to_hf_hub(restricted_df, hf_token, repo_id=RESTRICTED_DATASET_REPO, private=True)

    for repo, card in (
        (DATASET_REPO, "spadl-tracking-context.md"),
        (RESTRICTED_DATASET_REPO, "spadl-tracking-context-restricted.md"),
    ):
        upload_hf_readme(
            repo_id=repo,
            readme_path=get_hf_card_path(card, kind="dataset"),
            hf_token=hf_token,
        )
    logger.info("Pipeline complete: %s", url)


if __name__ == "__main__":
    main()
