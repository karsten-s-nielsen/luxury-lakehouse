# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.39-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish action context features (fct_action_context) to HF Hub.

Datasets: luxury-lakehouse/spadl-action-context (public)
          luxury-lakehouse/spadl-action-context-restricted (private companion, ADR-049)
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
DATASET_REPO = f"{HF_ORG}/spadl-action-context"
# PRIVATE companion repo for license-gated partitions (ADR-049; org-members only).
# Naming + split criterion are owned by ingestion.hf_publish — single source of
# truth shared with every other ADR-049 publisher and with trainers. The pair is
# PERMANENT infrastructure: both repos are ensured on every run, even when the
# restricted set is empty.
RESTRICTED_DATASET_REPO = restricted_repo_id(DATASET_REPO)

# The SQL pulls ALL providers; the HF license gate is applied at the PUBLISH split
# (ingestion.hf_publish.split_restricted — ADR-049): restricted rows go to the PRIVATE
# RESTRICTED_DATASET_REPO, the rest to the public DATASET_REPO. Granting a provider
# full permission = remove it from RESTRICTED_HF_PROVIDERS; the next publish migrates
# its partition to the public repo and sweeps it from the restricted one.
_ACTION_CONTEXT_SQL = """\
SELECT * FROM soccer_analytics.dev_gold.fct_action_context
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
    """Write partitioned Parquet and upload to a HF dataset repo.

    Args:
        df: Action context DataFrame to publish (may be empty — see below).
        hf_token: HuggingFace API token.
        repo_id: Target dataset repo (default: the public DATASET_REPO; the
            restricted companion passes RESTRICTED_DATASET_REPO).
        private: Create the repo private (org-members only) if it does not
            exist yet. Does NOT flip an existing repo's visibility.
    """
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)
    api.create_repo(repo_id, exist_ok=True, repo_type="dataset", token=hf_token, private=private)
    logger.info("Ensured dataset repo exists: %s (private=%s)", repo_id, private)
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)
        if df.empty:
            # Sweep-only publish (ADR-049): zero partitions uploaded; the recursive
            # delete_patterns below removes any previously-restricted partitions —
            # the migration-to-public mechanic.
            logger.info("0 partitions for %s — sweep-only publish (delete_patterns clears stale data/)", repo_id)
        for source, sub_df in df.groupby("data_source"):
            partition_dir = staging_dir / f"data_source={source}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            sub_df.drop(columns=["data_source"]).to_parquet(
                partition_dir / "data.parquet",
                index=False,
                engine="pyarrow",
            )
        # delete_patterns match paths RELATIVE to path_in_repo ("data/"), so the
        # pattern must be "**" — a "data/"-prefixed pattern matches nothing and
        # silently no-ops (ADR-049; the no-op left stale Spark part-files in
        # spadl-vaep partitions for months). Re-uploaded files are pruned from
        # the delete set by upload_folder itself.
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

    df = query_databricks_sql(host, db_token, _ACTION_CONTEXT_SQL, warehouse_id)
    if df.empty:
        raise RuntimeError("0 rows from fct_action_context — verify dbt build")
    logger.info("Retrieved %s action context rows", f"{len(df):,}")

    # License-gate split (ADR-049): restricted rows → PRIVATE companion repo.
    public_df, restricted_df = split_restricted(df)

    logger.info("Publishing PUBLIC action context to HF Hub: %s", DATASET_REPO)
    url = publish_to_hf_hub(public_df, hf_token)

    # Fail-loud ONLY when the restricted set expects data the mart doesn't have
    # (the silent-corpus-shrink class). An EMPTY restricted set is healthy: the
    # always-run restricted publish below then sweeps previously-restricted
    # partitions while this run's public publish carries them.
    if RESTRICTED_HF_PROVIDERS and restricted_df.empty:
        raise RuntimeError(
            f"No rows for restricted providers {sorted(RESTRICTED_HF_PROVIDERS)} in fct_action_context — "
            "refusing to publish an empty restricted dataset while the policy expects data."
        )
    logger.info(
        "Publishing RESTRICTED action context (%s rows) to PRIVATE repo: %s",
        f"{len(restricted_df):,}",
        RESTRICTED_DATASET_REPO,
    )
    publish_to_hf_hub(restricted_df, hf_token, repo_id=RESTRICTED_DATASET_REPO, private=True)

    for repo, card in (
        (DATASET_REPO, "spadl-action-context.md"),
        (RESTRICTED_DATASET_REPO, "spadl-action-context-restricted.md"),
    ):
        upload_hf_readme(
            repo_id=repo,
            readme_path=get_hf_card_path(card, kind="dataset"),
            hf_token=hf_token,
        )
    logger.info("Pipeline complete: %s", url)


if __name__ == "__main__":
    main()
