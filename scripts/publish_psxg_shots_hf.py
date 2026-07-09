# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.72-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish shot-grain PSxG (fct_shot_psxg) to HF Hub.

Datasets: luxury-lakehouse/psxg-shots (public)
          luxury-lakehouse/psxg-shots-restricted (private companion, ADR-049)

One row per on-target shot, all providers (provider is the ``data_source`` column,
not a code fork): the post-shot xG plus the projected/measured goalmouth geometry
and the resolved shooter + defending-GK keys. Mirrors the action-context publisher:
GradientSports partitions are license-restricted and route to the private companion
repo via ``ingestion.hf_publish.split_restricted``; StatsBomb / SkillCorner / IDSSE
publish to the public repo. Files are flat per-provider (``data/<provider>.parquet``)
so the card declares one HF config per provider (ADR-054).
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
DATASET_REPO = f"{HF_ORG}/psxg-shots"
# PRIVATE companion repo for license-gated partitions (ADR-049; org-members only).
# Naming + split criterion are owned by ingestion.hf_publish — the single source of
# truth shared with every other ADR-049 publisher. The pair is PERMANENT infrastructure:
# both repos are ensured on every run, even when the restricted set is empty.
RESTRICTED_DATASET_REPO = restricted_repo_id(DATASET_REPO)

# The SQL pulls ALL providers; the HF license gate is applied at the PUBLISH split
# (ingestion.hf_publish.split_restricted — ADR-049): restricted rows (GradientSports)
# go to the PRIVATE RESTRICTED_DATASET_REPO, the rest to the public DATASET_REPO.
# Granting a provider full permission = remove it from RESTRICTED_HF_PROVIDERS; the
# next publish migrates its partition to the public repo and sweeps the restricted one.
_PSXG_SHOTS_SQL = """\
SELECT * FROM soccer_analytics.dev_gold.fct_shot_psxg
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
    """Write flat per-provider Parquet and upload to a HF dataset repo.

    Args:
        df: PSxG shots DataFrame to publish (may be empty — see below).
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
            # Flat per-provider files (data/<provider>.parquet) and KEEP the data_source
            # column, so the card can declare one HF config per provider (ADR-054) — we do
            # NOT rely on Hive path-key recovery, which HF does not apply to explicit data_files.
            sub_df.to_parquet(staging_dir / f"{source}.parquet", index=False, engine="pyarrow")
        # delete_patterns match RELATIVE to path_in_repo ("data/"), so the pattern MUST be
        # "**" — a "data/"-prefixed pattern matches nothing and silently no-ops (ADR-049).
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

    df = query_databricks_sql(host, db_token, _PSXG_SHOTS_SQL, warehouse_id)
    if df.empty:
        raise RuntimeError("0 rows from fct_shot_psxg — verify dbt build / goalkeeper_enabled")
    logger.info("Retrieved %s PSxG shot rows", f"{len(df):,}")

    # Per-match split keyed on access_tier (spec §6.5): restricted AND NULL/unknown rows →
    # PRIVATE companion repo (fail-safe; split_restricted never leaks an unclassified row).
    public_df, restricted_df = split_restricted(df, column="access_tier")

    # Fail-closed leak guard on the PUBLIC frame BEFORE upload — needs access_tier present.
    assert_no_private_leak(public_df, publisher="publish_psxg_shots_hf")

    # Per-tier observability (spec C7): row counts per repo at INFO.
    pub_by = public_df["data_source"].value_counts().to_dict()
    res_by = restricted_df["data_source"].value_counts().to_dict() if not restricted_df.empty else {}
    logger.info(
        "Per-tier publish counts — public: %d rows %s; restricted: %d rows %s",
        len(public_df),
        pub_by,
        len(restricted_df),
        res_by,
    )

    # R2: drop the internal access_tier column from BOTH frames AFTER split + guard, before
    # upload — constant per repo; keeping it is a Hyrum additive-schema change to the public
    # dataset. Order: split -> guard -> drop -> upload.
    public_df = public_df.drop(columns=["access_tier"], errors="ignore")
    restricted_df = restricted_df.drop(columns=["access_tier"], errors="ignore")

    logger.info("Publishing PUBLIC PSxG shots to HF Hub: %s", DATASET_REPO)
    url = publish_to_hf_hub(public_df, hf_token)

    # Fail-loud ONLY when the restricted set expects data the mart doesn't have
    # (the silent-corpus-shrink class). An EMPTY restricted set is healthy: the
    # always-run restricted publish below then sweeps previously-restricted
    # partitions while this run's public publish carries them.
    if RESTRICTED_HF_PROVIDERS and restricted_df.empty:
        raise RuntimeError(
            f"No rows for restricted providers {sorted(RESTRICTED_HF_PROVIDERS)} in fct_shot_psxg — "
            "refusing to publish an empty restricted dataset while the policy expects data."
        )
    logger.info(
        "Publishing RESTRICTED PSxG shots (%s rows) to PRIVATE repo: %s",
        f"{len(restricted_df):,}",
        RESTRICTED_DATASET_REPO,
    )
    publish_to_hf_hub(restricted_df, hf_token, repo_id=RESTRICTED_DATASET_REPO, private=True)

    # Inject a data-driven per-provider `configs:` block into each card (ADR-054) so the
    # viewer shows a per-provider subset selector and consumers can pull one provider —
    # load_dataset(repo, "<provider>") — without downloading the rest. Providers come from
    # the data actually published to each repo, so the card never drifts from the dataset.
    public_providers = sorted(public_df["data_source"].unique())
    restricted_providers = sorted(restricted_df["data_source"].unique())
    for repo, card, providers in (
        (DATASET_REPO, "psxg-shots.md", public_providers),
        (RESTRICTED_DATASET_REPO, "psxg-shots-restricted.md", restricted_providers),
    ):
        upload_hf_readme(
            repo_id=repo,
            readme_path=get_hf_card_path(card, kind="dataset"),
            hf_token=hf_token,
            config_providers=providers,
        )
    logger.info("Pipeline complete: %s", url)


if __name__ == "__main__":
    main()
