# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.107-py3-none-any.whl",
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


def publish_to_hf_hub(guarded: GuardedFrame, hf_token: str, *, repo_id: str = DATASET_REPO) -> str:
    """Write partitioned Parquet and upload to a HF dataset repo.

    Args:
        guarded: Action context frame that passed the ADR-072 seam guard (may be
            empty — see below).
        hf_token: HuggingFace API token.
        repo_id: Target dataset repo (default: the public DATASET_REPO; the
            restricted companion passes RESTRICTED_DATASET_REPO).

    Repo privacy is DERIVED from ``guarded.tier`` inside ``upload_guarded`` — no ``private``
    flag to forget, and a restricted frame targeting a non-``-restricted`` repo is refused.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)
        if guarded.frame.empty:
            # Sweep-only publish (ADR-049): zero partitions uploaded; the recursive
            # delete_patterns below removes any previously-restricted partitions —
            # the migration-to-public mechanic.
            logger.info("0 partitions for %s — sweep-only publish (delete_patterns clears stale data/)", repo_id)
        for source, sub in guarded.groupby("data_source"):
            # Flat per-provider files (data/<provider>.parquet) and KEEP the data_source
            # column. This lets the dataset card declare one HF config per provider so
            # consumers can pull a single provider (e.g. load_dataset(repo, "skillcorner"))
            # and the viewer shows a per-provider subset selector. data_source stays an
            # explicit column so EVERY config (incl. the default "all") carries it — we do
            # NOT rely on Hive `data_source=<x>/` path-key recovery, which HF does not apply
            # to explicitly-listed `data_files`. See the card's `configs:` block.
            sub.write_parquet(staging_dir / f"{source}.parquet")
        # delete_patterns match paths RELATIVE to path_in_repo ("data/"), so the
        # pattern must be "**" — a "data/"-prefixed pattern matches nothing and
        # silently no-ops (ADR-049; the no-op left stale Spark part-files in
        # spadl-vaep partitions for months). Re-uploaded files are pruned from
        # the delete set by upload_folder itself.
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

    df = query_databricks_sql(host, db_token, _ACTION_CONTEXT_SQL, warehouse_id)
    if df.empty:
        raise RuntimeError("0 rows from fct_action_context — verify dbt build")
    logger.info("Retrieved %s action context rows", f"{len(df):,}")

    # Per-match split keyed on access_tier (spec §6.5): restricted AND NULL/unknown rows →
    # PRIVATE companion repo (fail-safe; split_restricted never leaks an unclassified row).
    prepared = prepare_public_upload(df, publisher="publish_action_context_hf")
    if prepared.restricted is None:
        raise RuntimeError("publish_action_context_hf is registered 'split' — expected a restricted frame")
    public_df, restricted_df = prepared.public.frame, prepared.restricted.frame

    # Fail-closed leak guard on the PUBLIC frame BEFORE upload — needs access_tier present.

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

    logger.info("Publishing PUBLIC action context to HF Hub: %s", DATASET_REPO)
    url = publish_to_hf_hub(prepared.public, hf_token)

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
    publish_to_hf_hub(prepared.restricted, hf_token, repo_id=RESTRICTED_DATASET_REPO)

    # Inject a data-driven per-provider `configs:` block into each card so the viewer shows a
    # per-provider subset selector and consumers can pull one provider —
    # `load_dataset(repo, "<provider>")` — without downloading the rest. Providers are taken
    # from the data actually published to each repo, so the card never drifts from the dataset.
    public_providers = sorted(public_df["data_source"].unique())
    restricted_providers = sorted(restricted_df["data_source"].unique())
    for repo, card, providers in (
        (DATASET_REPO, "spadl-action-context.md", public_providers),
        (RESTRICTED_DATASET_REPO, "spadl-action-context-restricted.md", restricted_providers),
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
