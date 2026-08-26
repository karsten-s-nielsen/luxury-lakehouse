# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.108-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish the pre-shot xG v3 training corpus (shot rows) from Databricks gold to HF Hub.

Datasets: luxury-lakehouse/xg-shot-data-v3 (public)
          luxury-lakehouse/xg-shot-data-v3-restricted (private companion, ADR-049)

This is the tabular half of the ``xg_model_v3`` training corpus (spec §A3): one row per
shot from the gold ``fct_action_values`` fact, carrying the canonical-SPADL shot geometry
and outcome label that the trainer joins to the freeze-frame set on ``(match_key, action_id)``.
Penalties are INCLUDED so the downstream scorer's penalty path has rows; the trainer filters
``shot_penalty`` out separately (spec D4).

The SQL pulls ALL providers; the HF license gate is applied at the PUBLISH split
(ingestion.hf_publish.split_restricted — ADR-049/064): restricted rows (RM SkillCorner + GS
per-match) go to the PRIVATE companion repo, the rest to the public repo. The gate lives ONLY
at the split — there is NO SQL-side ``data_source`` filter (a SQL filter silently shrinks the
restricted repo and any training corpus).

Usage (HF Jobs CLI):
    hf jobs uv run scripts/publish_xg_shot_data_v3_hf.py \\
        --flavor cpu-basic --timeout 30m \\
        --secrets HF_TOKEN \\
        --secrets DATABRICKS_TOKEN=$DATABRICKS_TOKEN \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_SQL_WAREHOUSE_ID=$DATABRICKS_SQL_WAREHOUSE_ID
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd

from analytics.databricks_sql_fetch import query_databricks_sql
from ingestion.hf_publish import (
    RESTRICTED_HF_PROVIDERS,
    get_hf_card_path,
    restricted_repo_id,
    upload_hf_readme,
)
from ingestion.hf_upload_seam import GuardedFrame, prepare_public_upload, upload_guarded

# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HF_ORG = "luxury-lakehouse"
DATASET_REPO = f"{HF_ORG}/xg-shot-data-v3"
# PRIVATE companion repo for license-gated partitions (ADR-049; org-members only). The naming
# convention + split criterion are owned by ingestion.hf_publish (single source of truth — the
# xg_model_v3 trainer imports the same constants, so the publish split and the training-corpus
# expectation can never drift). The pair is PERMANENT infrastructure: both repos are ensured on
# every run, even when the restricted set is empty.
RESTRICTED_DATASET_REPO = restricted_repo_id(DATASET_REPO)

# Shot rows from the gold action-values fact. The action_type IN (...) filter restricts to the
# shot family (spec D4); penalties are INTENTIONALLY included so the scorer's penalty path has
# rows — the trainer excludes shot_penalty separately. This is NOT a provider filter: there is
# deliberately NO data_source predicate here (the license gate is the access_tier split below).
_XG_SHOT_DATA_SQL = """\
SELECT
    match_key,
    action_id,
    action_result,
    action_type,
    start_x,
    start_y,
    data_source,
    access_tier
FROM soccer_analytics.dev_gold.fct_action_values
WHERE action_type IN ('shot', 'shot_freekick', 'shot_penalty')
"""


# ---------------------------------------------------------------------------
# dtype normalization
# ---------------------------------------------------------------------------


def normalize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the published-contract dtypes to shot columns after Arrow deserialization.

    Contract (spec §7): ``match_key``/``action_id`` BIGINT, ``start_x``/``start_y`` DOUBLE,
    ``action_type``/``action_result``/``data_source`` STRING. ``access_tier`` is a NULLABLE
    ``string`` — a plain ``astype(str)`` would coerce NULL into the literal ``"nan"`` and defeat
    the fail-safe split (``split_restricted`` treats NULL/unknown as restricted).

    Args:
        df: Raw DataFrame from the Databricks SQL API response.

    Returns:
        DataFrame with enforced dtypes matching the HF Hub schema contract.
    """
    # Integer keys (Kimball surrogate + per-match action id).
    for col in ("match_key", "action_id"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Float geometry columns (canonical SPADL 105x68).
    for col in ("start_x", "start_y"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # String columns (non-null by contract).
    for col in ("action_type", "action_result", "data_source"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    # NULLABLE string — pandas "string" dtype preserves <NA>; a plain astype(str) would turn a
    # NULL tier into "nan" and break the fail-safe (NULL -> restricted) split.
    for col in ("access_tier",):
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


# ---------------------------------------------------------------------------
# HF Hub publishing
# ---------------------------------------------------------------------------


def publish_to_hf_hub(guarded: GuardedFrame, hf_token: str, *, repo_id: str = DATASET_REPO) -> str:
    """Write flat per-provider Parquet (ADR-054) and upload to a HF dataset repo.

    One flat file per provider (``data/<provider>.parquet``), KEEPING the ``data_source``
    column so every HF config (incl. the default ``all``) carries it and consumers can pull a
    single provider — ``load_dataset(repo, "<provider>")`` — without downloading the rest.

    Args:
        guarded: Frame that passed the ADR-072 seam guard (may be empty — sweep-only).
        hf_token: HuggingFace API token.
        repo_id: Target dataset repo (default: the public DATASET_REPO; the restricted
            companion passes RESTRICTED_DATASET_REPO).
    Repo privacy is DERIVED from ``guarded.tier`` inside ``upload_guarded`` — no ``private``
    flag to forget, and a restricted frame targeting a non-``-restricted`` repo is refused.

    Returns:
        URL of the published dataset.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)

        if guarded.frame.empty:
            # Sweep-only publish (ADR-049): zero partitions uploaded; the recursive
            # delete_patterns below removes any previously-restricted partitions — the
            # migration-to-public mechanic.
            logger.info("0 partitions for %s — sweep-only publish (delete_patterns clears stale data/)", repo_id)

        for source, sub in guarded.groupby("data_source"):
            out_path = staging_dir / f"{source}.parquet"
            sub.write_parquet(out_path)
            logger.info("Wrote %s -> %s (%s bytes)", source, out_path, f"{out_path.stat().st_size:,}")

        # delete_patterns match paths RELATIVE to path_in_repo ("data/"), so the pattern MUST be
        # "**" — a "data/"-prefixed pattern matches nothing and silently no-ops (ADR-049, verified
        # 2026-06-10). Re-uploaded files are pruned from the delete set by upload_folder itself.
        return upload_guarded(
            staging_dir,
            frames=[guarded],
            repo_id=repo_id,
            token=hf_token,
            delete_patterns=["**"],
        )


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def main() -> None:
    """Extract shot rows from the gold layer and publish the xg-shot-data-v3 dataset pair."""
    logger.info("Starting pre-shot xG v3 shot-data publication pipeline")

    # ------------------------------------------------------------------
    # 1. Validate environment
    # ------------------------------------------------------------------
    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        from huggingface_hub import get_token

        hf_token = get_token() or ""
    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    databricks_host = os.environ.get("DATABRICKS_HOST", "")
    if not databricks_host:
        raise RuntimeError("DATABRICKS_HOST environment variable required")

    databricks_token = os.environ.get("DATABRICKS_TOKEN", "")
    if not databricks_token:
        raise RuntimeError("DATABRICKS_TOKEN environment variable required")

    warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
    if not warehouse_id:
        raise RuntimeError(
            "DATABRICKS_SQL_WAREHOUSE_ID environment variable required. "
            "Get it from: terraform -chdir=terraform/environments/dev output -raw warehouse_id"
        )

    databricks_host = databricks_host.replace("https://", "").replace("http://", "").rstrip("/")
    logger.info("Databricks host: %s", databricks_host)
    logger.info("Warehouse ID: %s", warehouse_id)

    # ------------------------------------------------------------------
    # 2. Query shot rows from the gold layer
    # ------------------------------------------------------------------
    logger.info("Querying shot rows from soccer_analytics.dev_gold.fct_action_values")
    raw_df = query_databricks_sql(
        host=databricks_host,
        token=databricks_token,
        sql=_XG_SHOT_DATA_SQL,
        warehouse_id=warehouse_id,
    )
    if raw_df.empty:
        raise RuntimeError("Query returned no shot rows — check that fct_action_values has been built by dbt")
    logger.info("Retrieved %s raw shot rows", f"{len(raw_df):,}")

    # ------------------------------------------------------------------
    # 3. Normalize dtypes
    # ------------------------------------------------------------------
    logger.info("Normalizing column dtypes")
    shots_df = normalize_dtypes(raw_df)
    type_counts = shots_df["action_type"].value_counts().to_dict()
    source_counts = shots_df["data_source"].value_counts().to_dict()
    logger.info("Shot summary: %s rows, by type: %s, by source: %s", f"{len(shots_df):,}", type_counts, source_counts)

    # ------------------------------------------------------------------
    # 4. Publish to HF Hub — license-gate split (ADR-049/064)
    # ------------------------------------------------------------------
    # Per-match split keyed on access_tier (spec §6.5): public rows -> public repo, restricted
    # AND NULL/unknown -> the private companion (fail-safe; split_restricted never leaks).
    prepared = prepare_public_upload(shots_df, publisher="publish_xg_shot_data_v3_hf")
    if prepared.restricted is None:
        raise RuntimeError("publish_xg_shot_data_v3_hf is registered 'split' — expected a restricted frame")
    public_df, restricted_df = prepared.public.frame, prepared.restricted.frame

    # Fail-closed leak guard on the PUBLIC frame BEFORE upload — needs access_tier present.

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
    # upload — it is constant per repo; keeping it would be a Hyrum additive-schema change to the
    # public dataset. Order is strict: split -> guard -> drop -> upload.

    logger.info("Publishing PUBLIC shot data to HF Hub: %s", DATASET_REPO)
    dataset_url = publish_to_hf_hub(prepared.public, hf_token)

    # Fail-loud ONLY when the restricted set expects data the mart doesn't have (the
    # silent-corpus-shrink class — the trainer reads BOTH repos, spec B2). An EMPTY restricted
    # set is healthy: the always-run restricted publish below then sweeps previously-restricted
    # partitions while this run's public publish carries them.
    if RESTRICTED_HF_PROVIDERS and restricted_df.empty:
        raise RuntimeError(
            f"No rows for restricted providers {sorted(RESTRICTED_HF_PROVIDERS)} in fct_action_values shots — "
            "refusing to publish an empty restricted dataset while the policy expects data "
            "(xg_model_v3 training reads both repos)."
        )
    logger.info(
        "Publishing RESTRICTED shot data (%s rows) to PRIVATE repo: %s",
        f"{len(restricted_df):,}",
        RESTRICTED_DATASET_REPO,
    )
    publish_to_hf_hub(prepared.restricted, hf_token, repo_id=RESTRICTED_DATASET_REPO)

    # ------------------------------------------------------------------
    # 5. Publish READMEs with per-provider configs injected (ADR-054 / ADR-014)
    # ------------------------------------------------------------------
    public_providers = sorted(public_df["data_source"].unique())
    restricted_providers = sorted(restricted_df["data_source"].unique()) if not restricted_df.empty else []
    for repo, card, providers in (
        (DATASET_REPO, "xg-shot-data-v3.md", public_providers),
        (RESTRICTED_DATASET_REPO, "xg-shot-data-v3-restricted.md", restricted_providers),
    ):
        readme_result = upload_hf_readme(
            repo_id=repo,
            readme_path=get_hf_card_path(card, kind="dataset"),
            hf_token=hf_token,
            config_providers=providers,
        )
        logger.info(
            "Uploaded README to %s: %s (sha256=%s)",
            repo,
            readme_result["commit_url"],
            readme_result["sha256"][:8],
        )

    logger.info("Pipeline complete. Dataset: %s", dataset_url)


if __name__ == "__main__":
    main()
