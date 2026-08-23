# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.97-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish the pre-shot xG v3 freeze-frame set from Databricks bronze to HF Hub.

Datasets: luxury-lakehouse/xg-shot-freeze-frames (public)
          luxury-lakehouse/xg-shot-freeze-frames-restricted (private companion, ADR-049)

This is the CONTEXT half of the ``xg_model_v3`` training corpus (spec §A4): one row per
(shot, player) from the ``bronze.shot_freeze_frames`` table, carrying the canonical-SPADL
player positions + set-encoder flags that the trainer joins to the tabular shot rows
(``xg-shot-data-v3``) on ``(match_key, action_id)``. ``bronze.shot_freeze_frames`` is already
shot-scoped (only shot rows are written by ``compute_shot_freeze_frames``), so there is NO
action_type / provider filter in the SQL — the whole table is published.

The HF license gate is applied at the PUBLISH split (ingestion.hf_publish.split_restricted —
ADR-049/064): restricted rows (RM SkillCorner + GS per-match) go to the PRIVATE companion repo,
the rest to the public repo. The gate lives ONLY at the split — there is NO SQL-side
``data_source`` filter (a SQL filter silently shrinks the restricted repo and any training corpus).

Usage (HF Jobs CLI):
    hf jobs uv run scripts/publish_shot_freeze_frames_hf.py \\
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
import time
from pathlib import Path

import pandas as pd
import requests

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
DATASET_REPO = f"{HF_ORG}/xg-shot-freeze-frames"
# PRIVATE companion repo for license-gated partitions (ADR-049; org-members only). The naming
# convention + split criterion are owned by ingestion.hf_publish (single source of truth — the
# xg_model_v3 trainer imports the same constants, so the publish split and the training-corpus
# expectation can never drift). The pair is PERMANENT infrastructure: both repos are ensured on
# every run, even when the restricted set is empty.
RESTRICTED_DATASET_REPO = restricted_repo_id(DATASET_REPO)

# Per-(shot, player) freeze-frame rows from the bronze table. bronze.shot_freeze_frames is ALREADY
# shot-scoped (compute_shot_freeze_frames writes only shot rows), so there is deliberately NO
# action_type / type_id filter and NO data_source predicate here — the whole table is published and
# the license gate is the access_tier split below.
_SHOT_FREEZE_FRAMES_SQL = """\
SELECT
    match_key,
    action_id,
    data_source,
    player_id,
    x,
    y,
    is_keeper,
    is_teammate,
    set_cardinality,
    shooter_attacks_high_x,
    team_attacking_direction,
    access_tier
FROM soccer_analytics.bronze.shot_freeze_frames
"""

# Databricks SQL Statement Execution API polling interval (seconds)
_POLL_INTERVAL_S = 2.0

# HTTP timeouts: (connect, read) in seconds
_TIMEOUT_SUBMIT = (10, 120)
_TIMEOUT_POLL = (10, 30)
_TIMEOUT_CHUNK = (10, 120)


# ---------------------------------------------------------------------------
# Databricks SQL Statement Execution API
# ---------------------------------------------------------------------------


def query_databricks_sql(host: str, token: str, sql: str, warehouse_id: str) -> pd.DataFrame:
    """Execute SQL via Databricks Statement Execution API and return DataFrame.

    Handles asynchronous execution (PENDING/RUNNING states) and paginated result chunks
    using EXTERNAL_LINKS disposition with Arrow stream format.

    Args:
        host: Databricks workspace hostname (without ``https://`` prefix).
        token: Databricks personal access token or OAuth token.
        sql: SQL statement to execute.
        warehouse_id: SQL warehouse ID to target.

    Returns:
        DataFrame with columns matching the SQL result schema.

    Raises:
        RuntimeError: If the SQL statement fails on the server or returns no data.
        requests.HTTPError: If an API call returns a non-2xx status.
    """
    url = f"https://{host}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "50s",
        "disposition": "EXTERNAL_LINKS",
        "format": "ARROW_STREAM",
    }

    logger.info("Submitting SQL query to Databricks (warehouse=%s)", warehouse_id)
    resp = requests.post(url, json=payload, headers=headers, timeout=_TIMEOUT_SUBMIT, verify=True)
    if resp.status_code != 200:
        logger.error("SQL API error %d: %s", resp.status_code, resp.text[:500])
    resp.raise_for_status()
    result = resp.json()

    # Poll until terminal state if the query is still running.
    statement_id = result.get("statement_id")
    status = result.get("status", {}).get("state")
    logger.info("Statement %s — initial state: %s", statement_id, status)

    while status in ("PENDING", "RUNNING"):
        time.sleep(_POLL_INTERVAL_S)
        poll_resp = requests.get(f"{url}/{statement_id}", headers=headers, timeout=_TIMEOUT_POLL, verify=True)
        poll_resp.raise_for_status()
        result = poll_resp.json()
        status = result.get("status", {}).get("state")
        logger.info("Statement %s — polled state: %s", statement_id, status)

    if status == "FAILED":
        error = result.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL statement failed: {error.get('message', 'unknown error')}")

    if status != "SUCCEEDED":
        raise RuntimeError(f"Unexpected terminal state: {status}")

    manifest = result.get("manifest", {})
    columns = [col["name"] for col in manifest.get("schema", {}).get("columns", [])]
    total_row_count = manifest.get("total_row_count", "unknown")
    total_chunk_count = manifest.get("total_chunk_count", 0)
    logger.info("Query returned %s rows in %s chunks, columns: %s", total_row_count, total_chunk_count, columns)

    import pyarrow as pa

    arrow_tables: list[pa.Table] = []
    n_chunks = int(total_chunk_count) if total_chunk_count else 0

    for chunk_idx in range(n_chunks):
        chunk_url = f"{url}/{statement_id}/result/chunks/{chunk_idx}"
        logger.info("Fetching chunk %d/%d", chunk_idx + 1, n_chunks)
        chunk_resp = requests.get(chunk_url, headers=headers, timeout=_TIMEOUT_CHUNK, verify=True)
        chunk_resp.raise_for_status()
        for link_info in chunk_resp.json().get("external_links", []):
            link_url = link_info["external_link"]
            byte_count = link_info.get("byte_count", 0)
            logger.info("  Downloading %d bytes", byte_count)
            dl_resp = requests.get(link_url, timeout=_TIMEOUT_CHUNK, verify=True)
            dl_resp.raise_for_status()
            reader = pa.ipc.open_stream(dl_resp.content)
            arrow_tables.append(reader.read_all())

    if not arrow_tables:
        raise RuntimeError("No data chunks returned from Databricks SQL")

    combined = pa.concat_tables(arrow_tables)
    logger.info("Collected %d total rows from %d Arrow chunks", combined.num_rows, len(arrow_tables))
    return combined.to_pandas()


# ---------------------------------------------------------------------------
# dtype normalization
# ---------------------------------------------------------------------------


def normalize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the published-contract dtypes to freeze-frame columns after Arrow deserialization.

    Contract (spec §7): ``match_key``/``action_id`` BIGINT, ``x``/``y`` DOUBLE,
    ``is_keeper``/``is_teammate``/``set_cardinality`` INT, ``shooter_attacks_high_x`` BOOLEAN,
    ``player_id``/``data_source``/``team_attacking_direction`` STRING. ``shooter_attacks_high_x`` is
    a NULLABLE ``boolean`` (a shot whose team attacking direction could not be derived leaves it NA),
    and ``access_tier`` is a NULLABLE ``string`` — a plain ``astype(str)`` would coerce NULL into the
    literal ``"nan"`` and defeat the fail-safe split (``split_restricted`` treats NULL/unknown as
    restricted).

    Args:
        df: Raw DataFrame from the Databricks SQL API response.

    Returns:
        DataFrame with enforced dtypes matching the HF Hub schema contract.
    """
    # Integer keys (Kimball surrogate + per-match action id).
    for col in ("match_key", "action_id"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Integer set-encoder flags / count (nullable Int64 tolerates any upstream NULL).
    for col in ("is_keeper", "is_teammate", "set_cardinality"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Float geometry columns (canonical SPADL 105x68).
    for col in ("x", "y"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # String columns (non-null by contract).
    for col in ("player_id", "data_source", "team_attacking_direction"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    # NULLABLE boolean — the per-shot orientation is NA when team_attacking_direction could not be
    # derived; astype("boolean") preserves <NA> (a plain bool cast would coerce NA to False).
    for col in ("shooter_attacks_high_x",):
        if col in df.columns:
            df[col] = df[col].astype("boolean")

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
    """Extract freeze-frame rows from bronze and publish the xg-shot-freeze-frames dataset pair."""
    logger.info("Starting pre-shot xG v3 freeze-frame publication pipeline")

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
    # 2. Query freeze-frame rows from the bronze layer
    # ------------------------------------------------------------------
    logger.info("Querying freeze-frame rows from soccer_analytics.bronze.shot_freeze_frames")
    raw_df = query_databricks_sql(
        host=databricks_host,
        token=databricks_token,
        sql=_SHOT_FREEZE_FRAMES_SQL,
        warehouse_id=warehouse_id,
    )
    if raw_df.empty:
        raise RuntimeError(
            "Query returned no freeze-frame rows — check that compute_shot_freeze_frames has populated "
            "bronze.shot_freeze_frames"
        )
    logger.info("Retrieved %s raw freeze-frame rows", f"{len(raw_df):,}")

    # ------------------------------------------------------------------
    # 3. Normalize dtypes
    # ------------------------------------------------------------------
    logger.info("Normalizing column dtypes")
    frames_df = normalize_dtypes(raw_df)
    source_counts = frames_df["data_source"].value_counts().to_dict()
    shot_counts = frames_df.groupby("data_source")[["match_key", "action_id"]].apply(
        lambda g: g.drop_duplicates().shape[0]
    )
    logger.info(
        "Freeze-frame summary: %s player-rows, by source: %s, distinct shots by source: %s",
        f"{len(frames_df):,}",
        source_counts,
        shot_counts.to_dict(),
    )

    # ------------------------------------------------------------------
    # 4. Publish to HF Hub — license-gate split (ADR-049/064)
    # ------------------------------------------------------------------
    # Per-match split keyed on access_tier (spec §6.5): public rows -> public repo, restricted
    # AND NULL/unknown -> the private companion (fail-safe; split_restricted never leaks).
    prepared = prepare_public_upload(frames_df, publisher="publish_shot_freeze_frames_hf")
    if prepared.restricted is None:
        raise RuntimeError("publish_shot_freeze_frames_hf is registered 'split' — expected a restricted frame")
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

    logger.info("Publishing PUBLIC freeze frames to HF Hub: %s", DATASET_REPO)
    dataset_url = publish_to_hf_hub(prepared.public, hf_token)

    # Fail-loud ONLY when the restricted set expects data the mart doesn't have (the
    # silent-corpus-shrink class — the trainer reads BOTH repos, spec B2). An EMPTY restricted
    # set is healthy: the always-run restricted publish below then sweeps previously-restricted
    # partitions while this run's public publish carries them.
    if RESTRICTED_HF_PROVIDERS and restricted_df.empty:
        raise RuntimeError(
            f"No rows for restricted providers {sorted(RESTRICTED_HF_PROVIDERS)} in bronze.shot_freeze_frames — "
            "refusing to publish an empty restricted dataset while the policy expects data "
            "(xg_model_v3 training reads both repos)."
        )
    logger.info(
        "Publishing RESTRICTED freeze frames (%s rows) to PRIVATE repo: %s",
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
        (DATASET_REPO, "xg-shot-freeze-frames.md", public_providers),
        (RESTRICTED_DATASET_REPO, "xg-shot-freeze-frames-restricted.md", restricted_providers),
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
