# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.87-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish on-target shots from Databricks gold layer to HF Hub.

Local PEP 723 alternative to the Databricks workflow task in
``ingestion.export_shots_on_target``. Same SELECT + same destination repo,
but runs via Databricks SQL Statement Execution API so it can be invoked
from a local shell without uploading a notebook + Python source tree to
the workspace.

Filter: true on-target (``shot_outcome IN ('Goal','Saved','Post','Saved to Post')``
with a non-null ``end_location_z`` coordinate guard), used for PSxG model training.
The SELECT + filter are the SINGLE source of truth in
``ingestion.export_shots_on_target._build_query`` (imported below) — do NOT
re-inline the SQL here; a divergent copy is what contaminated the training
population in the first place (D-0, spec 2026-06-20-psxg-tracking-extension).

Usage (locally with env vars set):
    DATABRICKS_SQL_WAREHOUSE_ID=<id> uv run --no-project --script \
        scripts/publish_shots_on_target_hf.py

Usage (HF Jobs CLI):
    hf jobs uv run scripts/publish_shots_on_target_hf.py \
        --flavor cpu-basic --timeout 30m \
        --secrets HF_TOKEN \
        --env DATABRICKS_HOST=$DATABRICKS_HOST \
        --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN \
        --env DATABRICKS_SQL_WAREHOUSE_ID=$DATABRICKS_SQL_WAREHOUSE_ID
"""

from __future__ import annotations

import io
import logging
import os
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd
import requests

from ingestion.export_shots_on_target import _build_query
from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from ingestion.hf_upload_seam import GuardedFrame, prepare_public_upload, upload_guarded

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
DATASET_REPO = f"{HF_ORG}/statsbomb-shots-on-target"

# Single source of truth for the SELECT + on-target filter (D-0). This local
# PEP 723 publisher targets the same gold catalog/schema as the workflow task.
_SHOTS_SQL = _build_query("soccer_analytics", "dev_gold")
_PARQUET_FILENAME = "shots_on_target.parquet"

_POLL_INTERVAL_S = 2.0
_TIMEOUT_SUBMIT = (10, 120)
_TIMEOUT_POLL = (10, 30)
_TIMEOUT_CHUNK = (10, 60)


def query_databricks_sql(host: str, token: str, sql: str, warehouse_id: str) -> pd.DataFrame:
    """Execute SQL via Databricks Statement Execution API and return DataFrame."""
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
    resp.raise_for_status()
    result = resp.json()

    statement_id = result.get("statement_id")
    status = result.get("status", {}).get("state")
    logger.info("Statement %s — initial state: %s", statement_id, status)

    while status in ("PENDING", "RUNNING"):
        time.sleep(_POLL_INTERVAL_S)
        poll_resp = requests.get(
            f"{url}/{statement_id}",
            headers=headers,
            timeout=_TIMEOUT_POLL,
            verify=True,
        )
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
    logger.info(
        "Query returned %s rows in %s chunks, columns: %s",
        total_row_count,
        total_chunk_count,
        columns,
    )

    import pyarrow as pa  # type: ignore[import-not-found]

    arrow_tables: list[pa.Table] = []
    n_chunks = int(total_chunk_count) if total_chunk_count else 0
    for chunk_idx in range(n_chunks):
        logger.info("Fetching chunk %d/%d", chunk_idx + 1, n_chunks)
        chunk_url = f"{url}/{statement_id}/result/chunks/{chunk_idx}"
        chunk_resp = requests.get(chunk_url, headers=headers, timeout=_TIMEOUT_POLL, verify=True)
        chunk_resp.raise_for_status()
        chunk_meta = chunk_resp.json()
        external_link = chunk_meta.get("external_links", [{}])[0].get("external_link")
        if not external_link:
            raise RuntimeError(f"Chunk {chunk_idx} missing external_link")
        # External-link fetches don't take the auth header
        data_resp = requests.get(external_link, timeout=_TIMEOUT_CHUNK, verify=True)
        data_resp.raise_for_status()
        with pa.ipc.open_stream(io.BytesIO(data_resp.content)) as reader:
            arrow_tables.append(reader.read_all())
    if not arrow_tables:
        raise RuntimeError("No data chunks fetched")
    final = pa.concat_tables(arrow_tables)
    logger.info("Collected %d total rows from %d Arrow chunks", final.num_rows, n_chunks)
    return final.to_pandas()


def publish_to_hf_hub(guarded: GuardedFrame, hf_token: str) -> str:
    """Stage the single canonical Parquet and upload it, sweeping stale siblings.

    Single-file canonical layout: ``load_shots`` concatenates every ``data/*.parquet``, so a
    leftover part-file of a different schema/population silently contaminates training (the ADR-049
    stale-part-file class — a mixed population poisoned a PSxG retrain on 2026-06-21).
    ``delete_patterns`` are matched RELATIVE to ``path_in_repo`` ("data") and ``upload_folder``
    prunes re-uploaded files from the delete set, so ``["**"]`` removes every stale sibling while
    keeping the file just written. This replaces the previous ``list_repo_files``/``delete_files``
    pair, which needed a raw ``HfApi`` client the seam no longer permits (ADR-072).

    Extracted from ``main()`` so the staged tree and upload contract are testable without
    credentials (``test_publisher_upload_contract.py``) — and so this publisher has the same shape
    as its twelve siblings.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        guarded.write_parquet(staging_dir / _PARQUET_FILENAME)
        return upload_guarded(
            staging_dir,
            frames=[guarded],
            repo_id=DATASET_REPO,
            token=hf_token,
            delete_patterns=["**"],
        )


def main() -> None:
    host = os.environ.get("DATABRICKS_HOST", "").replace("https://", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
    hf_token = os.environ.get("HF_TOKEN", "")

    for name, val in [
        ("DATABRICKS_HOST", host),
        ("DATABRICKS_TOKEN", token),
        ("DATABRICKS_SQL_WAREHOUSE_ID", warehouse_id),
        ("HF_TOKEN", hf_token),
    ]:
        if not val:
            raise SystemExit(f"{name} environment variable required")

    df = query_databricks_sql(host, token, _SHOTS_SQL, warehouse_id)
    logger.info("Loaded %d on-target shots into pandas DataFrame", len(df))

    # R-12: the SELECT LEFT JOINs dim_matches, so an unmatched match_key yields a NULL tier.
    # split_restricted fail-safes NULL to restricted, which for this fail_closed publisher would
    # silently WITHHOLD public data. Fail loud instead — silent withholding is the failure class
    # this whole change exists to prevent.
    unmatched = int(df["access_tier"].isna().sum())
    if unmatched:
        raise RuntimeError(
            f"publish_shots_on_target_hf: {unmatched} shot rows have NULL access_tier "
            f"(match_key missing from dim_matches) — refusing to publish and silently withhold public data"
        )

    # NOTE: fct_shots is fed by int_unified_shots, which today has only the statsbomb and wyscout
    # legs — both public-by-licence — so this guard passes on current data. The day a SkillCorner or
    # Gradient Sports shot leg joins that mart, this publisher hard-fails every run until it is
    # converted from fail_closed to split. That dependency is deliberate and loud.
    prepared = prepare_public_upload(df, publisher="publish_shots_on_target_hf")
    url = publish_to_hf_hub(prepared.public, hf_token)
    logger.info("Uploaded parquet to %s", url)

    readme_result = upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_hf_card_path("statsbomb-shots-on-target.md", kind="dataset"),
        hf_token=hf_token,
    )
    logger.info(
        "Uploaded README: %s (sha256=%s)",
        readme_result["commit_url"],
        readme_result["sha256"][:8],
    )
    logger.info("Published dataset to https://huggingface.co/datasets/%s", DATASET_REPO)


if __name__ == "__main__":
    main()
