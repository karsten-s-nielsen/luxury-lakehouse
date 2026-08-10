# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.93-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish OBSO+PAUSA prerequisite inputs (IDSSE events + ELASTIC sync) to HF Hub.

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
import time
from pathlib import Path

import pandas as pd
import requests

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

# Combine IDSSE events + ELASTIC sync results in a single denormalized payload.
_INPUTS_SQL = """\
SELECT
    e.match_id,
    e.event_id,
    e.event_type,
    e.timestamp_seconds,
    e.period,
    e.player_id,
    e.team,
    e.x,
    e.y,
    sync.frame_id,
    sync.alignment_confidence,
    sync.alignment_error_seconds,
    dm.access_tier
FROM soccer_analytics.bronze.idsse_events e
INNER JOIN soccer_analytics.bronze.elastic_sync_results sync
    ON e.match_id = sync.match_id AND e.event_id = sync.event_id
-- ADR-072 / R-13: prepare_public_upload refuses a frame with no access_tier column, so "no
-- restricted rows" and "no tier column" are not interchangeable. idsse_events carries the NATIVE
-- string match id, so the join is on (provider, native_match_id) — NOT match_key, which this
-- bronze source does not have. IDSSE is public-by-licence, so this publisher stays fail_closed;
-- the join lets it PROVE that rather than assume it.
LEFT JOIN soccer_analytics.dev_gold.dim_matches dm
    ON dm.provider = 'idsse' AND dm.native_match_id = e.match_id
"""

_POLL_INTERVAL_S = 2.0
_TIMEOUT_SUBMIT = (10, 120)
_TIMEOUT_POLL = (10, 30)
_TIMEOUT_CHUNK = (10, 120)


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
    if resp.status_code != 200:
        logger.error("SQL API error %d: %s", resp.status_code, resp.text[:500])
    resp.raise_for_status()
    result = resp.json()

    statement_id = result.get("statement_id")
    status = result.get("status", {}).get("state")
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

    if status == "FAILED":
        error = result.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL failed: {error.get('message', 'unknown')}")
    if status != "SUCCEEDED":
        raise RuntimeError(f"Unexpected state: {status}")

    manifest = result.get("manifest", {})
    total_chunks = int(manifest.get("total_chunk_count", 0) or 0)

    import pyarrow as pa

    arrow_tables: list[pa.Table] = []
    for chunk_idx in range(total_chunks):
        chunk_url = f"{url}/{statement_id}/result/chunks/{chunk_idx}"
        chunk_resp = requests.get(chunk_url, headers=headers, timeout=_TIMEOUT_CHUNK, verify=True)
        chunk_resp.raise_for_status()
        chunk_data = chunk_resp.json()
        for link_info in chunk_data.get("external_links", []):
            dl_resp = requests.get(link_info["external_link"], timeout=_TIMEOUT_CHUNK, verify=True)
            dl_resp.raise_for_status()
            reader = pa.ipc.open_stream(dl_resp.content)
            arrow_tables.append(reader.read_all())

    if not arrow_tables:
        raise RuntimeError("No data chunks returned")
    combined = pa.concat_tables(arrow_tables)
    logger.info("Collected %d rows from %d chunks", combined.num_rows, len(arrow_tables))
    return combined.to_pandas()


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
        raise RuntimeError("Query returned 0 rows — verify idsse_events + elastic_sync_results are populated")
    logger.info("Retrieved %s rows across %s matches", f"{len(df):,}", df["match_id"].nunique())

    # R-13: LEFT JOIN on dim_matches, so an unmatched match yields NULL. split_restricted
    # fail-safes NULL to restricted, which for this fail_closed publisher would silently WITHHOLD
    # public open data. Fail loud instead.
    unmatched = int(df["access_tier"].isna().sum())
    if unmatched:
        raise RuntimeError(
            f"publish_obso_pausa_inputs_hf: {unmatched} rows have NULL access_tier "
            f"(idsse match_id missing from dim_matches) — refusing to publish and silently withhold public data"
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
