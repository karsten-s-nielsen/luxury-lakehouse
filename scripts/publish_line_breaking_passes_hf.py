# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.34-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish line-breaking passes (fct_passes with line-breaking enrichment) to HF Hub.

Migrated from the line-breaking cell of notebooks/publish_datasets.py per HF4
(SK3-MIG-B). Inventory-only — NOT fired by the SK3-MIG-B Group 3 republishes
(line-breaking detection runs from the canonical SPADL-LTR fct_passes; coord
correctness is preserved through SK3-MIG-A).

Dataset: luxury-lakehouse/line-breaking-passes
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

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
DATASET_REPO = f"{HF_ORG}/line-breaking-passes"

_PASSES_SQL = """\
SELECT p.pass_id,
       p.match_key, p.team_key, p.passer_player_key, p.recipient_player_key,
       dm.native_match_id AS match_id,
       p.player_id, p.team_id, p.pass_recipient_id,
       p.competition_id, p.season_id, p.period, p.minute, p.second,
       p.start_x, p.start_y, p.end_x, p.end_y,
       p.pass_type, p.pass_height, p.body_part,
       p.pass_length, p.pass_angle_radians,
       p.pass_outcome, p.is_cross, p.is_switch, p.is_through_ball,
       p.is_complete, p.is_progressive,
       p.pass_direction, p.is_line_breaking, p.lines_broken, p.line_breaking_type,
       p.data_source
FROM soccer_analytics.dev_gold.fct_passes p
LEFT JOIN soccer_analytics.dev_gold.dim_matches dm ON p.match_key = dm.match_key
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


def publish_to_hf_hub(df: pd.DataFrame, hf_token: str) -> str:
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)
    api.create_repo(DATASET_REPO, exist_ok=True, repo_type="dataset", token=hf_token)
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)
        for source, sub_df in df.groupby("data_source"):
            partition_dir = staging_dir / f"data_source={source}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            sub_df.drop(columns=["data_source"]).to_parquet(
                partition_dir / "data.parquet",
                index=False,
                engine="pyarrow",
            )
        api.upload_folder(
            folder_path=str(staging_dir),
            path_in_repo="data",
            repo_id=DATASET_REPO,
            repo_type="dataset",
            token=hf_token,
            delete_patterns=["data/*"],
        )
    return f"https://huggingface.co/datasets/{DATASET_REPO}"


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

    df = query_databricks_sql(host, db_token, _PASSES_SQL, warehouse_id)
    if df.empty:
        raise RuntimeError("0 rows from fct_passes — verify dbt build")
    logger.info("Retrieved %s passes (%s line-breaking)", f"{len(df):,}", f"{int(df['is_line_breaking'].sum()):,}")

    url = publish_to_hf_hub(df, hf_token)
    upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_hf_card_path("line-breaking-passes.md", kind="dataset"),
        hf_token=hf_token,
    )
    logger.info("Pipeline complete: %s", url)


if __name__ == "__main__":
    main()
