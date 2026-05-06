# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.34-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish Football2Vec player embeddings (career / season / per-match) to HF Hub.

Migrated from the embeddings cell of notebooks/publish_datasets.py per HF4
(SK3-MIG-B). Fired by SK3-MIG-B Group 3 republishes after F2V v1/v2/360 retrain.

Dataset: luxury-lakehouse/football2vec-player-embeddings

Three sub-tables uploaded under data/career/, data/season/, data/per_match/.
Each sub-table is its own parquet (no per-source partitioning — the
data_sources column inside each row carries the array of source providers).
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
DATASET_REPO = f"{HF_ORG}/football2vec-player-embeddings"

_CAREER_SQL = """\
SELECT e.canonical_player_id, p.player_name,
       e.behavioral_vector, e.stat_vector,
       e.total_matches, e.data_sources
FROM soccer_analytics.dev_gold.fct_player_embeddings_career e
LEFT JOIN soccer_analytics.dev_gold.dim_players p
  ON e.canonical_player_id = p.canonical_player_id
"""

_SEASON_SQL = """\
SELECT embedding_season_id, canonical_player_id, competition_id, season_id,
       behavioral_vector, stat_vector, matches_in_sample, data_sources
FROM soccer_analytics.dev_gold.fct_player_embeddings_season
"""

_PER_MATCH_SQL = """\
SELECT embedding_id, canonical_player_id, match_id, data_source,
       behavioral_vector, stat_vector
FROM soccer_analytics.dev_gold.fct_player_embeddings
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


def publish_to_hf_hub(
    career_df: pd.DataFrame,
    season_df: pd.DataFrame,
    per_match_df: pd.DataFrame,
    hf_token: str,
) -> str:
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)
    api.create_repo(DATASET_REPO, exist_ok=True, repo_type="dataset", token=hf_token)

    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        for sub_dir, sub_df in (
            ("career", career_df),
            ("season", season_df),
            ("per_match", per_match_df),
        ):
            target_dir = staging_dir / sub_dir
            target_dir.mkdir(parents=True, exist_ok=True)
            sub_df.to_parquet(target_dir / "data.parquet", index=False, engine="pyarrow")
            logger.info("Wrote %s/%s: %s rows", sub_dir, "data.parquet", f"{len(sub_df):,}")
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

    logger.info("Querying career embeddings")
    career_df = query_databricks_sql(host, db_token, _CAREER_SQL, warehouse_id)
    logger.info("Querying season embeddings")
    season_df = query_databricks_sql(host, db_token, _SEASON_SQL, warehouse_id)
    logger.info("Querying per-match embeddings")
    per_match_df = query_databricks_sql(host, db_token, _PER_MATCH_SQL, warehouse_id)

    if career_df.empty or season_df.empty or per_match_df.empty:
        raise RuntimeError(
            f"One or more embedding marts empty: career={len(career_df)} "
            f"season={len(season_df)} per_match={len(per_match_df)}"
        )

    url = publish_to_hf_hub(career_df, season_df, per_match_df, hf_token)
    upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_hf_card_path("football2vec-player-embeddings.md", kind="dataset"),
        hf_token=hf_token,
    )
    logger.info(
        "Pipeline complete: %s (career=%s season=%s per_match=%s)",
        url,
        f"{len(career_df):,}",
        f"{len(season_df):,}",
        f"{len(per_match_df):,}",
    )


if __name__ == "__main__":
    main()
