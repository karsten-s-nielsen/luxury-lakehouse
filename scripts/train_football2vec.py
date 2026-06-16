# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse[spadl] @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.42-py3-none-any.whl",
#     "databricks-sdk>=0.20",
#     "gensim>=4.3",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.19",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
# ]
# ///
"""Train Football2Vec v1 (gensim Doc2Vec) on canonical-LTR SPADL actions.

Migrated from notebooks/train_football2vec.py per HF4 (SK3-MIG-B).

Per ADR-012 delivery contract:
- require_mlflow_env() at top of main() — fail loud on missing env (no silent skip).
- set_and_verify_mlflow_champion(...) post-MLflow log_model — zombie-alias guard.
- UC Volume upload (gensim binary; bypasses upload_weights_to_uc_volume's .json
  filename validator — direct files.upload + SHA-256 sidecar matches the helper's
  internal pattern, just with a non-JSON extension).
- HF token via huggingface_hub.get_token() — NOT os.environ.get HF_TOKEN.

Per ADR-014: upload_hf_readme() after the model upload (filename == repo basename).

Data source change vs. notebook:
The legacy notebook trained on raw silver event tables (stg_*__events). SK3-MIG-B
moves training to canonical-LTR SPADL actions (fct_action_values) so the embedding
space reflects SK3-MIG-A's left-to-right orientation. Tokenize via the existing
analytics.football2vec spatial-grid helper after column rename.

Usage (HF Jobs):
    hf jobs uv run scripts/train_football2vec.py \\
        --flavor cpu-large --timeout 90m \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        --secrets DATABRICKS_TOKEN=$DATABRICKS_TOKEN \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_SQL_WAREHOUSE_ID=$DATABRICKS_SQL_WAREHOUSE_ID \\
        --env MLFLOW_TRACKING_URI=databricks
"""

from __future__ import annotations

import hashlib
import io
import logging
import os
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd
import requests
from databricks.sdk import WorkspaceClient
from gensim.models.doc2vec import Doc2Vec
from huggingface_hub import HfApi, get_token

from analytics.football2vec import (
    Football2VecModel,
    TokenizerConfig,
    TrainingConfig,
    tokenize_match_events,
    train_model,
)
from ingestion.artifact_deploy import require_mlflow_env, set_and_verify_mlflow_champion
from ingestion.hf_publish import get_hf_card_path, upload_hf_readme

# Validated HF Jobs flavor — single source of truth, asserted against
# scripts/sk3_mig_b_retrain.py:_FLAVOR_MAP at CI time. f2v_v1 trains on
# CPU; the script docstring's `--flavor cpu-large` example is the validated
# invocation.
VALIDATED_HF_FLAVOR: str = "cpu-xl"

# uv silent-downgrade footgun (CLAUDE.md): a top-level silly-kicks pin in PEP
# 723 deps silently overrides the wheel's transitive pin; explicit pins are an
# active footgun, not a safety net (verified empirically 2026-05-04).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 30, 0)


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} — refusing to train."
        )


logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
HF_MODEL_REPO = f"{HF_ORG}/football2vec-statsbomb-wyscout"
MLFLOW_FQN = "soccer_analytics.dev_gold.football2vec"
UC_CATALOG = "soccer_analytics"
UC_SCHEMA = "dev_gold"
UC_MODEL_NAME = "football2vec"
MODEL_FILENAME = "player2vec.model"

# Per spec §3 F2V v1 acceptance: 32-d, 20 epochs default (training_config defaults).

# SPADL actions from canonical-LTR fct_action_values + canonical_player_id JOIN.
# Filter to StatsBomb + Wyscout (the v1 training providers) and exclude actions
# without spatial coords (the tokenizer drops them anyway, but pre-filter is
# cheaper than per-row tokenize-then-discard).
_SPADL_SQL = """\
SELECT
    p.canonical_player_id           AS canonical_player_id,
    a.match_id                      AS match_id,
    a.action_type                   AS action_type,
    a.start_x                       AS start_x,
    a.start_y                       AS start_y,
    a.time_seconds                  AS event_index,
    a.data_source                   AS data_source
FROM soccer_analytics.dev_gold.fct_action_values a
INNER JOIN soccer_analytics.dev_gold.dim_players p
    ON a.player_key = p.player_key
WHERE a.data_source IN ('statsbomb', 'wyscout')
  AND a.start_x IS NOT NULL
  AND a.start_y IS NOT NULL
"""

_POLL_INTERVAL_S = 2.0
_TIMEOUT_SUBMIT = (10, 120)
_TIMEOUT_POLL = (10, 30)
_TIMEOUT_CHUNK = (10, 300)


def query_databricks_sql(host: str, token: str, sql: str, warehouse_id: str) -> pd.DataFrame:
    """Execute SQL via Databricks Statement Execution + Arrow chunks."""
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


def upload_gensim_to_uc_volume(
    workspace_client: WorkspaceClient,
    *,
    catalog: str,
    schema: str,
    model_name: str,
    filename: str,
    weights_path: Path,
) -> dict[str, str]:
    """Upload gensim binary model + SHA-256 sidecar to UC Volume.

    Mirrors ingestion.artifact_deploy.upload_weights_to_uc_volume but skips that
    helper's `.json` filename validator (gensim Doc2Vec is binary). The artifact
    + sidecar pair is what the inference path's SEC2 hash check expects.
    """
    weights_bytes = weights_path.read_bytes()
    if not weights_bytes:
        raise ValueError("weights_bytes empty; refusing to upload")
    sha256 = hashlib.sha256(weights_bytes).hexdigest()
    artifact_path = f"/Volumes/{catalog}/{schema}/model_weights/{model_name}/{filename}"
    sidecar_path = f"{artifact_path}.sha256"
    workspace_client.files.upload(artifact_path, io.BytesIO(weights_bytes), overwrite=True)
    workspace_client.files.upload(sidecar_path, io.BytesIO(sha256.encode("utf-8")), overwrite=True)
    logger.info("Uploaded %s to UC Volume (%d bytes, sha256=%s)", artifact_path, len(weights_bytes), sha256[:8])
    return {"path": artifact_path, "sha256": sha256}


def main() -> None:
    _assert_silly_kicks_min()

    require_mlflow_env()

    hf_token = get_token() or os.environ.get("HF_TOKEN", "")
    if not hf_token:
        raise RuntimeError(
            "HF token required (huggingface_hub.get_token() or HF_TOKEN env). "
            "Per ADR-012 §2: pass via --secrets HF_TOKEN, NOT --env."
        )

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "").rstrip("/")
    db_token = os.environ["DATABRICKS_TOKEN"]
    warehouse_id = os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]

    logger.info("Querying canonical-LTR SPADL actions from fct_action_values")
    df = query_databricks_sql(host, db_token, _SPADL_SQL, warehouse_id)
    if df.empty:
        raise RuntimeError("0 rows from fct_action_values for statsbomb/wyscout")
    logger.info("Loaded %s SPADL actions across %s players", f"{len(df):,}", df["canonical_player_id"].nunique())

    # Tokenize via existing analytics helper (spatial grid).
    tokenizer_config = TokenizerConfig()
    documents = tokenize_match_events(df, tokenizer_config)
    logger.info("Built %s player-match document corpus", f"{len(documents):,}")
    if not documents:
        raise RuntimeError(
            "tokenize_match_events returned 0 documents — verify SQL produces "
            "canonical_player_id, match_id, action_type, start_x, start_y, event_index."
        )

    # Train.
    training_config = TrainingConfig()
    logger.info("Training Doc2Vec (vector_size=%d, epochs=%d)", training_config.vector_size, training_config.epochs)
    import mlflow

    mlflow.set_experiment("/soccer_analytics/football2vec")
    with mlflow.start_run(run_name="football2vec_training_sk3_mig_b") as run:
        mlflow.log_params(
            {
                "vector_size": training_config.vector_size,
                "window": training_config.window,
                "min_count": training_config.min_count,
                "epochs": training_config.epochs,
                "dm": training_config.dm,
                "n_documents": len(documents),
                "data_source": "fct_action_values (canonical-LTR SPADL)",
            }
        )
        model = train_model(documents, training_config)
        mlflow.log_metrics(
            {
                "vocabulary_size": len(model.wv),
                "document_vectors": len(model.dv),
                "vector_size": model.vector_size,
            }
        )
        logger.info(
            "Training complete: vocab=%d docs=%d vec_dim=%d",
            len(model.wv),
            len(model.dv),
            model.vector_size,
        )

        # Save to a tempdir + register pyfunc model with artifacts.
        with tempfile.TemporaryDirectory(prefix="f2v-v1-") as tmpdir:
            local_path = Path(tmpdir) / MODEL_FILENAME
            model.save(str(local_path))
            mlflow.pyfunc.log_model(
                artifact_path="football2vec_model",
                python_model=Football2VecModel(),
                artifacts={"model_dir": str(tmpdir)},
                registered_model_name=MLFLOW_FQN,
            )
            run_id = run.info.run_id
            logger.info("MLflow run_id=%s", run_id)

            # ADR-012 zombie-alias guard.
            mlflow_client = mlflow.tracking.MlflowClient()
            version = set_and_verify_mlflow_champion(
                mlflow_client,
                mlflow_fqn=MLFLOW_FQN,
                run_id=run_id,
            )

            # ADR-012 second leg: UC Volume.
            workspace_client = WorkspaceClient()
            upload_gensim_to_uc_volume(
                workspace_client,
                catalog=UC_CATALOG,
                schema=UC_SCHEMA,
                model_name=UC_MODEL_NAME,
                filename=MODEL_FILENAME,
                weights_path=local_path,
            )

            # HF Hub upload (model repo only — no embeddings here; embeddings are
            # published separately via publish_football2vec_embeddings_hf.py).
            api = HfApi(token=hf_token)
            api.create_repo(repo_id=HF_MODEL_REPO, repo_type="model", exist_ok=True, token=hf_token)
            api.upload_folder(
                folder_path=str(tmpdir),
                repo_id=HF_MODEL_REPO,
                repo_type="model",
                token=hf_token,
            )
            logger.info("Uploaded model to %s", HF_MODEL_REPO)

            # ADR-014 README upload.
            upload_hf_readme(
                repo_id=HF_MODEL_REPO,
                readme_path=get_hf_card_path("football2vec-statsbomb-wyscout.md", kind="model"),
                hf_token=hf_token,
                repo_type="model",
            )

            # Smoke verification BEFORE the tempdir cleans up.
            reloaded = Doc2Vec.load(str(local_path))
            if len(reloaded.wv) != len(model.wv):
                raise RuntimeError(f"Reload vocab size drift: original={len(model.wv)} reloaded={len(reloaded.wv)}")

    logger.info("Pipeline complete (Champion v%s, HF=%s)", version, HF_MODEL_REPO)


if __name__ == "__main__":
    main()
