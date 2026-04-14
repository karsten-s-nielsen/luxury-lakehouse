#!/usr/bin/env python3
"""SEC2 one-off: bootstrap SHA-256 hashes for all model artifacts.

Walks the 4 model paths used by the daily ingestion pipeline and writes:

- MLflow run tag ``artifact_sha256`` for @Champion-aliased models
- ``<file>.sha256`` sidecar files for UC Volume artifacts

Idempotent: re-running with ``--apply`` is a no-op when hashes already
match. Defense-in-depth for SEC-AUDIT-v1.12.0 ML-02 (CWE-345).

Usage:
    # Dry run — print planned operations without making changes
    python scripts/bootstrap_artifact_hashes.py --catalog soccer_analytics --schema dev_gold --dry-run

    # Apply — write tags and sidecars
    python scripts/bootstrap_artifact_hashes.py --catalog soccer_analytics --schema dev_gold --apply
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sys
from typing import TYPE_CHECKING

from shared.constants import IDENTIFIER_RE

if TYPE_CHECKING:
    from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)

# Models with @Champion aliases in MLflow
_MLFLOW_MODELS = ["xg_model", "xg_model_v2", "vaep_model", "defcon_model"]

# UC Volume artifact paths (relative to /Volumes/{catalog}/{schema}/model_weights/)
_VOLUME_ARTIFACTS = [
    "xg_model/logistic_model.json",
    "xg_model/xgboost_model.json",
    "xg_model_v2/model_weights.json",
]


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def download_artifact_bytes(run_id: str, artifact_path: str) -> bytes:
    """Download an MLflow artifact and return its bytes.

    Wraps ``mlflow.artifacts.download_artifacts`` (which returns a local
    path) with a file read.
    """
    from mlflow.artifacts import download_artifacts

    local_path = download_artifacts(run_id=run_id, artifact_path=artifact_path)
    with open(local_path, "rb") as f:
        return f.read()


def bootstrap_mlflow_model(
    mlflow_client: MlflowClient,
    catalog: str,
    schema: str,
    model_name: str,
    *,
    apply: bool,
) -> int:
    """Tag the @Champion run of ``{catalog}.{schema}.{model_name}`` with its artifact SHA-256.

    Returns:
        1 if the tag was written or would have been written; 0 if no change
        (already correct or model not found).
    """
    full_name = f"{catalog}.{schema}.{model_name}"
    try:
        version = mlflow_client.get_model_version_by_alias(full_name, "Champion")
    except Exception:
        logger.info("No @Champion alias for %s — skipping", full_name)
        return 0

    run_id = version.run_id
    if run_id is None:
        logger.info("No run_id for %s @Champion — skipping", full_name)
        return 0
    # Bootstrap ONLY the xg_model_v2-style flavor where the loader reads the raw
    # artifact file (model_weights.json) byte-for-byte. The sklearn and pyfunc
    # flavors used by xg_model v1, vaep_model, and defcon_model materialize the
    # model in-memory (mlflow_sklearn.load_model / mlflow_pyfunc.load_model) and
    # the loader then re-serializes via serialize_xgboost_model / save_raw("json")
    # to produce bytes — that serialize output is NOT bytewise-identical to the
    # raw MLflow artifact (model.pkl), so a tag written from the raw artifact
    # bytes would never match the loader's verify_artifact_hash(data=...). We
    # therefore refuse to bootstrap those models via MLflow tags; their loaders
    # fail-open when the tag is absent. If xg_model_v2 (or any future raw-json
    # flavor) is the only @Champion model, only that one gets a tag.
    try:
        artifact_bytes = download_artifact_bytes(run_id, "model_weights.json")
    except Exception:
        logger.info(
            "No model_weights.json artifact for %s — skipping MLflow tag bootstrap "
            "(sklearn/pyfunc flavors are not bytewise-stable for tag-based "
            "verification; see comment in bootstrap_mlflow_model).",
            full_name,
        )
        return 0

    new_hash = _sha256(artifact_bytes)
    run = mlflow_client.get_run(run_id)
    existing_hash = run.data.tags.get("artifact_sha256")

    if existing_hash == new_hash:
        logger.info("%s: hash already recorded (%s) — no change", full_name, new_hash[:8])
        return 0

    if apply:
        mlflow_client.set_tag(run_id, "artifact_sha256", new_hash)
        logger.info("%s: wrote artifact_sha256 tag (%s)", full_name, new_hash[:8])
    else:
        logger.info("[DRY-RUN] %s: would write artifact_sha256=%s", full_name, new_hash[:8])
    return 1


def bootstrap_volume_artifact(
    workspace_client: object,
    catalog: str,
    schema: str,
    relative_path: str,
    *,
    apply: bool,
) -> int:
    """Write a ``.sha256`` sidecar file alongside a UC Volume model artifact.

    Returns:
        1 if the sidecar was written or would have been; 0 if already
        correct or artifact missing.
    """
    volume_path = f"/Volumes/{catalog}/{schema}/model_weights/{relative_path}"
    sidecar_path = volume_path + ".sha256"

    try:
        # Use the Files API via WorkspaceClient
        response = workspace_client.files.download(volume_path)  # type: ignore[attr-defined]
        artifact_bytes = response.contents.read()
    except Exception:
        logger.info("Volume artifact not found (skipping): %s", volume_path)
        return 0

    new_hash = _sha256(artifact_bytes)

    # Check if sidecar exists and matches
    try:
        sidecar_response = workspace_client.files.download(sidecar_path)  # type: ignore[attr-defined]
        existing = sidecar_response.contents.read().decode("utf-8").strip()
        if existing == new_hash:
            logger.info("%s: sidecar already recorded — no change", relative_path)
            return 0
    except Exception:
        logger.debug("%s: sidecar not found — will create", relative_path)

    if apply:
        import io

        workspace_client.files.upload(  # type: ignore[attr-defined]
            sidecar_path, io.BytesIO(new_hash.encode("utf-8")), overwrite=True
        )
        logger.info("%s: wrote sidecar (%s)", relative_path, new_hash[:8])
    else:
        logger.info("[DRY-RUN] %s: would write sidecar=%s", relative_path, new_hash[:8])
    return 1


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Bootstrap SHA-256 hashes for model artifacts."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Bootstrap SHA-256 hashes for model artifacts")
    parser.add_argument("--catalog", default="soccer_analytics", help="Unity Catalog name")
    parser.add_argument("--schema", default="dev_gold", help="Schema name")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Print planned operations without writing")
    mode.add_argument("--apply", action="store_true", help="Write tags and sidecars")
    args = parser.parse_args(argv)

    for field in ("catalog", "schema"):
        value = getattr(args, field)
        if not IDENTIFIER_RE.match(value):
            parser.error(f"Invalid {field} name '{value}': must match {IDENTIFIER_RE.pattern}")

    from databricks.sdk import WorkspaceClient
    from mlflow.tracking import MlflowClient

    workspace_client = WorkspaceClient()
    mlflow_client = MlflowClient()

    total_changes = 0

    logger.info("Phase 1: MLflow @Champion tags")
    for model in _MLFLOW_MODELS:
        total_changes += bootstrap_mlflow_model(
            mlflow_client,
            args.catalog,
            args.schema,
            model,
            apply=args.apply,
        )

    logger.info("Phase 2: UC Volume sidecars")
    for relative_path in _VOLUME_ARTIFACTS:
        total_changes += bootstrap_volume_artifact(
            workspace_client,
            args.catalog,
            args.schema,
            relative_path,
            apply=args.apply,
        )

    mode_str = "applied" if args.apply else "would apply"
    logger.info("Done — %s %d changes", mode_str, total_changes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
