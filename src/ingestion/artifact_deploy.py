"""Shared training-to-production delivery pattern for ML model artifacts.

This module codifies the post-2026-04-22 hardening applied to xG v1 and v2
training scripts. Previous training scripts had a three-way latent-bug class:

1. **Silent MLflow skip.** ``if tracking_uri:`` let the whole MLflow block
   disappear when ``MLFLOW_TRACKING_URI`` was unset. Training would report
   success without ever registering the model. ADR-002's silent-swallow
   elimination converted the CONSUMER's soft-fail to a hard-fail, which
   surfaced the gap; this module's ``require_mlflow_env`` closes the gap
   at the PRODUCER side.

2. **No UC Volume write.** Training published to HF Hub + (conditionally)
   MLflow. The Databricks inference consumer looks at MLflow + UC Volume.
   If MLflow was silently skipped, UC Volume was also empty, so the
   consumer had no way to load weights. ``upload_weights_to_uc_volume``
   provides the second leg of the delivery chain.

3. **Zombie @Champion alias.** ``mlflow.pyfunc.log_model`` registers a
   version as a side effect; ``set_registered_model_alias`` then points
   the alias at it. If the alias-set quietly fails (permission glitch,
   registry race), the run still exits 0 with a registered version and
   no alias — consumer still broken. ``set_and_verify_mlflow_champion``
   round-trips the alias read to catch that zombie state.

Training scripts on the Databricks inference path (e.g. ``scripts/train_xg_v3_hf.py``,
``scripts/train_vaep_model_hf.py``) import from this module. Any future training script should follow the
same pattern.
"""

from __future__ import annotations

import hashlib
import io
import logging
import os
from typing import TYPE_CHECKING, Any

from shared.constants import IDENTIFIER_RE

if TYPE_CHECKING:
    from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)

# Base env vars always required for Databricks-backed MLflow + UC registration.
_BASE_MLFLOW_ENV_VARS: tuple[str, ...] = (
    "MLFLOW_TRACKING_URI",
    "DATABRICKS_HOST",
)
# M2M OAuth service-principal credential (client_id + client_secret). The
# databricks-sdk (and MLflow-on-Databricks, and
# ``ingestion.databricks_auth.workspace_client``) mint AND auto-refresh tokens
# from these on demand inside the running job — so, unlike a static token, this
# credential survives an HF-Jobs run longer than an OAuth access token's ~1h TTL
# (e.g. a 180-min ScoutGPT fit). See ADR-079.
_M2M_ENV_VARS: tuple[str, ...] = (
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
)

# Back-compat: the historical name = base env vars + the static-token credential.
# Retained for external references; ``require_mlflow_env`` now ALSO accepts the
# M2M credential form (``_M2M_ENV_VARS``) as an alternative to DATABRICKS_TOKEN.
REQUIRED_MLFLOW_ENV_VARS: tuple[str, ...] = (*_BASE_MLFLOW_ENV_VARS, "DATABRICKS_TOKEN")
"""Environment variables to register a model with a STATIC-token credential."""


def require_mlflow_env() -> None:
    """Fail loud unless the Databricks MLflow/UC registration env is complete.

    Requires ``MLFLOW_TRACKING_URI`` + ``DATABRICKS_HOST`` **and** a Databricks
    credential in one of two forms:

    - ``DATABRICKS_TOKEN`` — a static bearer token (a PAT, or a short-lived OAuth
      access token). Simple, but expires mid-job; unsafe for a training run
      longer than the token's TTL.
    - ``DATABRICKS_CLIENT_ID`` + ``DATABRICKS_CLIENT_SECRET`` — M2M OAuth
      service-principal creds. The databricks-sdk / MLflow-on-Databricks /
      :func:`ingestion.databricks_auth.workspace_client` mint and **auto-refresh**
      tokens from these on demand, so the credential survives an arbitrarily long
      job. PREFERRED for HF-Jobs training since the PAT->OAuth migration (ADR-079).

    Training scripts must call this at the top of ``main()`` BEFORE any work, so
    a silent skip of the registry step cannot occur (ADR-002).

    Raises:
        RuntimeError: naming what is missing, with the ``hf jobs uv run``
            remediation hint (both credential forms).
    """
    missing_base = [name for name in _BASE_MLFLOW_ENV_VARS if not os.environ.get(name)]
    has_token = bool(os.environ.get("DATABRICKS_TOKEN"))
    has_m2m = all(os.environ.get(name) for name in _M2M_ENV_VARS)
    if not missing_base and (has_token or has_m2m):
        return

    problems: list[str] = []
    if missing_base:
        problems.append(f"missing base env vars {missing_base}")
    if not (has_token or has_m2m):
        problems.append(
            "no Databricks credential (need DATABRICKS_TOKEN, or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)"
        )
    raise RuntimeError(
        "MLflow UC registration env incomplete: " + "; ".join(problems) + ". "
        "On `hf jobs uv run`: pass MLFLOW_TRACKING_URI (literal 'databricks') + "
        "DATABRICKS_HOST via `--env`, and the Databricks credential via `--secrets` "
        "(encrypted). PREFER M2M OAuth — DATABRICKS_CLIENT_ID + "
        "DATABRICKS_CLIENT_SECRET (service-principal creds auto-refresh, so a >1h "
        "job survives) — over a static DATABRICKS_TOKEN, which expires mid-job. "
        "Silent MLflow skip is not allowed (ADR-002)."
    )


def upload_weights_to_uc_volume(
    workspace_client: Any,
    *,
    catalog: str,
    schema: str,
    model_name: str,
    filename: str,
    weights_bytes: bytes,
) -> dict[str, str]:
    """Upload a model artifact to UC Volume with a SHA-256 sidecar.

    Writes two files to ``/Volumes/{catalog}/{schema}/model_weights/{model_name}/``:
      - ``{filename}`` — the raw artifact bytes
      - ``{filename}.sha256`` — hex SHA-256 of the bytes

    Both writes use ``overwrite=True`` because this is a republish path.
    The SHA-256 sidecar is consumed by ``ingestion.utils._load_volume_sidecar_hash``
    during inference (SEC2 artifact integrity verification).

    Args:
        workspace_client: An authenticated ``databricks.sdk.WorkspaceClient``
            (or any object exposing ``.files.upload(path, body, overwrite=True)``).
        catalog: Unity Catalog name (validated against ``IDENTIFIER_RE``).
        schema: UC schema name (validated against ``IDENTIFIER_RE``).
        model_name: Model subdirectory name under ``model_weights/``
            (validated against ``IDENTIFIER_RE``).
        filename: Target filename (e.g. ``"model_weights.json"`` or
            ``"xgboost_model.json"``). Must end in ``.json``.
        weights_bytes: Serialized artifact bytes.

    Returns:
        Dict with keys ``path`` (canonical volume path of the artifact)
        and ``sha256`` (hex digest of the bytes).

    Raises:
        ValueError: If any identifier fails SQL-safety validation, if
            ``filename`` is malformed, or if ``weights_bytes`` is empty.
    """
    if not IDENTIFIER_RE.match(catalog):
        raise ValueError(f"Invalid catalog name: {catalog!r}")
    if not IDENTIFIER_RE.match(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    if not IDENTIFIER_RE.match(model_name):
        raise ValueError(f"Invalid model_name: {model_name!r}")
    # filename validation: basename only (no path separators), must end .json
    if "/" in filename or "\\" in filename or not filename.endswith(".json"):
        raise ValueError(f"Invalid filename (must be a basename ending in .json): {filename!r}")
    if not weights_bytes:
        raise ValueError("weights_bytes is empty; refusing to upload")

    artifact_path = f"/Volumes/{catalog}/{schema}/model_weights/{model_name}/{filename}"
    sidecar_path = artifact_path + ".sha256"
    sha256 = hashlib.sha256(weights_bytes).hexdigest()

    workspace_client.files.upload(artifact_path, io.BytesIO(weights_bytes), overwrite=True)
    workspace_client.files.upload(sidecar_path, io.BytesIO(sha256.encode("utf-8")), overwrite=True)

    logger.info(
        "Uploaded %s to UC Volume: %s (%d bytes, sha256=%s)",
        model_name,
        artifact_path,
        len(weights_bytes),
        sha256[:8],
    )
    return {"path": artifact_path, "sha256": sha256}


def set_and_verify_mlflow_champion(
    mlflow_client: MlflowClient,
    *,
    mlflow_fqn: str,
    run_id: str,
) -> str:
    """Set ``@Champion`` alias on the latest registered version + verify.

    Protects against the "zombie @Champion" failure mode:
    ``mlflow.pyfunc.log_model(..., registered_model_name=...)`` registers
    a version as a side effect, ``set_registered_model_alias`` points the
    alias at it. If the alias-set silently no-ops (permission glitch, race),
    the run ships with a registered version and no alias — consumer still
    broken. This helper does a round-trip ``get_model_version_by_alias``
    read and raises if the resolved version doesn't match the one we just
    registered.

    Args:
        mlflow_client: An ``mlflow.tracking.MlflowClient`` instance. Caller
            must have already called ``mlflow.set_tracking_uri(...)``.
        mlflow_fqn: Fully-qualified UC model URI, e.g.
            ``"soccer_analytics.dev_gold.xg_model_v3"``.
        run_id: The MLflow run ID for logging context.

    Returns:
        The registered version string that got the alias.

    Raises:
        RuntimeError: If no versions were registered under ``mlflow_fqn``
            (indicates a silent ``log_model`` failure), or if the alias
            does not resolve to the expected version after being set.
    """
    versions = mlflow_client.search_model_versions(f"name='{mlflow_fqn}'")
    if not versions:
        raise RuntimeError(
            f"mlflow.pyfunc.log_model registered no versions under {mlflow_fqn!r}. "
            "This indicates a silent registration failure — refusing to exit "
            "successfully and leave the daily consumer without a fresh model. "
            f"Check MLflow run {run_id} and the Databricks UC registry."
        )
    latest = max(versions, key=lambda v: int(v.version))
    mlflow_client.set_registered_model_alias(name=mlflow_fqn, alias="Champion", version=latest.version)

    resolved = mlflow_client.get_model_version_by_alias(mlflow_fqn, "Champion")
    if str(resolved.version) != str(latest.version):
        raise RuntimeError(
            f"MLflow @Champion alias did not resolve to v{latest.version} after "
            f"set_registered_model_alias(). Resolved to v{resolved.version} instead. "
            "This is a zombie-alias state; registry is inconsistent."
        )
    logger.info("MLflow complete (version=%s, run=%s, @Champion verified)", latest.version, run_id)
    return str(latest.version)
