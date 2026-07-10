"""SEC2: smoke tests for the bootstrap_artifact_hashes one-off script.

The script requires real Databricks credentials + MLflow + UC Volumes to run
meaningfully, so these tests only cover the arg parser and the pure helper
functions. Full E2E coverage happens when the script is manually run against
the dev workspace.
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

# Add scripts/ to path so we can import the bootstrap module directly
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))


def test_sha256_helper_matches_hashlib() -> None:
    """The script's _sha256 helper must match hashlib.sha256.hexdigest()."""
    from bootstrap_artifact_hashes import _sha256

    data = b"the quick brown fox"
    assert _sha256(data) == hashlib.sha256(data).hexdigest()


def test_sha256_handles_empty_bytes() -> None:
    from bootstrap_artifact_hashes import _sha256

    empty_sha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"  # pragma: allowlist secret
    assert _sha256(b"") == empty_sha


def test_main_requires_mode_flag() -> None:
    """main() without --dry-run or --apply must exit non-zero (argparse required)."""
    from bootstrap_artifact_hashes import main

    with pytest.raises(SystemExit):
        main(["--catalog", "test", "--schema", "dev"])


def test_main_rejects_invalid_catalog_name() -> None:
    """Invalid catalog name (containing shell-dangerous chars) must be rejected."""
    from bootstrap_artifact_hashes import main

    with pytest.raises(SystemExit):
        main(["--catalog", "bad;name", "--schema", "dev", "--dry-run"])


def test_main_rejects_invalid_schema_name() -> None:
    from bootstrap_artifact_hashes import main

    with pytest.raises(SystemExit):
        main(["--catalog", "test", "--schema", "bad-schema", "--dry-run"])


def test_mlflow_models_list_matches_sec2_scope() -> None:
    """The 4 MLflow models list must match SEC2 spec: xg_model, xg_model_v3,
    vaep_model, defcon_model. (xg_model_v3 replaced xg_model_v2 on 2026-07-10 when
    the v2 producer chain retired — ADR-066; pre-shot xG stays hash-covered.)
    """
    from bootstrap_artifact_hashes import _MLFLOW_MODELS

    assert set(_MLFLOW_MODELS) == {"xg_model", "xg_model_v3", "vaep_model", "defcon_model"}


def test_volume_artifacts_list_matches_sec2_scope() -> None:
    """The 3 UC Volume artifact paths must match SEC2 spec."""
    from bootstrap_artifact_hashes import _VOLUME_ARTIFACTS

    expected = {
        "xg_model/logistic_model.json",
        "xg_model/xgboost_model.json",
        "xg_model_v3/model_weights.json",
    }
    assert set(_VOLUME_ARTIFACTS) == expected
