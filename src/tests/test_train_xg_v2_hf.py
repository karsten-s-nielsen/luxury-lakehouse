"""Unit tests for scripts/train_xg_v2_hf.py pre-flight checks.

Shared helpers (``require_mlflow_env``, ``upload_weights_to_uc_volume``,
``set_and_verify_mlflow_champion``) were extracted into
``ingestion.artifact_deploy`` on 2026-04-22 and now have dedicated tests in
``test_artifact_deploy.py``. The AST-based source-inspection regressions in
this file remain — they lock in the training script's contract that MLflow
registration never becomes conditional again, independent of where the
helpers live.
"""

from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


class TestMandatoryMlflowRegistration:
    """Regression: MLflow registration must not become conditional again.

    Pre-2026-04-22 the script wrapped the entire MLflow block in
    ``if tracking_uri:`` which silently skipped registration on HF Jobs
    invocations that lacked ``MLFLOW_TRACKING_URI``. ADR-002 converted the
    consumer's silent swallow to a hard-fail, surfacing the gap. This test
    locks in the post-hardening contract: the training script never
    conditionally skips MLflow registration.
    """

    def test_no_if_tracking_uri_gate_in_main(self) -> None:
        """Source-level regression: `if tracking_uri:` must not appear in train_xg_v2_hf.

        Parses the script with AST and walks every ``ast.If`` node looking for
        a test expression that is the bare name ``tracking_uri``. This catches
        the actual gate pattern without false-firing on docstrings, comments,
        or string literals that mention the phrase in prose.
        """
        import ast

        script_path = _SCRIPTS_DIR / "train_xg_v2_hf.py"
        source = script_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.If) and isinstance(node.test, ast.Name) and node.test.id == "tracking_uri":
                raise AssertionError(
                    f"Found forbidden `if tracking_uri:` gate at "
                    f"scripts/train_xg_v2_hf.py:{node.lineno}. "
                    "MLflow registration is mandatory — see ADR-002 and "
                    "2026-04-22-xg2-production-unblock plan."
                )

    def test_tracking_uri_is_subscripted_not_get(self) -> None:
        """Source-level regression: must use ``os.environ[...]`` (raise on missing)
        rather than ``os.environ.get(..., "")`` (silent empty default) for
        MLFLOW_TRACKING_URI."""
        script_path = _SCRIPTS_DIR / "train_xg_v2_hf.py"
        source = script_path.read_text(encoding="utf-8")
        # The `.get("MLFLOW_TRACKING_URI", "")` pattern was the mechanism that
        # let the `if tracking_uri:` branch silently skip. Ban it outright.
        assert 'os.environ.get("MLFLOW_TRACKING_URI"' not in source, (
            'Found forbidden `os.environ.get("MLFLOW_TRACKING_URI", ...)` in '
            'scripts/train_xg_v2_hf.py. Use ``os.environ["MLFLOW_TRACKING_URI"]`` '
            "(subscript) so a missing value raises KeyError instead of silently "
            "turning into an empty string."
        )
