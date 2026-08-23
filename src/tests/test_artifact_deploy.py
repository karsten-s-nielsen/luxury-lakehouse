"""Unit tests for src/ingestion/artifact_deploy.py."""

from __future__ import annotations

import hashlib
from unittest.mock import MagicMock

import pytest


class TestRequireMlflowEnv:
    """Pre-flight check — fails loud unless base env + a credential form is present.

    Accepts EITHER a static ``DATABRICKS_TOKEN`` OR M2M OAuth service-principal
    creds (``DATABRICKS_CLIENT_ID`` + ``DATABRICKS_CLIENT_SECRET``) — ADR-079.
    """

    @staticmethod
    def _clear_m2m(monkeypatch: pytest.MonkeyPatch) -> None:
        """Ensure ambient M2M creds (a dev shell may export them) don't mask a gap."""
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)

    def test_raises_when_mlflow_tracking_uri_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from ingestion.artifact_deploy import require_mlflow_env

        monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-xxx")
        self._clear_m2m(monkeypatch)

        with pytest.raises(RuntimeError, match="MLFLOW_TRACKING_URI"):
            require_mlflow_env()

    def test_raises_when_databricks_host_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from ingestion.artifact_deploy import require_mlflow_env

        monkeypatch.setenv("MLFLOW_TRACKING_URI", "databricks")
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-xxx")
        self._clear_m2m(monkeypatch)

        with pytest.raises(RuntimeError, match="DATABRICKS_HOST"):
            require_mlflow_env()

    def test_raises_when_no_credential(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Base env present but NEITHER a static token NOR M2M creds -> raise."""
        from ingestion.artifact_deploy import require_mlflow_env

        monkeypatch.setenv("MLFLOW_TRACKING_URI", "databricks")
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        self._clear_m2m(monkeypatch)

        with pytest.raises(RuntimeError, match="no Databricks credential"):
            require_mlflow_env()

    def test_passes_when_static_token_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from ingestion.artifact_deploy import require_mlflow_env

        monkeypatch.setenv("MLFLOW_TRACKING_URI", "databricks")
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-xxx")
        self._clear_m2m(monkeypatch)
        require_mlflow_env()

    def test_passes_with_m2m_creds_and_no_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """M2M OAuth creds (client_id + client_secret) satisfy the gate with NO token."""
        from ingestion.artifact_deploy import require_mlflow_env

        monkeypatch.setenv("MLFLOW_TRACKING_URI", "databricks")
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "008b207b-0000-0000-0000-000000000000")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "dose-fake-secret")
        require_mlflow_env()

    def test_raises_with_partial_m2m_and_no_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """client_id WITHOUT client_secret is not a usable credential -> raise."""
        from ingestion.artifact_deploy import require_mlflow_env

        monkeypatch.setenv("MLFLOW_TRACKING_URI", "databricks")
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "008b207b-0000-0000-0000-000000000000")
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)

        with pytest.raises(RuntimeError, match="no Databricks credential"):
            require_mlflow_env()

    def test_error_message_names_remediation_flags(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Error message must mention --secrets, --env, BOTH cred forms, and ADR-002."""
        from ingestion.artifact_deploy import require_mlflow_env

        monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        self._clear_m2m(monkeypatch)

        with pytest.raises(RuntimeError) as excinfo:
            require_mlflow_env()
        msg = str(excinfo.value)
        assert "--secrets" in msg
        assert "--env" in msg
        assert "ADR-002" in msg
        assert "DATABRICKS_CLIENT_ID" in msg
        assert "DATABRICKS_TOKEN" in msg


class TestUploadWeightsToUcVolume:
    """Uploads a model artifact + .sha256 sidecar to UC Volume."""

    def test_uploads_artifact_and_sidecar_with_correct_paths(self) -> None:
        from ingestion.artifact_deploy import upload_weights_to_uc_volume

        weights = b'{"model_type": "set_encoder_xg_v2", "weights": {}}'
        mock_client = MagicMock()

        result = upload_weights_to_uc_volume(
            mock_client,
            catalog="soccer_analytics",
            schema="dev_gold",
            model_name="xg_model_v2",
            filename="model_weights.json",
            weights_bytes=weights,
        )

        assert mock_client.files.upload.call_count == 2
        uploaded_paths = [c.args[0] for c in mock_client.files.upload.call_args_list]
        expected_artifact = "/Volumes/soccer_analytics/dev_gold/model_weights/xg_model_v2/model_weights.json"
        assert expected_artifact in uploaded_paths
        assert expected_artifact + ".sha256" in uploaded_paths

        for call in mock_client.files.upload.call_args_list:
            assert call.kwargs.get("overwrite") is True

        assert result["path"] == expected_artifact
        assert result["sha256"] == hashlib.sha256(weights).hexdigest()

    def test_accepts_different_filenames(self) -> None:
        """Filename arg lets v1 upload both logistic_model.json AND xgboost_model.json."""
        from ingestion.artifact_deploy import upload_weights_to_uc_volume

        mock_client = MagicMock()
        result = upload_weights_to_uc_volume(
            mock_client,
            catalog="soccer_analytics",
            schema="dev_gold",
            model_name="xg_model",
            filename="xgboost_model.json",
            weights_bytes=b"x",
        )
        assert result["path"].endswith("/xg_model/xgboost_model.json")

    def test_sidecar_contents_match_sha256(self) -> None:
        from ingestion.artifact_deploy import upload_weights_to_uc_volume

        weights = b"abcdef"
        expected_hex = hashlib.sha256(weights).hexdigest()
        mock_client = MagicMock()

        upload_weights_to_uc_volume(
            mock_client,
            catalog="soccer_analytics",
            schema="dev_gold",
            model_name="xg_model_v2",
            filename="model_weights.json",
            weights_bytes=weights,
        )

        sidecar_call = next(c for c in mock_client.files.upload.call_args_list if c.args[0].endswith(".sha256"))
        body = sidecar_call.args[1]
        assert hasattr(body, "read")
        assert body.read().decode("utf-8").strip() == expected_hex

    def test_rejects_sql_unsafe_identifiers(self) -> None:
        from ingestion.artifact_deploy import upload_weights_to_uc_volume

        mock_client = MagicMock()
        weights = b"x"

        with pytest.raises(ValueError, match="Invalid catalog"):
            upload_weights_to_uc_volume(
                mock_client,
                catalog="bad;name",
                schema="dev_gold",
                model_name="xg_model_v2",
                filename="model_weights.json",
                weights_bytes=weights,
            )
        with pytest.raises(ValueError, match="Invalid schema"):
            upload_weights_to_uc_volume(
                mock_client,
                catalog="soccer_analytics",
                schema="dev gold",
                model_name="xg_model_v2",
                filename="model_weights.json",
                weights_bytes=weights,
            )
        with pytest.raises(ValueError, match="Invalid model_name"):
            upload_weights_to_uc_volume(
                mock_client,
                catalog="soccer_analytics",
                schema="dev_gold",
                model_name="xg/model/v2",
                filename="model_weights.json",
                weights_bytes=weights,
            )

    def test_rejects_malformed_filenames(self) -> None:
        from ingestion.artifact_deploy import upload_weights_to_uc_volume

        mock_client = MagicMock()
        weights = b"x"

        # Slashes not allowed
        with pytest.raises(ValueError, match="Invalid filename"):
            upload_weights_to_uc_volume(
                mock_client,
                catalog="soccer_analytics",
                schema="dev_gold",
                model_name="xg_model_v2",
                filename="sub/dir/model.json",
                weights_bytes=weights,
            )
        # Must end in .json
        with pytest.raises(ValueError, match="Invalid filename"):
            upload_weights_to_uc_volume(
                mock_client,
                catalog="soccer_analytics",
                schema="dev_gold",
                model_name="xg_model_v2",
                filename="model.pkl",
                weights_bytes=weights,
            )

    def test_empty_weights_rejected(self) -> None:
        from ingestion.artifact_deploy import upload_weights_to_uc_volume

        mock_client = MagicMock()
        with pytest.raises(ValueError, match="empty"):
            upload_weights_to_uc_volume(
                mock_client,
                catalog="soccer_analytics",
                schema="dev_gold",
                model_name="xg_model_v2",
                filename="model_weights.json",
                weights_bytes=b"",
            )
        mock_client.files.upload.assert_not_called()


class TestSetAndVerifyMlflowChampion:
    """Zombie-alias guard — fails if the alias doesn't actually point at the registered version."""

    def test_sets_and_verifies_alias_on_happy_path(self) -> None:
        from ingestion.artifact_deploy import set_and_verify_mlflow_champion

        mock_client = MagicMock()
        # One version, v5
        v5 = MagicMock()
        v5.version = 5
        mock_client.search_model_versions.return_value = [v5]
        # Alias resolution succeeds and matches v5
        resolved = MagicMock()
        resolved.version = 5
        mock_client.get_model_version_by_alias.return_value = resolved

        result = set_and_verify_mlflow_champion(
            mock_client, mlflow_fqn="soccer_analytics.dev_gold.xg_model_v2", run_id="run-abc"
        )
        assert result == "5"
        mock_client.set_registered_model_alias.assert_called_once_with(
            name="soccer_analytics.dev_gold.xg_model_v2", alias="Champion", version=5
        )

    def test_raises_when_no_versions_registered(self) -> None:
        from ingestion.artifact_deploy import set_and_verify_mlflow_champion

        mock_client = MagicMock()
        mock_client.search_model_versions.return_value = []

        with pytest.raises(RuntimeError, match="registered no versions"):
            set_and_verify_mlflow_champion(
                mock_client, mlflow_fqn="soccer_analytics.dev_gold.xg_model_v2", run_id="run-xyz"
            )

    def test_raises_on_zombie_alias(self) -> None:
        """If set_registered_model_alias succeeds but the alias resolves to the wrong version, fail."""
        from ingestion.artifact_deploy import set_and_verify_mlflow_champion

        mock_client = MagicMock()
        v5 = MagicMock()
        v5.version = 5
        mock_client.search_model_versions.return_value = [v5]
        # Alias-set "succeeds" silently but resolves to a STALE v4 (zombie)
        stale = MagicMock()
        stale.version = 4
        mock_client.get_model_version_by_alias.return_value = stale

        with pytest.raises(RuntimeError, match="did not resolve to v5"):
            set_and_verify_mlflow_champion(
                mock_client, mlflow_fqn="soccer_analytics.dev_gold.xg_model_v2", run_id="run-zombie"
            )

    def test_picks_latest_version_when_multiple_exist(self) -> None:
        from ingestion.artifact_deploy import set_and_verify_mlflow_champion

        mock_client = MagicMock()
        v2 = MagicMock()
        v2.version = 2
        v10 = MagicMock()
        v10.version = 10  # Numeric, not string — ``int(v.version)`` must pick this one
        v5 = MagicMock()
        v5.version = 5
        mock_client.search_model_versions.return_value = [v2, v10, v5]
        resolved = MagicMock()
        resolved.version = 10
        mock_client.get_model_version_by_alias.return_value = resolved

        result = set_and_verify_mlflow_champion(
            mock_client, mlflow_fqn="soccer_analytics.dev_gold.xg_model_v2", run_id="run-multi"
        )
        assert result == "10"
