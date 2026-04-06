"""Tests for the HFJobsBackend — mocked HF API, no real job submission."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from huggingface_hub._jobs_api import JobStage

from evolve.backends.hf_jobs import HFJobsBackend

# --- Fake HF Jobs API objects (using real JobStage enum) ---


@dataclass
class _FakeStatus:
    stage: JobStage
    message: str | None = None


@dataclass
class _FakeJobInfo:
    id: str = "job-test-123"
    status: _FakeStatus = _FakeStatus(stage=JobStage.RUNNING)  # noqa: RUF009


class TestHFJobsBackend:
    def test_train_submits_job_and_parses_metrics(self) -> None:
        """Full happy-path: submit, poll RUNNING then COMPLETED, parse logs."""
        backend = HFJobsBackend(hf_flavor="l40sx1", timeout=600)

        mock_api = MagicMock()

        # run_uv_job returns a JobInfo with an ID
        mock_api.run_uv_job.return_value = _FakeJobInfo(id="job-abc")

        # First inspect_job: RUNNING, second: COMPLETED
        mock_api.inspect_job.side_effect = [
            _FakeJobInfo(id="job-abc", status=_FakeStatus(stage=JobStage.RUNNING)),
            _FakeJobInfo(id="job-abc", status=_FakeStatus(stage=JobStage.COMPLETED)),
        ]

        # Logs contain a JSON metrics line
        metrics_json = json.dumps({"spearman_rho": 0.45, "top1_accuracy": 0.82, "combined_score": 0.55})
        mock_api.fetch_job_logs.return_value = [
            "INFO Loading model...\n",
            "INFO Epoch 1/3\n",
            f"{metrics_json}\n",
        ]

        with patch("huggingface_hub.HfApi", return_value=mock_api), \
             patch("evolve.backends.hf_jobs.time") as mock_time:
            mock_time.monotonic.side_effect = [0.0, 1.0, 2.0, 3.0]
            mock_time.sleep = MagicMock()

            result = backend.train(
                candidate_config={"hidden_dim": 256, "num_layers": 6},
                target="scoutgpt",
                epochs=3,
                seed=42,
            )

        assert result["spearman_rho"] == pytest.approx(0.45)
        assert result["combined_score"] == pytest.approx(0.55)

        # Verify the env vars passed to run_uv_job
        call_kwargs = mock_api.run_uv_job.call_args
        env = call_kwargs.kwargs["env"]
        decoded_config = json.loads(base64.b64decode(env["EVOLVE_CANDIDATE_CONFIG"]).decode())
        assert decoded_config["hidden_dim"] == 256
        assert env["EVOLVE_EPOCHS"] == "3"
        assert call_kwargs.kwargs["flavor"] == "l40sx1"

    def test_train_returns_fail_on_job_error(self) -> None:
        """Job fails — should return fail_metrics, not raise."""
        backend = HFJobsBackend(hf_flavor="l40sx1", timeout=600)

        mock_api = MagicMock()
        mock_api.run_uv_job.return_value = _FakeJobInfo(id="job-fail")
        mock_api.inspect_job.return_value = _FakeJobInfo(
            id="job-fail",
            status=_FakeStatus(stage=JobStage.ERROR, message="OOM killed"),
        )

        with patch("huggingface_hub.HfApi", return_value=mock_api), \
             patch("evolve.backends.hf_jobs.time") as mock_time:
            mock_time.monotonic.side_effect = [0.0, 1.0]
            mock_time.sleep = MagicMock()

            result = backend.train(
                candidate_config={"hidden_dim": 256},
                target="scoutgpt",
                epochs=3,
                seed=42,
            )

        assert result["combined_score"] == 0.0
        assert result["error"] == 1.0

    def test_train_returns_fail_on_timeout(self) -> None:
        """Job never completes — should timeout and cancel."""
        backend = HFJobsBackend(hf_flavor="l40sx1", timeout=30)

        mock_api = MagicMock()
        mock_api.run_uv_job.return_value = _FakeJobInfo(id="job-slow")
        mock_api.inspect_job.return_value = _FakeJobInfo(
            id="job-slow",
            status=_FakeStatus(stage=JobStage.RUNNING),
        )

        with patch("huggingface_hub.HfApi", return_value=mock_api), \
             patch("evolve.backends.hf_jobs.time") as mock_time:
            # monotonic returns values past the deadline
            mock_time.monotonic.side_effect = [0.0, 0.0, 31.0]
            mock_time.sleep = MagicMock()

            result = backend.train(
                candidate_config={"hidden_dim": 256},
                target="scoutgpt",
                epochs=3,
                seed=42,
            )

        assert result["combined_score"] == 0.0
        mock_api.cancel_job.assert_called_once_with(job_id="job-slow", namespace=None)

    def test_parse_metrics_finds_json_in_noisy_logs(self) -> None:
        """Logs contain non-JSON lines — parser should find the metrics line."""
        backend = HFJobsBackend(hf_flavor="l40sx1")
        mock_api = MagicMock()

        metrics = {"spearman_rho": 0.5, "top1_accuracy": 0.8, "combined_score": 0.6}
        mock_api.fetch_job_logs.return_value = [
            "WARNING some warning\n",
            "INFO training started\n",
            json.dumps(metrics) + "\n",
            "INFO cleanup\n",
        ]

        result = backend._parse_metrics_from_logs(mock_api, "job-123")
        assert result["combined_score"] == pytest.approx(0.6)

    def test_available_requires_hf_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Without HF_TOKEN, available() returns False."""
        monkeypatch.delenv("HF_TOKEN", raising=False)
        backend = HFJobsBackend(hf_flavor="l40sx1")
        assert backend.available() is False

    def test_config_encoding_roundtrip(self) -> None:
        """Candidate config survives base64 encoding/decoding."""
        config: dict[str, Any] = {
            "hidden_dim": 256,
            "num_layers": 6,
            "learning_rate": 1e-4,
            "conditioning_type": "cross_attention",
        }
        encoded = base64.b64encode(json.dumps(config).encode()).decode()
        decoded = json.loads(base64.b64decode(encoded).decode())
        assert decoded == config
