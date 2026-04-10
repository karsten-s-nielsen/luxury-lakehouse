"""HFJobsBackend — runs training evaluations on Hugging Face Jobs infrastructure.

Submits a PEP 723 UV script via ``huggingface_hub.run_uv_job``, polls until
the job completes, and parses JSON metrics from the job logs.  Each instance
handles one job at a time — for parallel jobs, list ``hf_jobs`` multiple
times in the config ``type`` field and set ``parallel_evaluations``
accordingly.

The worker script installs the ``luxury-lakehouse`` wheel from HF Hub,
receives the candidate config as a base64-encoded JSON env var, trains the
model, and prints a single JSON metrics line to stdout.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any

from evolve.backends.base import fail_metrics
from shared.wheel import WHEEL_BASE_URL

_log = logging.getLogger(__name__)

# Polling interval (seconds) when waiting for a job to complete.
_POLL_INTERVAL = 15

# The PEP 723 worker script that runs on HF Jobs.  It receives the candidate
# config as a base64-encoded JSON string in the ``EVOLVE_CANDIDATE_CONFIG``
# env var, and the target/device/epochs/seed as additional env vars.
_WORKER_SCRIPT = f'''\
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "luxury-lakehouse[analytics,training] @ {WHEEL_BASE_URL}",
# ]
# ///

"""HF Jobs worker for evolve candidate evaluation."""

import base64
import importlib
import json
import os
import sys

# Redirect logging to stderr so stdout stays clean for JSON output.
import logging
logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(name)s %(message)s")

_log = logging.getLogger("hf_jobs_worker")


def main() -> None:
    config_b64 = os.environ.get("EVOLVE_CANDIDATE_CONFIG", "")
    if not config_b64:
        print(json.dumps({{"combined_score": 0.0, "error": 1.0, "reason": "missing EVOLVE_CANDIDATE_CONFIG"}}))
        sys.exit(0)

    candidate_config: dict = json.loads(base64.b64decode(config_b64).decode())
    device = os.environ.get("EVOLVE_DEVICE", "cuda:0")
    epochs = int(os.environ.get("EVOLVE_EPOCHS", "5"))
    seed = int(os.environ.get("EVOLVE_SEED", "42"))
    target = os.environ.get("EVOLVE_TARGET", "scoutgpt")

    # Decode Level 2 program file if provided.
    program_b64 = os.environ.get("EVOLVE_PROGRAM")
    program_path = None
    if program_b64:
        from pathlib import Path as _Path

        program_source = base64.b64decode(program_b64).decode()
        program_path = "/tmp/evolve_program.py"
        _Path(program_path).write_text(program_source)

    _log.info("Running %s evaluator (device=%s, epochs=%d, seed=%d)", target, device, epochs, seed)

    target_module = importlib.import_module(f"evolve.targets.{{target}}.evaluator")
    metrics: dict = target_module.train_and_evaluate(
        candidate_config=candidate_config,
        device=device,
        epochs=epochs,
        seed=seed,
        program_path=program_path,
    )

    # Single JSON line to stdout — the backend parses this from job logs.
    print(json.dumps(metrics))


if __name__ == "__main__":
    main()
'''


class HFJobsBackend:
    """Compute backend that submits training jobs to Hugging Face Jobs.

    Each instance handles a single concurrent job.  For N concurrent
    jobs, list ``hf_jobs`` N times in the config's ``type`` field.
    The :class:`~evolve.backends.pool.BackendPool` dispatches to
    whichever instance is free.

    Args:
        hf_flavor: Hardware flavor (e.g. ``"l40sx1"``, ``"a100-large"``).
        timeout: Maximum seconds to wait for a job to complete.
        namespace: HF namespace for jobs (defaults to authenticated user).
    """

    def __init__(
        self,
        hf_flavor: str = "l40sx1",  # Best cost/candidate for 8M param models
        timeout: int = 6000,
        namespace: str | None = None,
    ) -> None:
        self._hf_flavor = hf_flavor
        self._timeout = timeout
        self._namespace = namespace
        _log.info("HFJobsBackend initialised", extra={"hf_flavor": hf_flavor, "timeout": timeout})

    # ------------------------------------------------------------------
    # ComputeBackend protocol
    # ------------------------------------------------------------------

    def train(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
        program_path: str | None = None,
    ) -> dict[str, float]:
        """Submit a training job to HF Jobs and wait for results.

        On any failure (submission, timeout, parse error) returns
        ``fail_metrics()`` so the evolution loop continues.
        """
        _log.info(
            "HFJobsBackend.train starting",
            extra={"target": target, "epochs": epochs, "seed": seed, "flavor": self._hf_flavor},
        )
        try:
            return self._train_impl(candidate_config, target, epochs, seed, program_path=program_path)
        except Exception:
            _log.exception("HFJobsBackend.train failed")
            return fail_metrics()

    def _train_impl(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
        program_path: str | None = None,
    ) -> dict[str, float]:
        """Core training logic — submit job, poll, parse metrics."""
        from huggingface_hub import HfApi

        api = HfApi()

        # Encode candidate config as base64 JSON for the env var.
        config_b64 = base64.b64encode(json.dumps(candidate_config).encode()).decode()

        # Write script to temp file — run_uv_job expects a file path, not inline content.
        tmp_dir = Path(tempfile.mkdtemp(prefix="evolve_hfjob_"))
        script_file = tmp_dir / "worker.py"
        script_file.write_text(_WORKER_SCRIPT, encoding="utf-8")

        # Build env dict for the job.
        env: dict[str, str] = {
            "EVOLVE_CANDIDATE_CONFIG": config_b64,
            "EVOLVE_DEVICE": "cuda:0",
            "EVOLVE_EPOCHS": str(epochs),
            "EVOLVE_SEED": str(seed),
            "EVOLVE_TARGET": target,
        }

        # Encode program source for Level 2 code evolution.
        if program_path is not None:
            program_source = Path(program_path).read_text()
            env["EVOLVE_PROGRAM"] = base64.b64encode(program_source.encode()).decode()

        # Submit the job.
        job_info = api.run_uv_job(
            script=str(script_file),
            env=env,
            secrets={"HF_TOKEN": self._get_hf_token()},
            flavor=self._hf_flavor,
            timeout=f"{self._timeout}s",
            namespace=self._namespace,
        )

        job_id = job_info.id
        _log.info("HF Job submitted: %s (flavor=%s)", job_id, self._hf_flavor)

        # Poll until the job completes or times out.
        metrics = self._poll_job(api, job_id)

        _log.info("HFJobsBackend.train complete", extra={"target": target, "metrics": metrics})
        return metrics

    @staticmethod
    def _get_hf_token() -> str:
        """Resolve HF token via huggingface_hub (respects login cache + env var)."""
        from huggingface_hub import get_token

        return get_token() or ""

    def _poll_job(self, api: Any, job_id: str) -> dict[str, float]:
        """Poll a running job until completion, then parse metrics from logs."""
        from huggingface_hub._jobs_api import JobStage

        deadline = time.monotonic() + self._timeout

        while time.monotonic() < deadline:
            info = api.inspect_job(job_id=job_id, namespace=self._namespace)
            stage = info.status.stage

            if stage == JobStage.COMPLETED:
                return self._parse_metrics_from_logs(api, job_id)
            if stage in (JobStage.ERROR, JobStage.CANCELED, JobStage.DELETED):
                msg = info.status.message or "unknown"
                _log.error("HF Job %s failed: stage=%s, message=%s", job_id, stage.value, msg)
                return fail_metrics()

            time.sleep(_POLL_INTERVAL)

        # Timeout — cancel the job.
        _log.error("HF Job %s timed out after %ds", job_id, self._timeout)
        try:
            api.cancel_job(job_id=job_id, namespace=self._namespace)
        except Exception:
            _log.warning("Failed to cancel timed-out job %s", job_id, exc_info=True)
        return fail_metrics()

    def _parse_metrics_from_logs(self, api: Any, job_id: str) -> dict[str, float]:
        """Extract the JSON metrics line from job logs."""
        logs = list(api.fetch_job_logs(job_id=job_id, namespace=self._namespace))
        # The worker prints a single JSON line to stdout.  Scan from the
        # end to find it (earlier lines may be stderr/logging).
        for line in reversed(logs):
            stripped = line.strip()
            if stripped.startswith("{") and stripped.endswith("}"):
                try:
                    metrics: dict[str, float] = json.loads(stripped)
                    if "combined_score" in metrics or "spearman_rho" in metrics:
                        return metrics
                except json.JSONDecodeError:
                    continue

        _log.error("No valid JSON metrics found in HF Job %s logs (%d lines)", job_id, len(logs))
        return fail_metrics()

    def available(self) -> bool:
        """Return True if HF_TOKEN is set and the HF API is reachable."""
        if not os.environ.get("HF_TOKEN"):
            return False
        try:
            from huggingface_hub import HfApi

            api = HfApi()
            api.whoami()
            return True
        except Exception:
            _log.warning("HFJobsBackend not available: HF API check failed", exc_info=True)
            return False
