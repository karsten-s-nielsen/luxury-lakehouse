"""RemoteSSHBackend — runs training evaluations on a remote host over SSH."""

from __future__ import annotations

import atexit
import json
import logging
import os
import subprocess
import tempfile
import threading
from typing import Any

from evolve.backends.base import fail_metrics

_log = logging.getLogger(__name__)


# Timeouts (seconds) for lightweight SSH commands (availability check, mkdir, scp).
_SSH_CMD_TIMEOUT = 30


class RemoteSSHBackend:
    """Compute backend that dispatches training to a remote host via SSH.

    The backend transfers a candidate config file to the remote machine,
    invokes the ``evolve.remote_worker`` module over SSH, and parses the
    JSON metrics printed to stdout.

    Requirements:
        * SSH key-based authentication configured (no passwords).
        * The ``evolve`` package available on the remote ``PYTHONPATH``.
    """

    def __init__(
        self,
        host: str = "",
        user: str = "",
        remote_dir: str = "",
        python_path: str = "",
        timeout: int = 900,
        device: str = "cuda:0",
    ) -> None:
        self._host = host
        self._user = user or os.environ.get("USER", "")
        self._remote_dir = remote_dir or "~/Development/evolve-workspace"
        self._python_path = python_path or "~/Development/evolve-env/bin/python"
        self._timeout = timeout
        self._device = device
        self._hf_cache_warmed = False
        self._active_procs: set[subprocess.Popen[str]] = set()
        self._proc_lock = threading.Lock()
        atexit.register(self._cleanup_remote_procs)
        _log.info(
            "RemoteSSHBackend initialised",
            extra={
                "host": self._host,
                "user": self._user,
                "remote_dir": self._remote_dir,
                "timeout": self._timeout,
            },
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def _ssh_target(self) -> str:
        """Return ``user@host`` string for SSH/SCP commands."""
        return f"{self._user}@{self._host}"

    def _cleanup_remote_procs(self) -> None:
        """Kill any SSH subprocesses still running (registered via atexit).

        Prevents orphaned training processes on the remote host when the
        parent process crashes or is terminated (SIGTERM, Ctrl+C).
        """
        with self._proc_lock:
            if not self._active_procs:
                return
            _log.info("Cleaning up %d active SSH subprocess(es)", len(self._active_procs))
            for proc in list(self._active_procs):
                try:
                    proc.kill()
                    proc.wait(timeout=5)
                except (OSError, subprocess.TimeoutExpired):
                    pass
            self._active_procs.clear()

    def _run_ssh(
        self,
        remote_cmd: str,
        *,
        timeout: int | None = None,
        capture: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        """Execute *remote_cmd* on the remote host via ``ssh``.

        Args:
            remote_cmd: Shell command string to execute remotely.
            timeout: Per-command timeout in seconds (defaults to instance timeout).
            capture: Whether to capture stdout/stderr.

        Returns:
            The :class:`subprocess.CompletedProcess` result.

        Raises:
            subprocess.TimeoutExpired: If the command exceeds *timeout*.
            subprocess.CalledProcessError: If the remote command exits non-zero
                and the caller used ``check=True``.
        """
        effective_timeout = timeout if timeout is not None else self._timeout
        cmd = ["ssh", self._ssh_target, remote_cmd]
        return subprocess.run(  # noqa: S603
            cmd,
            capture_output=capture,
            text=True,
            timeout=effective_timeout,
        )

    def _ensure_remote_dir(self) -> None:
        """Create the remote workspace directory if it does not exist."""
        result = self._run_ssh(f"mkdir -p {self._remote_dir}", timeout=_SSH_CMD_TIMEOUT)
        if result.returncode != 0:
            _log.warning(
                "Failed to create remote directory",
                extra={"remote_dir": self._remote_dir, "stderr": result.stderr.strip()},
            )

    def _warm_hf_cache(self, dataset_repo: str) -> None:
        """Pre-download HF dataset on the remote host so training doesn't pay the cold-cache cost.

        Only runs once per backend lifetime. Failures are logged but not fatal —
        training will still download on first use, just slower.
        """
        if self._hf_cache_warmed:
            return
        _log.info("Pre-warming HF cache on remote: %s", dataset_repo)
        cmd = (
            f"{self._python_path} -c "
            f'"from huggingface_hub import snapshot_download; '
            f"snapshot_download('{dataset_repo}', repo_type='dataset')\""
        )
        try:
            result = self._run_ssh(cmd, timeout=300)
            if result.returncode == 0:
                _log.info("HF cache warm on remote")
            else:
                _log.warning("HF cache warming failed (non-fatal): %s", result.stderr.strip()[:200])
        except (subprocess.TimeoutExpired, OSError) as exc:
            _log.warning("HF cache warming failed (non-fatal): %s", exc)
        self._hf_cache_warmed = True

    def _scp_to_remote(self, local_path: str, remote_filename: str) -> None:
        """Copy a local file to the remote workspace via ``scp``.

        Raises:
            subprocess.CalledProcessError: If the transfer fails.
        """
        remote_dest = f"{self._ssh_target}:{self._remote_dir}/{remote_filename}"
        subprocess.run(  # noqa: S603
            ["scp", local_path, remote_dest],  # noqa: S607
            check=True,
            capture_output=True,
            text=True,
            timeout=_SSH_CMD_TIMEOUT,
        )

    # ------------------------------------------------------------------
    # ComputeBackend protocol
    # ------------------------------------------------------------------

    def train(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
    ) -> dict[str, float]:
        """Transfer the candidate config and run training on the remote host.

        On any failure (network, remote crash, parse error) the method
        returns ``{"combined_score": 0.0, "error": 1.0}`` rather than
        propagating exceptions, so the evolution loop continues.
        """
        _log.info(
            "RemoteSSHBackend.train starting",
            extra={"target": target, "epochs": epochs, "seed": seed, "host": self._host},
        )

        try:
            return self._train_impl(candidate_config, target, epochs, seed)
        except subprocess.TimeoutExpired:
            _log.error(
                "Remote training timed out",
                extra={"host": self._host, "timeout": self._timeout},
            )
            return fail_metrics()
        except subprocess.CalledProcessError as exc:
            _log.error(
                "Remote command failed",
                extra={"host": self._host, "returncode": exc.returncode, "stderr": (exc.stderr or "").strip()},
            )
            return fail_metrics()
        except json.JSONDecodeError as exc:
            _log.error(
                "Failed to parse JSON from remote stdout",
                extra={"host": self._host, "error": str(exc)},
            )
            return fail_metrics()
        except OSError as exc:
            _log.error(
                "OS error during remote training",
                extra={"host": self._host, "error": str(exc)},
            )
            return fail_metrics()

    def _train_impl(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
    ) -> dict[str, float]:
        """Core training logic — separated so ``train`` can catch all errors."""
        # 0. Pre-warm HF cache on first call to avoid cold-start timeout.
        dataset_repo = candidate_config.get("dataset", "luxury-lakehouse/scoutgpt-training-data")
        self._warm_hf_cache(dataset_repo)

        # 1. Write candidate config as JSON to a temporary local file.
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            json.dump(candidate_config, tmp)
            local_candidate_path = tmp.name

        try:
            # 2. Ensure remote workspace exists.
            self._ensure_remote_dir()

            # 3. Transfer the candidate file.
            remote_filename = "candidate.json"
            self._scp_to_remote(local_candidate_path, remote_filename)
        finally:
            # Clean up the local temp file regardless of transfer outcome.
            os.unlink(local_candidate_path)

        # 4. Run the remote worker, streaming stderr for live progress.
        #    stdout is captured in full (contains the JSON metrics line).
        #    stderr is streamed line-by-line to the local logger.
        remote_cmd = (
            f"cd {self._remote_dir} && "
            f"PYTHONUNBUFFERED=1 stdbuf -oL -eL "
            f"{self._python_path} -m evolve.remote_worker "
            f"{remote_filename} {self._device} {epochs} {seed} {target}"
        )
        cmd = ["ssh", self._ssh_target, remote_cmd]
        effective_timeout = self._timeout

        proc = subprocess.Popen(  # noqa: S603
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        with self._proc_lock:
            self._active_procs.add(proc)

        # Stream stderr in a background thread so it doesn't block stdout reading.
        stderr_lines: list[str] = []

        def _stream_stderr() -> None:
            if proc.stderr is None:
                return
            try:
                for line in proc.stderr:
                    stripped = line.rstrip()
                    if stripped:
                        stderr_lines.append(stripped)
                        _log.info("[remote] %s", stripped)
            except ValueError:
                pass  # proc.communicate() closed the pipe

        stderr_thread = threading.Thread(target=_stream_stderr, daemon=True)
        stderr_thread.start()

        try:
            stdout, _ = proc.communicate(timeout=effective_timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            raise
        finally:
            with self._proc_lock:
                self._active_procs.discard(proc)

        stderr_thread.join(timeout=5)

        if proc.returncode != 0:
            _log.error(
                "Remote worker exited with non-zero status",
                extra={
                    "returncode": proc.returncode,
                    "stderr": "\n".join(stderr_lines[-10:]),
                    "stdout": (stdout or "").strip()[:500],
                },
            )
            return fail_metrics()

        # 5. Parse JSON metrics from stdout.
        #    The remote worker prints exactly one JSON line. If there is
        #    extra output (warnings, logs to stderr), stdout should still
        #    contain the JSON as the last non-empty line.
        stdout = (stdout or "").strip()
        if not stdout:
            _log.error("Remote worker produced empty stdout", extra={"host": self._host})
            return fail_metrics()

        # Take the last non-empty line — earlier lines may be stray warnings.
        json_line = stdout.splitlines()[-1]
        metrics: dict[str, float] = json.loads(json_line)

        _log.info(
            "RemoteSSHBackend.train complete",
            extra={"target": target, "metrics": metrics},
        )
        return metrics

    def available(self) -> bool:
        """Return True if the remote host is reachable via SSH."""
        if not self._host:
            return False
        try:
            result = self._run_ssh("echo OK", timeout=5)
            return result.returncode == 0 and "OK" in result.stdout
        except (subprocess.TimeoutExpired, OSError):
            return False
