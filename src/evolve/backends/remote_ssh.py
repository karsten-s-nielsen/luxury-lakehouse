"""RemoteSSHBackend — runs training evaluations on a remote host over SSH."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
import threading
from typing import Any

_log = logging.getLogger(__name__)

_FAIL_METRICS: dict[str, float] = {"combined_score": 0.0, "error": 1.0}

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
    ) -> None:
        self._host = host
        self._user = user or os.environ.get("USER", "")
        self._remote_dir = remote_dir or "~/Development/evolve-workspace"
        self._python_path = python_path or "~/Development/evolve-env/bin/python"
        self._timeout = timeout
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
            return dict(_FAIL_METRICS)
        except subprocess.CalledProcessError as exc:
            _log.error(
                "Remote command failed",
                extra={"host": self._host, "returncode": exc.returncode, "stderr": (exc.stderr or "").strip()},
            )
            return dict(_FAIL_METRICS)
        except json.JSONDecodeError as exc:
            _log.error(
                "Failed to parse JSON from remote stdout",
                extra={"host": self._host, "error": str(exc)},
            )
            return dict(_FAIL_METRICS)
        except OSError as exc:
            _log.error(
                "OS error during remote training",
                extra={"host": self._host, "error": str(exc)},
            )
            return dict(_FAIL_METRICS)

    def _train_impl(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
    ) -> dict[str, float]:
        """Core training logic — separated so ``train`` can catch all errors."""
        # 1. Write candidate config to a temporary local file.
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as tmp:
            tmp.write(f"config = {candidate_config!r}\n")
            local_candidate_path = tmp.name

        try:
            # 2. Ensure remote workspace exists.
            self._ensure_remote_dir()

            # 3. Transfer the candidate file.
            remote_filename = "candidate.py"
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
            f"{remote_filename} cuda:0 {epochs} {seed} {target}"
        )
        cmd = ["ssh", self._ssh_target, remote_cmd]
        effective_timeout = self._timeout

        proc = subprocess.Popen(  # noqa: S603
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Stream stderr in a background thread so it doesn't block stdout reading.
        stderr_lines: list[str] = []

        def _stream_stderr() -> None:
            assert proc.stderr is not None  # noqa: S101
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
            return dict(_FAIL_METRICS)

        # 5. Parse JSON metrics from stdout.
        #    The remote worker prints exactly one JSON line. If there is
        #    extra output (warnings, logs to stderr), stdout should still
        #    contain the JSON as the last non-empty line.
        stdout = (stdout or "").strip()
        if not stdout:
            _log.error("Remote worker produced empty stdout", extra={"host": self._host})
            return dict(_FAIL_METRICS)

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
