"""Backend registry for the Evolve engine.

Call :func:`create_backend` with a :class:`~evolve.config.BackendConfig` to
get a ready-to-use :class:`~evolve.backends.base.ComputeBackend` instance.
Imports for each backend class are deferred so unused backends impose no
import overhead.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from evolve.backends.base import ComputeBackend

if TYPE_CHECKING:
    from evolve.config import BackendConfig

_log = logging.getLogger(__name__)

__all__ = ["ComputeBackend", "create_backend"]

# ---------------------------------------------------------------------------
# Backend factory registry — add new backends here.
# Each factory receives the full BackendConfig and a timeout value,
# performs the lazy import, and returns a concrete backend instance.
# ---------------------------------------------------------------------------


def _make_local_cuda(config: BackendConfig, timeout: int) -> ComputeBackend:
    from evolve.backends.local_cuda import LocalCudaBackend

    return LocalCudaBackend(device=config.device)


def _make_docker(config: BackendConfig, timeout: int) -> ComputeBackend:
    from evolve.backends.docker import DockerBackend

    return DockerBackend(docker_image=config.docker_image or "")


def _make_hf_jobs(config: BackendConfig, timeout: int) -> ComputeBackend:
    from evolve.backends.hf_jobs import HFJobsBackend

    return HFJobsBackend(hf_flavor=config.hf_flavor or "")


def _make_remote_ssh(config: BackendConfig, timeout: int) -> ComputeBackend:
    from evolve.backends.remote_ssh import RemoteSSHBackend

    return RemoteSSHBackend(
        host=config.ssh_host or "",
        user=config.ssh_user or "",
        remote_dir=config.ssh_remote_dir or "",
        python_path=config.ssh_python_path or "",
        timeout=timeout,
        device=config.device,
    )


_BACKEND_REGISTRY: dict[str, Callable[[BackendConfig, int], ComputeBackend]] = {
    "local_cuda": _make_local_cuda,
    "docker": _make_docker,
    "hf_jobs": _make_hf_jobs,
    "remote_ssh": _make_remote_ssh,
}


def _create_single_backend(backend_type: str, config: BackendConfig, *, timeout: int = 900) -> ComputeBackend:
    """Instantiate a single backend by type name."""
    factory = _BACKEND_REGISTRY.get(backend_type)
    if factory is None:
        msg = f"Unknown backend type '{backend_type}'. Registered: {sorted(_BACKEND_REGISTRY)}"
        raise ValueError(msg)
    return factory(config, timeout)


def create_backend(config: BackendConfig, *, timeout: int = 900) -> ComputeBackend:
    """Instantiate and return the compute backend described by *config*.

    Supports comma-separated types (e.g. ``"local_cuda,remote_ssh"``)
    to create a :class:`~evolve.backends.pool.BackendPool` that dispatches
    evaluations across multiple backends concurrently.

    Args:
        config: A validated :class:`~evolve.config.BackendConfig` instance.
        timeout: Per-evaluation timeout in seconds, forwarded to backends
            that enforce timeouts (e.g. :class:`RemoteSSHBackend`).

    Returns:
        A concrete backend (or pool) that satisfies the :class:`ComputeBackend` protocol.

    Raises:
        ValueError: If any type in ``config.type`` is not a recognised backend name.
    """
    types = [t.strip() for t in config.type.split(",")]
    _log.info("create_backend called", extra={"types": types})

    if len(types) == 1:
        return _create_single_backend(types[0], config, timeout=timeout)

    from evolve.backends.pool import BackendPool

    backends = [_create_single_backend(t, config, timeout=timeout) for t in types]
    return BackendPool(backends)
