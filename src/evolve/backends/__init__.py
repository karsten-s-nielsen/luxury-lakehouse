"""Backend registry for the Evolve engine.

Call :func:`create_backend` with a :class:`~evolve.config.BackendConfig` to
get a ready-to-use :class:`~evolve.backends.base.ComputeBackend` instance.
Imports for each backend class are deferred so unused backends impose no
import overhead.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from evolve.backends.base import ComputeBackend

if TYPE_CHECKING:
    from evolve.config import BackendConfig

_log = logging.getLogger(__name__)

__all__ = ["ComputeBackend", "create_backend"]


def create_backend(config: BackendConfig) -> ComputeBackend:
    """Instantiate and return the compute backend described by *config*.

    Args:
        config: A validated :class:`~evolve.config.BackendConfig` instance.

    Returns:
        A concrete backend that satisfies the :class:`ComputeBackend` protocol.

    Raises:
        ValueError: If ``config.type`` is not a recognised backend name.
    """
    backend_type = config.type
    _log.info("create_backend called", extra={"type": backend_type})

    if backend_type == "local_cuda":
        from evolve.backends.local_cuda import LocalCudaBackend

        return LocalCudaBackend(device=config.device)

    if backend_type == "docker":
        from evolve.backends.docker import DockerBackend

        return DockerBackend(docker_image=config.docker_image or "")

    if backend_type == "hf_jobs":
        from evolve.backends.hf_jobs import HFJobsBackend

        return HFJobsBackend(hf_flavor=config.hf_flavor or "cpu-basic")

    if backend_type == "remote_ssh":
        from evolve.backends.remote_ssh import RemoteSSHBackend

        return RemoteSSHBackend(
            host=config.ssh_host or "",
            user=config.ssh_user or "",
            remote_dir=config.ssh_remote_dir or "",
            python_path=config.ssh_python_path or "",
        )

    msg = f"Unknown backend type '{backend_type}'"
    raise ValueError(msg)
