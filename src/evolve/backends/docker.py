"""DockerBackend — stub for container-based training evaluation."""

from __future__ import annotations

import logging
from typing import Any

_log = logging.getLogger(__name__)


class DockerBackend:
    """Compute backend that launches training inside a Docker container.

    Not yet implemented.  Instantiate to verify configuration; call
    :meth:`available` to confirm the backend is ready.
    """

    def __init__(self, docker_image: str = "") -> None:
        self._docker_image = docker_image
        _log.info("DockerBackend initialised (stub)", extra={"docker_image": docker_image})

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
        """Not implemented."""
        raise NotImplementedError("DockerBackend.train is not yet implemented")

    def available(self) -> bool:
        """Always returns False — backend not yet implemented."""
        return False
