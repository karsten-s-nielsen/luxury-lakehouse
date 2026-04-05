"""HFJobsBackend — stub for Hugging Face Jobs training evaluation."""

from __future__ import annotations

import logging
from typing import Any

from evolve.backends.base import fail_metrics

_log = logging.getLogger(__name__)


class HFJobsBackend:
    """Compute backend that submits training jobs to Hugging Face Jobs.

    Not yet implemented.  Instantiate to verify configuration; call
    :meth:`available` to confirm the backend is ready.
    """

    def __init__(self, hf_flavor: str = "cpu-basic") -> None:
        self._hf_flavor = hf_flavor
        _log.info("HFJobsBackend initialised (stub)", extra={"hf_flavor": hf_flavor})

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
        """Return failure metrics — backend not yet implemented."""
        _log.warning("HFJobsBackend.train called but backend is not yet implemented")
        return fail_metrics()

    def available(self) -> bool:
        """Always returns False — backend not yet implemented."""
        return False
