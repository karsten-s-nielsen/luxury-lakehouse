"""BackendPool — dispatches training evaluations across multiple compute backends."""

from __future__ import annotations

import logging
import queue
from typing import Any

from evolve.backends.base import ComputeBackend, fail_metrics

_log = logging.getLogger(__name__)


class BackendPool:
    """Distribute ``train()`` calls across multiple backends.

    OpenEvolve calls the evaluator concurrently when
    ``parallel_evaluations > 1``.  Each call blocks on
    :meth:`train` which acquires the next idle backend from a
    thread-safe FIFO, delegates training, then releases the backend.
    Faster backends naturally handle more candidates.

    Satisfies the :class:`ComputeBackend` protocol.
    """

    # Maximum seconds to wait for an idle backend before giving up.
    # Set generously above the per-evaluation timeout to allow for queue
    # contention without masking genuine deadlocks.
    _ACQUIRE_TIMEOUT = 3600

    def __init__(self, backends: list[ComputeBackend]) -> None:
        if not backends:
            msg = "BackendPool requires at least one backend"
            raise ValueError(msg)
        self._backends = backends
        self._available: queue.Queue[ComputeBackend] = queue.Queue()
        for b in backends:
            self._available.put(b)
        _log.info("BackendPool initialised with %d backends", len(backends))

    def train(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
    ) -> dict[str, float]:
        """Acquire the next idle backend, train, then release it."""
        try:
            backend = self._available.get(timeout=self._ACQUIRE_TIMEOUT)
        except queue.Empty:
            _log.error("No backend available after %ds — possible deadlock", self._ACQUIRE_TIMEOUT)
            return fail_metrics()
        backend_name = type(backend).__name__
        _log.info("BackendPool dispatching to %s", backend_name)
        try:
            return backend.train(
                candidate_config=candidate_config,
                target=target,
                epochs=epochs,
                seed=seed,
            )
        finally:
            self._available.put(backend)
            _log.info("BackendPool released %s", backend_name)

    def available(self) -> bool:
        """Return True if any backend in the pool is available."""
        return any(b.available() for b in self._backends)
