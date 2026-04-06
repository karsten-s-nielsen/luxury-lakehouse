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
    thread-safe priority queue, delegates training, then releases it.

    Priority is determined by insertion order: the first backend in the
    list has priority 0 (highest), the second has priority 1, etc.
    When multiple backends are idle, the highest-priority (lowest
    number) backend is dispatched first.  After completing work, a
    backend re-enters the queue at its original priority, so faster
    backends naturally handle more candidates *and* are preferred when
    multiple backends are simultaneously idle.

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
        # Map each backend instance to its fixed priority so we can
        # re-insert at the correct position after train() completes.
        self._priority: dict[int, int] = {id(b): i for i, b in enumerate(backends)}
        self._available: queue.PriorityQueue[tuple[int, int, ComputeBackend]] = queue.PriorityQueue()
        for i, b in enumerate(backends):
            self._available.put((i, id(b), b))
        _log.info("BackendPool initialised with %d backends (priority order)", len(backends))

    def train(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
    ) -> dict[str, float]:
        """Acquire the highest-priority idle backend, train, then release it."""
        try:
            _priority, _id, backend = self._available.get(timeout=self._ACQUIRE_TIMEOUT)
        except queue.Empty:
            _log.error("No backend available after %ds — possible deadlock", self._ACQUIRE_TIMEOUT)
            return fail_metrics()
        backend_name = type(backend).__name__
        _log.info("BackendPool dispatching to %s (priority %d)", backend_name, _priority)
        try:
            return backend.train(
                candidate_config=candidate_config,
                target=target,
                epochs=epochs,
                seed=seed,
            )
        finally:
            pri = self._priority[id(backend)]
            self._available.put((pri, id(backend), backend))
            _log.info("BackendPool released %s", backend_name)

    def available(self) -> bool:
        """Return True if any backend in the pool is available."""
        return any(b.available() for b in self._backends)
