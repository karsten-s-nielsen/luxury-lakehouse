"""Tests for the BackendPool multi-backend dispatcher."""

from __future__ import annotations

import threading
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from evolve.backends.pool import BackendPool


def _make_mock_backend(delay: float = 0.0, metrics: dict[str, float] | None = None) -> MagicMock:
    """Create a mock backend with configurable delay and return metrics."""
    backend = MagicMock()
    result = metrics or {"combined_score": 1.0, "spearman_rho": 0.5}

    def _train(**kwargs: Any) -> dict[str, float]:
        if delay > 0:
            time.sleep(delay)
        return dict(result)

    backend.train.side_effect = _train
    backend.available.return_value = True
    return backend


class TestBackendPool:
    def test_single_backend(self) -> None:
        backend = _make_mock_backend()
        pool = BackendPool([backend])
        result = pool.train(candidate_config={}, target="test", epochs=1, seed=42)
        assert result["combined_score"] == 1.0
        backend.train.assert_called_once()

    def test_two_backends_concurrent(self) -> None:
        """Two concurrent train() calls should use both backends."""
        fast = _make_mock_backend(delay=0.1, metrics={"combined_score": 1.0, "backend": 1.0})
        slow = _make_mock_backend(delay=0.2, metrics={"combined_score": 0.8, "backend": 2.0})
        pool = BackendPool([fast, slow])

        results: list[dict[str, float]] = []
        barrier = threading.Barrier(2)

        def _run() -> None:
            barrier.wait()
            r = pool.train(candidate_config={}, target="test", epochs=1, seed=42)
            results.append(r)

        t1 = threading.Thread(target=_run)
        t2 = threading.Thread(target=_run)
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        assert len(results) == 2
        assert fast.train.call_count == 1
        assert slow.train.call_count == 1

    def test_fast_backend_gets_more_work(self) -> None:
        """With 3 sequential calls and 2 backends, the faster one handles 2."""
        fast = _make_mock_backend(delay=0.05)
        slow = _make_mock_backend(delay=0.2)
        pool = BackendPool([fast, slow])

        results: list[dict[str, float]] = []

        def _run() -> None:
            r = pool.train(candidate_config={}, target="test", epochs=1, seed=42)
            results.append(r)

        threads = [threading.Thread(target=_run) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert len(results) == 3
        assert fast.train.call_count >= 2
        assert slow.train.call_count >= 1

    def test_backend_failure_does_not_crash_pool(self) -> None:
        """A failing backend should propagate the exception but not poison the pool."""
        good = _make_mock_backend(metrics={"combined_score": 1.0})
        bad = MagicMock()
        bad.train.side_effect = RuntimeError("GPU on fire")
        bad.available.return_value = True
        pool = BackendPool([good, bad])

        # First call gets good backend, second gets bad
        result = pool.train(candidate_config={}, target="test", epochs=1, seed=42)
        assert result["combined_score"] == 1.0

        with pytest.raises(RuntimeError, match="GPU on fire"):
            pool.train(candidate_config={}, target="test", epochs=1, seed=42)

        # Pool still works — bad backend was released back
        result = pool.train(candidate_config={}, target="test", epochs=1, seed=42)
        assert result["combined_score"] == 1.0

    def test_available_any(self) -> None:
        b1 = MagicMock()
        b1.available.return_value = False
        b2 = MagicMock()
        b2.available.return_value = True
        pool = BackendPool([b1, b2])
        assert pool.available() is True

    def test_available_none(self) -> None:
        b1 = MagicMock()
        b1.available.return_value = False
        pool = BackendPool([b1])
        assert pool.available() is False

    def test_empty_pool_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least one backend"):
            BackendPool([])
