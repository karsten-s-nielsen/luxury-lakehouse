"""Offline tests for SparkInterruptWatchdog's threading/re-raise logic (ADR-037).

No Spark needed: the session is injected, so a stub with addTag/interruptTag exercises
the thread + box + re-raise + abandonment-count logic (N2, P3, P6, P8).
"""

from __future__ import annotations

import time

import pytest

from analytics.action_context.drain import GameTimeoutError
from ingestion.action_context_queue import SparkInterruptWatchdog


class _StubSpark:
    """Minimal stand-in: addTag/interruptTag are no-ops (simulates the
    non-interruptible / event-only path where interruptTag cancels nothing)."""

    def __init__(self) -> None:
        self.interrupted: list[str] = []

    def addTag(self, tag: str) -> None:  # noqa: N802 - Spark Connect API name
        pass

    def interruptTag(self, tag: str) -> list[str]:  # noqa: N802 - Spark Connect API name
        self.interrupted.append(tag)
        return []  # a no-op build returns no op-ids


def test_watchdog_run_returns_value() -> None:
    wd = SparkInterruptWatchdog(_StubSpark())  # type: ignore[arg-type]
    assert wd.run(lambda: 7, "lbl", timeout_s=5) == 7


def test_watchdog_run_reraises_fn_exception() -> None:
    """N2/P8: a real processing error must surface (type preserved), NOT vanish in the thread."""
    wd = SparkInterruptWatchdog(_StubSpark())  # type: ignore[arg-type]

    def _boom() -> int:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        wd.run(_boom, "lbl", timeout_s=5)


def test_watchdog_timeout_abandons_noninterruptible() -> None:
    """P3: stub interruptTag is a no-op (non-interruptible path) -> the fn keeps sleeping
    past the grace join -> thread abandoned, counted LIVE."""
    wd = SparkInterruptWatchdog(_StubSpark(), interrupt_grace_s=0.1)  # type: ignore[arg-type]
    with pytest.raises(GameTimeoutError):
        wd.run(lambda: int(time.sleep(10) or 1), "lbl", timeout_s=0.2)
    assert wd.live_abandoned_count == 1  # still sleeping -> alive -> counted
