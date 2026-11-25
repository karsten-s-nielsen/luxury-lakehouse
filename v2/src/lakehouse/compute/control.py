"""The compute phase's deadline and stop control (perf spec §7.5, lever R; ADR-017).

HF Jobs sends SIGKILL about 2 seconds after SIGTERM (`docs/ops/deploy.md`), so on HF only a deadline the run sets
for itself ends a run cleanly. A `ComputeControl` carries one layer's deadlines and the run's stop flag:

- `may_start_match()` is False once the run is stopped or past the SOFT deadline: the loop starts no new match.
- `should_stop()` is True once the run is stopped or past the HARD deadline: a scorer raises `ComputeAbandoned`
  between frame groups, and the driver writes nothing for that match and counts it as deferred.
- `on_written()` runs after each written match (the run publishes progress through its checkpoint policy).

This module is on the scoring path (`TRACKING_CODE_MODULES`), so it imports nothing from `lakehouse` at runtime.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from lakehouse.config.model import ComputeConfig

__all__ = ["UNBOUNDED", "ComputeAbandoned", "ComputeControl"]


class ComputeAbandoned(Exception):  # noqa: N818 - a control signal, not an error: the match is deferred, not failed
    """Raised by a scorer when `should_stop()` turns True mid-match. The match is deferred: nothing is written."""


def _never() -> bool:
    return False


def _nothing() -> None:
    return None


@dataclass(frozen=True)
class ComputeControl:
    soft_deadline: datetime | None
    hard_deadline: datetime | None
    now: Callable[[], datetime]
    stop: Callable[[], bool]
    on_written: Callable[[], None]
    check_every: int

    def may_start_match(self) -> bool:
        """Whether the loop may start another match (not stopped, not past the soft deadline)."""
        if self.stop():
            return False
        return self.soft_deadline is None or self.now() < self.soft_deadline

    def should_stop(self) -> bool:
        """Whether a match in progress must be abandoned (stopped, or past the hard deadline)."""
        if self.stop():
            return True
        return self.hard_deadline is not None and self.now() >= self.hard_deadline

    @classmethod
    def for_layer(
        cls,
        config: ComputeConfig,
        *,
        started_at: datetime,
        layer: int,
        now: Callable[[], datetime],
        stop: Callable[[], bool],
        on_written: Callable[[], None],
    ) -> ComputeControl:
        """One layer's control. Layer 1 also leaves `layer2_reserve_seconds` so layer 2 progresses every night."""
        reserve = config.finish_reserve_seconds + (config.layer2_reserve_seconds if layer == 1 else 0)
        hard = started_at + timedelta(seconds=config.budget_seconds - reserve)
        soft = hard - timedelta(seconds=config.worst_match_seconds)
        return cls(
            soft_deadline=soft,
            hard_deadline=hard,
            now=now,
            stop=stop,
            on_written=on_written,
            check_every=config.check_every_frames,
        )


#: No deadline, never stops, no progress hook: unit tests and library callers outside a run.
UNBOUNDED: Final = ComputeControl(
    soft_deadline=None,
    hard_deadline=None,
    now=datetime.now,
    stop=_never,
    on_written=_nothing,
    check_every=500,
)
